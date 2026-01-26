# handlers/admin_handlers.py
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional, List

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest

from config import settings
import database as db

log = logging.getLogger(__name__)

admin_router = Router()


# =======================
#  Admin States
# =======================

class AdminState(StatesGroup):
    pass


# =======================
#  Access control
# =======================

def is_admin(user_id: int) -> bool:
    return user_id in settings.admin_ids


# =======================
#  Compact UI (single edited message)
# =======================

async def _try_delete_message(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        return


async def _clear_flow_keep_ui(state: FSMContext) -> None:
    """
    Очищає FSM, але зберігає admin_ui_msg_id,
    щоб адмін-панель НЕ створювала новий "екран".
    """
    data = await state.get_data()
    ui_msg_id = data.get("admin_ui_msg_id")
    await state.clear()
    if isinstance(ui_msg_id, int) and ui_msg_id > 0:
        await state.update_data(admin_ui_msg_id=ui_msg_id)


async def _ui_get_or_create_screen(message: Message, state: FSMContext) -> int:
    data = await state.get_data()
    ui_msg_id = data.get("admin_ui_msg_id")
    if isinstance(ui_msg_id, int) and ui_msg_id > 0:
        return ui_msg_id

    sent = await message.answer("Адмін-панель завантажується…")
    await state.update_data(admin_ui_msg_id=sent.message_id)
    return sent.message_id


async def _ui_render(
    *,
    bot,
    chat_id: int,
    state: FSMContext,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: str = "HTML",
) -> None:
    data = await state.get_data()
    ui_msg_id = data.get("admin_ui_msg_id")

    if isinstance(ui_msg_id, int) and ui_msg_id > 0:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=ui_msg_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            return
        except TelegramBadRequest as e:
            # message to edit not found / message is not modified / etc.
            log.warning("ADMIN UI edit failed: %s", e)
        except Exception as e:
            log.exception("ADMIN UI edit unexpected error: %s", e)

    sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    await state.update_data(admin_ui_msg_id=sent.message_id)


# =======================
#  Helpers
# =======================

UA_WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]


def _time_to_minutes(t: str) -> int:
    h = int(t[:2])
    m = int(t[3:5])
    return h * 60 + m


def _minutes_to_time(mm: int) -> str:
    h = mm // 60
    m = mm % 60
    return f"{h:02d}:{m:02d}"


def _ceil_to_step(value: int, step: int) -> int:
    if step <= 0:
        return value
    return ((value + step - 1) // step) * step


def _overlap(a_s: int, a_e: int, b_s: int, b_e: int) -> bool:
    return a_s < b_e and b_s < a_e


async def _set_shop_setting(key: str, value: int) -> None:
    """
    Підтримує 2 варіанти database.py:
    1) db.set_shop_setting(key, value)
    2) набір специфічних setter-ів
    """
    if hasattr(db, "set_shop_setting"):
        await db.set_shop_setting(key, int(value))
        return

    mapping = {
        "base_grid_minutes": "set_base_grid_minutes",
        "short_service_threshold_minutes": "set_short_service_threshold_minutes",
        "rest_minutes_after_short": "set_rest_minutes_after_short",
        "extra_round_minutes": "set_extra_round_minutes",
        "min_lead_minutes": "set_min_lead_minutes",
        "slot_step_minutes": "set_slot_step_minutes",  # legacy
    }
    fn_name = mapping.get(key)
    if not fn_name or not hasattr(db, fn_name):
        raise RuntimeError(f"No setter for shop setting: {key}")

    await getattr(db, fn_name)(int(value))


async def _get_global_breaks() -> List[dict]:
    """
    Безпечний доступ до "глобальних" перерв.
    Під різні database.py: пробуємо кілька варіантів.
    """
    # 1) якщо є спеціальна функція
    if hasattr(db, "get_global_breaks"):
        return await db.get_global_breaks()

    # 2) якщо get_breaks_for_weekday приймає weekday=None
    if hasattr(db, "get_breaks_for_weekday"):
        try:
            rows = await db.get_breaks_for_weekday(None)
            return rows or []
        except TypeError:
            pass
        except Exception as e:
            log.warning("get_breaks_for_weekday(None) failed: %s", e)

    # 3) fallback: беремо weekday=0 і відфільтровуємо weekday is None
    try:
        rows = await db.get_breaks_for_weekday(weekday=0)
        rows = rows or []
        return [b for b in rows if b.get("weekday") is None]
    except Exception as e:
        log.exception("Fallback breaks load failed: %s", e)
        return []


async def _add_break_global(start_time: str, end_time: str) -> None:
    if hasattr(db, "add_break"):
        await db.add_break(None, start_time, end_time)
        return
    if hasattr(db, "add_break_global"):
        await db.add_break_global(start_time, end_time)
        return
    raise RuntimeError("No function to add global break in database.py")


async def _remove_break(break_id: int) -> None:
    if hasattr(db, "remove_break"):
        await db.remove_break(break_id)
        return
    if hasattr(db, "delete_break"):
        await db.delete_break(break_id)
        return
    raise RuntimeError("No function to remove break in database.py")


def _kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Записи (сьогодні)", callback_data="ad:today")],
            [InlineKeyboardButton(text="⏳ Pending-заявки", callback_data="ad:pending")],
            [InlineKeyboardButton(text="📊 Звіти", callback_data="ad:reports")],
            [InlineKeyboardButton(text="⚙️ Налаштування", callback_data="ad:settings")],
        ]
    )


def _kb_back_to_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:menu")]])


def _kb_reports() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📆 За сьогодні", callback_data="ad:r:today")],
            [InlineKeyboardButton(text="📅 За 7 днів", callback_data="ad:r:week")],
            [InlineKeyboardButton(text="🗓 За 30 днів", callback_data="ad:r:month")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:menu")],
        ]
    )


def _kb_settings_home(s: dict) -> InlineKeyboardMarkup:
    base_grid = int(s.get("base_grid_minutes", 60))
    short_thr = int(s.get("short_service_threshold_minutes", 40))
    rest_short = int(s.get("rest_minutes_after_short", 5))
    extra_round = int(s.get("extra_round_minutes", 15))
    lead = int(s.get("min_lead_minutes", 0))

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🧱 Сітка: {base_grid} хв (база)", callback_data="ad:set:grid")],
            [InlineKeyboardButton(text=f"⚡ Коротка послуга < {short_thr} хв", callback_data="ad:set:short_thr")],
            [InlineKeyboardButton(text=f"🛑 Пауза після короткої: {rest_short} хв", callback_data="ad:set:rest_short")],
            [InlineKeyboardButton(text=f"🔁 Округлення дод. слоту: {extra_round} хв", callback_data="ad:set:extra_round")],
            [InlineKeyboardButton(text=f"⏳ Мін. запас: {lead} хв", callback_data="ad:set:lead")],
            [InlineKeyboardButton(text="🗓 Графік по днях", callback_data="ad:set:schedule")],
            [InlineKeyboardButton(text="☕ Перерви (для всіх днів)", callback_data="ad:set:breaks")],
            [InlineKeyboardButton(text="🚫 Вихідні по датах", callback_data="ad:set:dayoff")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:menu")],
        ]
    )


def _kb_pick_int(current: int, options: List[int], prefix: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for v in options:
        mark = "✅ " if v == current else ""
        row.append(InlineKeyboardButton(text=f"{mark}{v}", callback_data=f"{prefix}:{v}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_weekdays(schedule: dict) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for wd in range(7):
        s = schedule.get(wd, {"is_working": True, "work_start": "09:00", "work_end": "19:00"})
        status = "✅" if s["is_working"] else "🚫"
        text = f"{UA_WEEKDAYS[wd]} {status} {s['work_start']}–{s['work_end']}"
        rows.append([InlineKeyboardButton(text=text, callback_data=f"ad:sch:day:{wd}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_day_edit(wd: int, is_working: bool, ws: str, we: str) -> InlineKeyboardMarkup:
    status_btn = InlineKeyboardButton(
        text=("✅ Робочий" if is_working else "🚫 Вихідний"),
        callback_data=f"ad:sch:toggle:{wd}",
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [status_btn],
            [InlineKeyboardButton(text=f"🕘 Початок: {ws}", callback_data=f"ad:sch:set:ws:{wd}")],
            [InlineKeyboardButton(text=f"🕖 Кінець: {we}", callback_data=f"ad:sch:set:we:{wd}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:set:schedule")],
        ]
    )


def _kb_time_pick(wd: int, field: str, current: str) -> InlineKeyboardMarkup:
    times: List[str] = []
    for h in range(7, 23):
        for m in (0, 30):
            times.append(f"{h:02d}:{m:02d}")

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for t in times:
        hhmm = t.replace(":", "")
        mark = "✅ " if t == current else ""
        row.append(InlineKeyboardButton(text=f"{mark}{t}", callback_data=f"ad:sch:pick:{wd}:{field}:{hhmm}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ad:sch:day:{wd}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_breaks_list(breaks_rows: List[dict]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    rows.append(
        [
            InlineKeyboardButton(text="➕ 13:00–14:00", callback_data="ad:br:add:1300:1400"),
            InlineKeyboardButton(text="➕ 14:00–15:00", callback_data="ad:br:add:1400:1500"),
        ]
    )
    for b in breaks_rows:
        bid = int(b["id"])
        st = b["start_time"]
        et = b["end_time"]
        rows.append([InlineKeyboardButton(text=f"🗑 Видалити {st}–{et}", callback_data=f"ad:br:del:{bid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_dayoff_14days() -> InlineKeyboardMarkup:
    today = date.today()
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i in range(14):
        d = today + timedelta(days=i)
        wd = UA_WEEKDAYS[d.weekday()]
        label = f"{d.strftime('%d.%m')} ({wd})"
        row.append(InlineKeyboardButton(text=label, callback_data=f"ad:do:pick:{d.isoformat()}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_dayoff_toggle(date_str: str, off: bool) -> InlineKeyboardMarkup:
    btn = InlineKeyboardButton(
        text=("✅ Зробити робочим" if off else "🚫 Зробити вихідним"),
        callback_data=f"ad:do:toggle:{date_str}",
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="ad:set:dayoff")],
        ]
    )


# =======================
#  /admin
# =======================

@admin_router.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас немає прав доступу до адмін-панелі.", parse_mode=None)
        return

    await _try_delete_message(message)

    await _ui_get_or_create_screen(message, state)
    await _ui_render(
        bot=message.bot,
        chat_id=message.chat.id,
        state=state,
        text=(
            "<b>Панель адміністратора</b>\n"
            f"{settings.shop_name} — {settings.master_name}\n\n"
            "Оберіть дію нижче:"
        ),
        reply_markup=_kb_admin_main(),
    )


@admin_router.callback_query(F.data == "ad:menu")
async def ad_menu(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    await _clear_flow_keep_ui(state)
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=(
            "<b>Панель адміністратора</b>\n"
            f"{settings.shop_name} — {settings.master_name}\n\n"
            "Оберіть дію нижче:"
        ),
        reply_markup=_kb_admin_main(),
    )
    await callback.answer()


# =======================
#  Today / Pending / Reports
# =======================

@admin_router.callback_query(F.data == "ad:today")
async def ad_today(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    d = date.today().isoformat()
    rows = await db.get_bookings_for_date_admin(d)
    if not rows:
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text=f"На <b>{d}</b> записів немає.",
            reply_markup=_kb_back_to_main(),
        )
        await callback.answer()
        return

    status_map = {
        "pending": "⏳ очікує",
        "approved": "✅ підтверджено",
        "completed": "🏁 завершено",
        "rejected": "❌ відхилено",
        "cancelled_by_client": "🚫 скасовано клієнтом",
        "cancelled_by_admin": "🚫 скасовано майстром",
    }

    lines = [f"<b>Записи на {d}:</b>\n"]
    for booking_id, time_str, service_text, client_name, status in rows:
        lines.append(f"#{booking_id} {time_str} — {service_text} ({client_name}, {status_map.get(status, status)})")

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="\n".join(lines),
        reply_markup=_kb_back_to_main(),
    )
    await callback.answer()


@admin_router.callback_query(F.data == "ad:pending")
async def ad_pending(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    pending = await db.get_pending_bookings_admin()
    if not pending:
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="Немає заявок у статусі <b>pending</b>.",
            reply_markup=_kb_back_to_main(),
        )
        await callback.answer()
        return

    lines = ["<b>Pending-заявки:</b>\n"]
    for bid, d_str, time_str, service_text, client_name in pending:
        lines.append(f"#{bid} {d_str} {time_str} — {service_text} ({client_name})")

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="\n".join(lines),
        reply_markup=_kb_back_to_main(),
    )
    await callback.answer()


@admin_router.callback_query(F.data == "ad:reports")
async def ad_reports(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>Звіти</b>\nОберіть період:",
        reply_markup=_kb_reports(),
    )
    await callback.answer()


def _period_dates(days_back: int) -> tuple[str, str]:
    today = date.today()
    start = today - timedelta(days=days_back)
    return start.isoformat(), today.isoformat()


async def _render_report(bot, chat_id: int, state: FSMContext, start_date: str, end_date: str, title: str):
    total, unique_clients = await db.get_report_overview_admin(start_date, end_date)
    details = await db.get_report_by_period_admin(start_date, end_date)

    if total == 0:
        await _ui_render(
            bot=bot,
            chat_id=chat_id,
            state=state,
            text=f"<b>{title}</b>\n\nЗаписів у цей період немає.",
            reply_markup=_kb_reports(),
        )
        return

    lines = [f"• {service_text}: {cnt} запис(ів)" for service_text, cnt in details]
    text = (
        f"<b>{title}</b>\n\n"
        f"Період: {start_date} – {end_date}\n"
        f"Записів: <b>{total}</b>\n"
        f"Унікальних клієнтів: <b>{unique_clients}</b>\n\n"
        f"Деталізація:\n" + "\n".join(lines)
    )
    await _ui_render(bot=bot, chat_id=chat_id, state=state, text=text, reply_markup=_kb_reports())


@admin_router.callback_query(F.data == "ad:r:today")
async def ad_report_today(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    d = date.today().isoformat()
    await _render_report(callback.bot, callback.message.chat.id, state, d, d, "Звіт за сьогодні")
    await callback.answer()


@admin_router.callback_query(F.data == "ad:r:week")
async def ad_report_week(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    start, end = _period_dates(7)
    await _render_report(callback.bot, callback.message.chat.id, state, start, end, "Звіт за останні 7 днів")
    await callback.answer()


@admin_router.callback_query(F.data == "ad:r:month")
async def ad_report_month(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    start, end = _period_dates(30)
    await _render_report(callback.bot, callback.message.chat.id, state, start, end, "Звіт за останні 30 днів")
    await callback.answer()


# =======================
#  SETTINGS (Interactive)
# =======================

@admin_router.callback_query(F.data == "ad:settings")
async def ad_settings(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    s = await db.get_shop_settings()
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>⚙️ Налаштування майстра</b>\nОберіть, що змінюємо:",
        reply_markup=_kb_settings_home(s),
    )
    await callback.answer()


@admin_router.callback_query(F.data == "ad:set:grid")
async def ad_settings_grid_menu(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    s = await db.get_shop_settings()
    cur = int(s.get("base_grid_minutes", 60))
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>🧱 Базова сітка</b>\nПоказувати базові слоти кожні (хв):",
        reply_markup=_kb_pick_int(cur, [30, 60, 90, 120], "ad:grid"),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:grid:"))
async def ad_settings_grid_set(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    minutes = int(callback.data.split(":")[2])
    try:
        await _set_shop_setting("base_grid_minutes", minutes)
    except Exception as e:
        log.exception("Failed to set base_grid_minutes: %s", e)
        await callback.answer("Не вдалося змінити сітку.", show_alert=True)
        return

    await callback.answer("Збережено ✅", show_alert=True)
    await ad_settings(callback, state)


@admin_router.callback_query(F.data == "ad:set:short_thr")
async def ad_settings_short_thr_menu(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    s = await db.get_shop_settings()
    cur = int(s.get("short_service_threshold_minutes", 40))
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>⚡ Поріг короткої послуги</b>\nЯкщо тривалість < цього значення — додаємо 1 додатковий слот у годині:",
        reply_markup=_kb_pick_int(cur, [20, 30, 35, 40, 45, 50], "ad:shortthr"),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:shortthr:"))
async def ad_settings_short_thr_set(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    minutes = int(callback.data.split(":")[2])
    try:
        await _set_shop_setting("short_service_threshold_minutes", minutes)
    except Exception as e:
        log.exception("Failed to set short_service_threshold_minutes: %s", e)
        await callback.answer("Не вдалося змінити поріг.", show_alert=True)
        return

    await callback.answer("Збережено ✅", show_alert=True)
    await ad_settings(callback, state)


@admin_router.callback_query(F.data == "ad:set:rest_short")
async def ad_settings_rest_short_menu(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    s = await db.get_shop_settings()
    cur = int(s.get("rest_minutes_after_short", 5))
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>🛑 Пауза після короткої</b>\nСкільки хвилин додавати після короткої послуги перед наступним слотом:",
        reply_markup=_kb_pick_int(cur, [0, 5, 10, 15], "ad:restshort"),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:restshort:"))
async def ad_settings_rest_short_set(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    minutes = int(callback.data.split(":")[2])
    try:
        await _set_shop_setting("rest_minutes_after_short", minutes)
    except Exception as e:
        log.exception("Failed to set rest_minutes_after_short: %s", e)
        await callback.answer("Не вдалося змінити паузу.", show_alert=True)
        return

    await callback.answer("Збережено ✅", show_alert=True)
    await ad_settings(callback, state)


@admin_router.callback_query(F.data == "ad:set:extra_round")
async def ad_settings_extra_round_menu(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    s = await db.get_shop_settings()
    cur = int(s.get("extra_round_minutes", 15))
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>🔁 Округлення додаткового слоту</b>\nДо яких хвилин округлювати offset (15 => 10:15, 20 => 10:20):",
        reply_markup=_kb_pick_int(cur, [5, 10, 15, 20, 30], "ad:exround"),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:exround:"))
async def ad_settings_extra_round_set(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    minutes = int(callback.data.split(":")[2])
    try:
        await _set_shop_setting("extra_round_minutes", minutes)
    except Exception as e:
        log.exception("Failed to set extra_round_minutes: %s", e)
        await callback.answer("Не вдалося змінити округлення.", show_alert=True)
        return

    await callback.answer("Збережено ✅", show_alert=True)
    await ad_settings(callback, state)


@admin_router.callback_query(F.data == "ad:set:lead")
async def ad_settings_lead_menu(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    s = await db.get_shop_settings()
    cur = int(s.get("min_lead_minutes", 0))
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>⏳ Мінімальний запас</b>\nСкільки хвилин до візиту не показувати слоти:",
        reply_markup=_kb_pick_int(cur, [0, 15, 30, 60, 120], "ad:lead"),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:lead:"))
async def ad_settings_lead_set(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    minutes = int(callback.data.split(":")[2])
    try:
        await _set_shop_setting("min_lead_minutes", minutes)
    except Exception as e:
        log.exception("Failed to set min_lead_minutes: %s", e)
        await callback.answer("Не вдалося змінити запас.", show_alert=True)
        return

    await callback.answer("Збережено ✅", show_alert=True)
    await ad_settings(callback, state)


@admin_router.callback_query(F.data == "ad:set:schedule")
async def ad_schedule(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    schedule = await db.get_weekly_schedule()
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>🗓 Графік по днях</b>\nНатисніть день, щоб змінити:",
        reply_markup=_kb_weekdays(schedule),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:sch:day:"))
async def ad_schedule_day(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    wd = int(callback.data.split(":")[3])
    info = await db.get_day_schedule(wd)
    if not info:
        await callback.answer("Немає даних дня.", show_alert=True)
        return

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=f"<b>{UA_WEEKDAYS[wd]}</b>\nНалаштування дня:",
        reply_markup=_kb_day_edit(wd, info["is_working"], info["work_start"], info["work_end"]),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:sch:toggle:"))
async def ad_schedule_toggle(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    wd = int(callback.data.split(":")[3])
    info = await db.get_day_schedule(wd)
    if not info:
        await callback.answer("Немає даних дня.", show_alert=True)
        return

    await db.set_day_schedule(wd, is_working=not info["is_working"])
    await callback.answer("Оновлено ✅", show_alert=True)
    await ad_schedule_day(callback, state)


@admin_router.callback_query(F.data.startswith("ad:sch:set:ws:"))
async def ad_schedule_pick_ws(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    wd = int(callback.data.split(":")[4])
    info = await db.get_day_schedule(wd)
    if not info:
        await callback.answer("Немає даних дня.", show_alert=True)
        return

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=f"<b>{UA_WEEKDAYS[wd]}</b>\nОберіть <b>початок</b>:",
        reply_markup=_kb_time_pick(wd, "ws", info["work_start"]),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:sch:set:we:"))
async def ad_schedule_pick_we(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    wd = int(callback.data.split(":")[4])
    info = await db.get_day_schedule(wd)
    if not info:
        await callback.answer("Немає даних дня.", show_alert=True)
        return

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=f"<b>{UA_WEEKDAYS[wd]}</b>\nОберіть <b>кінець</b>:",
        reply_markup=_kb_time_pick(wd, "we", info["work_end"]),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:sch:pick:"))
async def ad_schedule_apply_time(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    parts = callback.data.split(":")  # ad:sch:pick:{wd}:{field}:{hhmm}
    wd = int(parts[3])
    field = parts[4]  # ws / we
    hhmm = parts[5]
    t = f"{hhmm[:2]}:{hhmm[2:]}"

    info = await db.get_day_schedule(wd)
    if not info:
        await callback.answer("Немає даних дня.", show_alert=True)
        return

    ws = info["work_start"]
    we = info["work_end"]

    if field == "ws":
        if _time_to_minutes(t) >= _time_to_minutes(we):
            await callback.answer("Початок має бути раніше за кінець.", show_alert=True)
            return
        await db.set_day_schedule(wd, work_start=t)
    else:
        if _time_to_minutes(ws) >= _time_to_minutes(t):
            await callback.answer("Кінець має бути пізніше за початок.", show_alert=True)
            return
        await db.set_day_schedule(wd, work_end=t)

    await callback.answer("Збережено ✅", show_alert=True)
    await ad_schedule_day(callback, state)


@admin_router.callback_query(F.data == "ad:set:breaks")
async def ad_breaks(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    global_breaks = await _get_global_breaks()

    text_lines = ["<b>☕ Перерви (для всіх днів)</b>\n"]
    if not global_breaks:
        text_lines.append("Перерв поки немає.\n")
    else:
        text_lines.append("Поточні перерви:\n")
        for b in global_breaks:
            text_lines.append(f"• {b['start_time']}–{b['end_time']}")

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="\n".join(text_lines).strip(),
        reply_markup=_kb_breaks_list(global_breaks),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:br:add:"))
async def ad_breaks_add(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    parts = callback.data.split(":")
    st = f"{parts[3][:2]}:{parts[3][2:]}"
    et = f"{parts[4][:2]}:{parts[4][2:]}"
    try:
        await _add_break_global(st, et)
    except Exception as e:
        log.exception("Failed to add break: %s", e)
        await callback.answer("Не вдалося додати перерву.", show_alert=True)
        return

    await callback.answer("Додано ✅", show_alert=True)
    await ad_breaks(callback, state)


@admin_router.callback_query(F.data.startswith("ad:br:del:"))
async def ad_breaks_del(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    bid = int(callback.data.split(":")[3])
    try:
        await _remove_break(bid)
    except Exception as e:
        log.exception("Failed to remove break: %s", e)
        await callback.answer("Не вдалося видалити.", show_alert=True)
        return

    await callback.answer("Видалено ✅", show_alert=True)
    await ad_breaks(callback, state)


@admin_router.callback_query(F.data == "ad:set:dayoff")
async def ad_dayoff_list(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="<b>🚫 Вихідні по датах</b>\nОберіть дату (14 днів вперед):",
        reply_markup=_kb_dayoff_14days(),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:do:pick:"))
async def ad_dayoff_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    date_str = callback.data.split(":")[3]
    off = await db.is_day_off(date_str)
    text = (
        f"<b>{date_str}</b>\n\n"
        f"Статус: {'🚫 вихідний' if off else '✅ робочий'}\n"
        "Перемкнути?"
    )
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=text,
        reply_markup=_kb_dayoff_toggle(date_str, off),
    )
    await callback.answer()


@admin_router.callback_query(F.data.startswith("ad:do:toggle:"))
async def ad_dayoff_toggle(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    date_str = callback.data.split(":")[3]
    off = await db.is_day_off(date_str)
    if off:
        await db.remove_day_off(date_str)
        await callback.answer("Зроблено робочим ✅", show_alert=True)
    else:
        await db.add_day_off(date_str)
        await callback.answer("Зроблено вихідним 🚫", show_alert=True)

    await ad_dayoff_pick(callback, state)


# =======================
#  OLD TEXT COMMANDS (kept)
# =======================

@admin_router.message(Command("today"))
async def today_bookings(message: Message):
    if not is_admin(message.from_user.id):
        return
    await _send_bookings_for_date(message, date.today().isoformat())


@admin_router.message(Command("date"))
async def date_bookings(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.answer("Формат: /date YYYY-MM-DD")
        return

    await _send_bookings_for_date(message, parts[1])


@admin_router.message(Command("week"))
async def week_bookings_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    today_d = date.today()
    end_d = today_d + timedelta(days=6)
    start_str, end_str = today_d.isoformat(), end_d.isoformat()

    rows = await db.get_bookings_for_period_admin(start_str, end_str)
    if not rows:
        await message.answer(f"Записів на період {start_str} – {end_str} немає.")
        return

    status_map = {
        "pending": "очікує",
        "approved": "підтверджено",
        "completed": "завершено",
        "rejected": "відхилено",
        "cancelled_by_client": "скасовано клієнтом",
        "cancelled_by_admin": "скасовано майстром",
    }

    lines = []
    for d_str, time_str, service_text, client_name, status in rows:
        lines.append(f"{d_str} {time_str} — {service_text} ({client_name}, {status_map.get(status, status)})")

    await message.answer(f"<b>Записи на {start_str} – {end_str}:</b>\n\n" + "\n".join(lines), parse_mode="HTML")


async def _send_bookings_for_date(message: Message, date_str: str):
    rows = await db.get_bookings_for_date_admin(date_str)
    if not rows:
        await message.answer(f"На {date_str} записів немає.")
        return

    status_map = {
        "pending": "очікує",
        "approved": "підтверджено",
        "completed": "завершено",
        "rejected": "відхилено",
        "cancelled_by_client": "скасовано клієнтом",
        "cancelled_by_admin": "скасовано майстром",
    }

    lines = []
    for booking_id, time_str, service_text, client_name, status in rows:
        lines.append(f"#{booking_id} {time_str} — {service_text} ({client_name}, {status_map.get(status, status)})")

    await message.answer(f"Записи на {date_str}:\n\n" + "\n".join(lines))


# =======================
#  Approve / Reject (callbacks)
# =======================

async def _booking_occupy_minutes(duration_minutes: int, shop: dict) -> int:
    short_thr = int(shop.get("short_service_threshold_minutes", 40))
    rest_short = int(shop.get("rest_minutes_after_short", 5))
    extra_round = int(shop.get("extra_round_minutes", 15))

    d = int(duration_minutes)
    if d < short_thr:
        return _ceil_to_step(d + rest_short, extra_round)
    return d


@admin_router.callback_query(F.data.startswith("approve:"))
async def approve_booking(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    booking_id = int(callback.data.split(":")[1])
    info = await db.get_booking_with_client_admin(booking_id)
    if not info:
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    (
        _bid, date_str, time_str, status,
        service_text, price_text, duration_minutes,
        client_tg_id, client_name
    ) = info

    if status in ("approved", "completed"):
        await callback.answer("Запис уже підтверджений/завершений.", show_alert=True)
        return
    if status in ("rejected", "cancelled_by_client", "cancelled_by_admin"):
        await callback.answer("Запис уже неактивний.", show_alert=True)
        return

    shop = await db.get_shop_settings()
    cand_s = _time_to_minutes(time_str)
    cand_e = cand_s + await _booking_occupy_minutes(duration_minutes, shop)

    active = await db.get_active_bookings_for_date(date.fromisoformat(date_str))
    for b in active:
        if int(b.get("id")) == booking_id:
            continue
        bs = _time_to_minutes(b["time"])
        occ = b.get("occupy_minutes")
        if occ is None:
            occ = await _booking_occupy_minutes(int(b["duration_minutes"]), shop)
        be = bs + int(occ)
        if _overlap(cand_s, cand_e, bs, be):
            await db.update_booking_status(booking_id, "rejected")
            await callback.answer("Конфлікт по часу. Запит відхилено.", show_alert=True)

            if callback.message:
                try:
                    await callback.message.edit_text(callback.message.text + "\n\n❌ Автоматично відхилено (конфлікт).")
                except Exception:
                    pass

            try:
                await callback.bot.send_message(
                    chat_id=client_tg_id,
                    text="На жаль, цей час уже зайнятий. Оберіть, будь ласка, інший час або день."
                )
            except Exception:
                pass
            return

    await db.update_booking_status(booking_id, "approved")
    await callback.answer("Запис підтверджено ✅", show_alert=True)

    if callback.message:
        try:
            await callback.message.edit_text(callback.message.text + "\n\n✅ Підтверджено.")
        except Exception:
            pass

    try:
        end_time = _minutes_to_time(_time_to_minutes(time_str) + int(duration_minutes))
        await callback.bot.send_message(
            chat_id=client_tg_id,
            text=(
                f"Ваш запис підтверджено ✅\n\n"
                f"Послуга: {service_text}\n"
                f"Дата: {date_str}\n"
                f"Час: {time_str}–{end_time}\n"
                f"Барбершоп: {settings.shop_name}, барбер {settings.master_name}."
            )
        )
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("reject:"))
async def reject_booking(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Недостатньо прав.", show_alert=True)
        return

    booking_id = int(callback.data.split(":")[1])
    info = await db.get_booking_with_client_admin(booking_id)
    if not info:
        await callback.answer("Запис не знайдено.", show_alert=True)
        return

    (
        _bid, date_str, time_str, status,
        service_text, price_text, duration_minutes,
        client_tg_id, client_name
    ) = info

    if status in ("rejected", "cancelled_by_client", "cancelled_by_admin"):
        await callback.answer("Запис уже скасований/відхилений.", show_alert=True)
        return

    await db.update_booking_status(booking_id, "rejected")
    await callback.answer("Запит відхилено ❌", show_alert=True)

    if callback.message:
        try:
            await callback.message.edit_text(callback.message.text + "\n\n❌ Відхилено.")
        except Exception:
            pass

    try:
        await callback.bot.send_message(
            chat_id=client_tg_id,
            text="На жаль, ваш запит на запис було відхилено ❌\n\nСпробуйте інший час або день."
        )
    except Exception:
        pass


# =======================
#  Clients + Broadcast
# =======================

@admin_router.message(Command("clients"))
async def clients_list_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    rows = await db.get_clients_with_stats_admin(limit=50)
    if not rows:
        await message.answer("Клієнтів поки немає.")
        return

    lines = []
    for tg_id, full_name, phone, total_all, total_approved in rows:
        p = phone or "-"
        lines.append(
            f"{full_name}\n"
            f"tg_id: <code>{tg_id}</code>, телефон: {p}, записів: {total_all}, підтверджених: {total_approved}\n"
        )

    await message.answer("<b>Клієнти:</b>\n\n" + "\n".join(lines), parse_mode="HTML")


@admin_router.message(Command("client"))
async def client_stats_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /client tg_id або /client @username")
        return

    ident = parts[1].strip()
    tg_id = None
    username = None

    if ident.startswith("@"):
        username = ident.lstrip("@")
    else:
        try:
            tg_id = int(ident)
        except ValueError:
            await message.answer("Невірний формат. Використовуйте tg_id або @username.")
            return

    row = await db.get_client_stats_admin(tg_id=tg_id, username=username)
    if not row:
        await message.answer("Клієнта не знайдено.")
        return

    full_name, phone, total_all, total_approved, first_date, last_date = row
    p = phone or "-"

    text = (
        "<b>Статистика по клієнту</b>\n\n"
        f"Ім'я: {full_name}\n"
        f"Телефон: {p}\n\n"
        f"Усього записів: <b>{total_all}</b>\n"
        f"Підтверджених: <b>{total_approved}</b>\n"
    )
    if first_date and last_date:
        text += f"Перший запис: {first_date}\nОстанній запис: {last_date}"

    await message.answer(text, parse_mode="HTML")


@admin_router.message(Command("broadcast"))
async def broadcast_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Формат: /broadcast Текст повідомлення")
        return

    text_to_send = parts[1].strip()
    if not text_to_send:
        await message.answer("Текст розсилки не може бути порожнім.")
        return

    tg_ids = await db.get_all_client_tg_ids()
    if not tg_ids:
        await message.answer("Немає клієнтів для розсилки.")
        return

    sent = 0
    failed = 0
    for uid in tg_ids:
        try:
            await message.bot.send_message(chat_id=uid, text=text_to_send)
            sent += 1
        except Exception:
            failed += 1

    await message.answer(
        f"Розсилку завершено.\nНадіслано: <b>{sent}</b>\nПомилок: <b>{failed}</b>",
        parse_mode="HTML"
    )


# =======================
#  Debug fallback (ADMIN ONLY)
# =======================

@admin_router.callback_query(
    F.data.startswith("ad:") | F.data.startswith("approve:") | F.data.startswith("reject:")
)
async def _debug_unhandled_admin_callbacks(callback: CallbackQuery):
    await callback.answer("Невідомий ADMIN callback. Дивись лог.", show_alert=True)
    log.warning("UNHANDLED ADMIN CALLBACK: %s", callback.data)
