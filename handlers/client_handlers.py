from __future__ import annotations

import re
import time
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest

from config import settings
from states import BookingState
from keyboards import (
    client_main_menu_inline,
    services_keyboard,
    booking_times_keyboard,
    client_confirm_keyboard,
)

from database import (
    upsert_user,
    update_user_phone,
    create_booking_atomic,
    get_active_bookings_for_date,
    count_client_active_requests_for_day,
    get_client_bookings,
    cancel_booking_by_client,

    # master schedule
    get_shop_settings,
    get_day_schedule,
    get_breaks_for_weekday,
    is_day_off,
)

log = logging.getLogger(__name__)

client_router = Router()

PHONE_RE = re.compile(r"^380\d{9}$")

SERVICE_CATALOG = {
    "lining": {"name": "Окантовка", "price_text": "100 грн", "duration": 15},
    "short": {"name": "Коротка стрижка", "price_text": "350 грн", "duration": 40},
    "medium": {"name": "Середня стрижка", "price_text": "350–400 грн", "duration": 50},
    "long": {"name": "Подовжена стрижка", "price_text": "450 грн", "duration": 60},
    "beard": {"name": "Борода", "price_text": "150 грн", "duration": 30},
}

BOOKING_COOLDOWN_SECONDS = 30
_last_booking_start: dict[int, float] = {}


# =========================
#  UI helpers (single-screen)
# =========================

async def _try_delete_message(msg: Message) -> None:
    """Видаляємо повідомлення користувача, щоб не засмічувати чат. Не критично при помилці."""
    try:
        await msg.delete()
    except Exception:
        return


async def _clear_flow_keep_ui(state: FSMContext) -> None:
    """
    Очищаємо FSM-стан, але зберігаємо ui_msg_id,
    щоб бот НЕ створював нове повідомлення-екран.
    """
    data = await state.get_data()
    ui_msg_id = data.get("ui_msg_id")
    await state.clear()
    if isinstance(ui_msg_id, int) and ui_msg_id > 0:
        await state.update_data(ui_msg_id=ui_msg_id)


async def _ui_get_or_create_screen(message: Message, state: FSMContext) -> int:
    data = await state.get_data()
    ui_msg_id = data.get("ui_msg_id")
    if isinstance(ui_msg_id, int) and ui_msg_id > 0:
        return ui_msg_id

    sent = await message.answer("Завантаження…")
    await state.update_data(ui_msg_id=sent.message_id)
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
    """
    Рендеримо "екран" в одному повідомленні (edit),
    якщо не виходить — відправляємо нове і запам'ятовуємо його id.
    """
    data = await state.get_data()
    ui_msg_id = data.get("ui_msg_id")

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
            # Напр.: message is not modified / message to edit not found
            log.warning("UI edit failed: %s", e)
        except Exception as e:
            log.exception("UI edit unexpected error: %s", e)

    sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    await state.update_data(ui_msg_id=sent.message_id)


def _normalize_phone(raw: str) -> str:
    return re.sub(r"\D", "", raw or "")


def _is_valid_ua_phone_380(raw: str) -> bool:
    return bool(PHONE_RE.match(_normalize_phone(raw)))


def _time_to_minutes(hhmm: str) -> int:
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


def _minutes_to_time(mm: int) -> str:
    h = mm // 60
    m = mm % 60
    return f"{h:02d}:{m:02d}"


def _ceil_to_step(value: int, step: int) -> int:
    if step <= 0:
        return value
    return ((value + step - 1) // step) * step


def _intervals_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return a_start < b_end and b_start < a_end


def _back_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="cl:nav:menu")]]
    )


# =========================
#  Working date filtering
# =========================

async def _is_working_date(d: date) -> bool:
    if await is_day_off(d.isoformat()):
        return False

    info = await get_day_schedule(d.weekday())
    if not info:
        return True  # fallback
    return bool(info.get("is_working", True))


async def _booking_dates_keyboard_filtered(days_ahead: int = 7) -> InlineKeyboardMarkup:
    today = date.today()
    dates: list[date] = []

    for i in range(days_ahead):
        d = today + timedelta(days=i)
        if await _is_working_date(d):
            dates.append(d)

    rows: list[list[InlineKeyboardButton]] = []

    if not dates:
        rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="cl:nav:menu")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    row: list[InlineKeyboardButton] = []
    for d in dates:
        label = d.strftime("%d.%m")
        row.append(InlineKeyboardButton(text=label, callback_data=f"cl:book:date:{d.isoformat()}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="cl:nav:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
#  Slot generation (NEW LOGIC)
# =========================

async def _get_work_window_minutes(target_date: date) -> tuple[int, int] | None:
    day_info = await get_day_schedule(target_date.weekday())
    if day_info and not day_info.get("is_working", True):
        return None

    if day_info and day_info.get("work_start") and day_info.get("work_end"):
        start_day = _time_to_minutes(day_info["work_start"])
        end_day = _time_to_minutes(day_info["work_end"])
    else:
        start_day = settings.work_start_hour * 60
        end_day = settings.work_end_hour * 60

    if end_day <= start_day:
        return None
    return start_day, end_day


async def _get_break_intervals(target_date: date) -> list[tuple[int, int]]:
    breaks_rows = await get_breaks_for_weekday(target_date.weekday())
    out: list[tuple[int, int]] = []
    for br in breaks_rows:
        st = br.get("start_time")
        et = br.get("end_time")
        if not st or not et:
            continue
        bs = _time_to_minutes(st)
        be = _time_to_minutes(et)
        if be > bs:
            out.append((bs, be))
    return out


def _booking_occupy_minutes(
    duration: int,
    *,
    short_threshold: int,
    rest_after_short: int,
    extra_round: int,
) -> int:
    duration = int(duration)
    if duration < int(short_threshold):
        return _ceil_to_step(duration + int(rest_after_short), int(extra_round))
    return duration


async def _generate_free_starts(
    target_date: date,
    duration_minutes: int,
    active: list[dict],
) -> list[str]:
    if not await _is_working_date(target_date):
        return []

    shop = await get_shop_settings()
    base_grid = int(shop.get("base_grid_minutes", 60))
    short_threshold = int(shop.get("short_service_threshold_minutes", 40))
    rest_after_short = int(shop.get("rest_minutes_after_short", 5))
    extra_round = int(shop.get("extra_round_minutes", 15))
    lead = int(shop.get("min_lead_minutes", 0))

    work = await _get_work_window_minutes(target_date)
    if not work:
        return []
    start_day, end_day = work

    busy: list[tuple[int, int]] = []
    for b in active:
        s = _time_to_minutes(b["time"])
        dur = int(b["duration_minutes"])
        occ = _booking_occupy_minutes(
            dur,
            short_threshold=short_threshold,
            rest_after_short=rest_after_short,
            extra_round=extra_round
        )
        busy.append((s, s + occ))

    breaks = await _get_break_intervals(target_date)

    cutoff = start_day
    if target_date == date.today():
        now = datetime.now()
        now_min = now.hour * 60 + now.minute
        cutoff = max(cutoff, now_min + lead)

    candidates: list[int] = []

    first = (start_day // base_grid) * base_grid
    if first < start_day:
        first += base_grid

    t = first
    while t < end_day:
        candidates.append(t)

        if int(duration_minutes) < short_threshold:
            offset = _ceil_to_step(int(duration_minutes) + rest_after_short, extra_round)
            extra_start = t + offset
            if extra_start < t + base_grid and extra_start + int(duration_minutes) <= end_day:
                candidates.append(extra_start)

        t += base_grid

    filtered: list[int] = []
    for s in sorted(set(candidates)):
        if s < start_day or s + int(duration_minutes) > end_day:
            continue
        if target_date == date.today() and s < cutoff:
            continue
        filtered.append(s)

    free: list[str] = []
    for s in filtered:
        e = s + int(duration_minutes)

        ok = True
        for bs, be in busy:
            if _intervals_overlap(s, e, bs, be):
                ok = False
                break
        if not ok:
            continue

        for bs, be in breaks:
            if _intervals_overlap(s, e, bs, be):
                ok = False
                break
        if not ok:
            continue

        free.append(_minutes_to_time(s))

    return free


# =======================
#  START + MAIN MENU
# =======================

@client_router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await _clear_flow_keep_ui(state)
    await upsert_user(message.from_user.id, message.from_user.full_name)
    await _try_delete_message(message)

    await _ui_get_or_create_screen(message, state)
    await _ui_render(
        bot=message.bot,
        chat_id=message.chat.id,
        state=state,
        text="Привіт! Це бот запису до барбера 💈\nОберіть дію нижче.",
        reply_markup=client_main_menu_inline(),
    )


@client_router.callback_query(F.data == "cl:nav:menu")
async def to_menu(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    await _clear_flow_keep_ui(state)
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="Оберіть дію нижче:",
        reply_markup=client_main_menu_inline(),
    )
    await callback.answer()


@client_router.callback_query(F.data == "cl:menu:contacts")
async def menu_contacts(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    await _clear_flow_keep_ui(state)

    text = (
        f"💈 <b>Барбершоп {settings.shop_name}</b>\n\n"
        "📍 <b>Адреса:</b>\n"
        "м. Любомль, вул. ________\n\n"
        "📞 <b>Телефон:</b>\n"
        "+380 XX XXX XX XX\n\n"
        "📸 <b>Instagram:</b>\n"
        "https://instagram.com/cyrulnya__\n\n"
        "🕒 <b>Графік роботи:</b>\n"
        "Актуальний графік задає майстер у налаштуваннях."
    )

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=text,
        reply_markup=_back_menu_kb(),
    )
    await callback.answer()


# =======================
#  BOOKING FLOW
# =======================

@client_router.callback_query(F.data == "cl:menu:book")
async def start_booking(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    now = time.monotonic()
    last = _last_booking_start.get(callback.from_user.id, 0.0)
    if now - last < BOOKING_COOLDOWN_SECONDS:
        wait = int(BOOKING_COOLDOWN_SECONDS - (now - last))
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text=f"Зачекайте <b>{wait} с</b> і спробуйте ще раз.",
            reply_markup=_back_menu_kb(),
        )
        await callback.answer()
        return

    _last_booking_start[callback.from_user.id] = now

    await _clear_flow_keep_ui(state)
    await state.set_state(BookingState.choosing_date)

    kb = await _booking_dates_keyboard_filtered(days_ahead=7)
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="Оберіть дату для запису:",
        reply_markup=kb,
    )
    await callback.answer()


@client_router.callback_query(BookingState.choosing_date, F.data.startswith("cl:book:date:"))
async def choose_date(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    date_str = callback.data.split("cl:book:date:", 1)[1]
    target = date.fromisoformat(date_str)

    if not await _is_working_date(target):
        kb = await _booking_dates_keyboard_filtered(days_ahead=7)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="Цей день зараз <b>неробочий</b>. Оберіть іншу дату:",
            reply_markup=kb,
        )
        await callback.answer()
        return

    await state.update_data(date_str=date_str)
    await state.set_state(BookingState.choosing_service)

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=f"Оберіть послугу на <b>{target.strftime('%d.%m.%Y')}</b>:",
        reply_markup=services_keyboard(),
    )
    await callback.answer()


@client_router.callback_query(BookingState.choosing_service, F.data.startswith("cl:book:svc:"))
async def choose_service(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    code = callback.data.split("cl:book:svc:", 1)[1]
    svc = SERVICE_CATALOG.get(code)
    if not svc:
        await callback.answer("Невідома послуга.", show_alert=True)
        return

    data = await state.get_data()
    date_str = data.get("date_str")
    if not date_str:
        await _clear_flow_keep_ui(state)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="Стан загубився. Повертаю в меню.",
            reply_markup=client_main_menu_inline(),
        )
        await callback.answer()
        return

    target = date.fromisoformat(date_str)

    if not await _is_working_date(target):
        kb = await _booking_dates_keyboard_filtered(days_ahead=7)
        await state.set_state(BookingState.choosing_date)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="День став <b>неробочим</b>. Оберіть іншу дату:",
            reply_markup=kb,
        )
        await callback.answer()
        return

    duration = int(svc["duration"])

    active = await get_active_bookings_for_date(target)
    free_starts = await _generate_free_starts(target, duration, active)

    if not free_starts:
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text=(
                f"На <b>{target.strftime('%d.%m.%Y')}</b> немає доступного часу для:\n"
                f"— <b>{svc['name']}</b> ({svc['price_text']}, ~{duration} хв)\n\n"
                f"Оберіть іншу послугу або іншу дату."
            ),
            reply_markup=services_keyboard(),
        )
        await callback.answer()
        return

    await state.update_data(
        service_code=code,
        service_text=svc["name"],
        price_text=svc["price_text"],
        duration_minutes=duration,
    )
    await state.set_state(BookingState.choosing_time)

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=(
            f"Оберіть час на <b>{target.strftime('%d.%m.%Y')}</b>:\n"
            f"Послуга: <b>{svc['name']}</b> — {svc['price_text']} (~{duration} хв)"
        ),
        reply_markup=booking_times_keyboard(date_str, free_starts),
    )
    await callback.answer()


@client_router.callback_query(BookingState.choosing_time, F.data.startswith("cl:book:time:"))
async def choose_time(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    payload = callback.data.split("cl:book:time:", 1)[1]
    date_str, time_str = payload.split(":", 1)

    await state.update_data(time_str=time_str)
    await state.set_state(BookingState.waiting_phone)

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=(
            "Вкажіть ваш номер телефону.\n\n"
            "<b>Формат:</b> 12 цифр, починається з 380.\n"
            "Приклад: <code>380971234567</code>"
        ),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="cl:nav:times")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="cl:nav:menu")],
            ]
        ),
    )
    await callback.answer()


@client_router.message(BookingState.waiting_phone)
async def get_phone(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    await _try_delete_message(message)

    if not _is_valid_ua_phone_380(raw):
        await _ui_render(
            bot=message.bot,
            chat_id=message.chat.id,
            state=state,
            text=(
                "Невірний номер телефону.\n\n"
                "<b>Формат:</b> 12 цифр, починається з 380.\n"
                "Приклад: <code>380971234567</code>\n\n"
                "Введіть номер ще раз:"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="cl:nav:times")],
                    [InlineKeyboardButton(text="🏠 Меню", callback_data="cl:nav:menu")],
                ]
            ),
        )
        return

    phone = _normalize_phone(raw)
    await update_user_phone(message.from_user.id, phone)
    await state.update_data(phone=phone)

    await state.set_state(BookingState.waiting_full_name)
    await _ui_render(
        bot=message.bot,
        chat_id=message.chat.id,
        state=state,
        text="Напишіть ваше ім'я та прізвище (як вас підписати в записі).",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="cl:nav:times")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="cl:nav:menu")],
            ]
        ),
    )


@client_router.message(BookingState.waiting_full_name)
async def get_full_name(message: Message, state: FSMContext):
    full_name = (message.text or "").strip()
    await _try_delete_message(message)

    if not full_name:
        await _ui_render(
            bot=message.bot,
            chat_id=message.chat.id,
            state=state,
            text="Будь ласка, введіть ім'я та прізвище текстом.",
            reply_markup=None,
        )
        return

    await state.update_data(client_name=full_name)

    data = await state.get_data()
    date_str = data["date_str"]
    time_str = data["time_str"]
    service_text = data["service_text"]
    price_text = data["price_text"]
    duration_minutes = int(data["duration_minutes"])
    phone = data["phone"]

    target = date.fromisoformat(date_str)
    end_time = _minutes_to_time(_time_to_minutes(time_str) + duration_minutes)

    text = (
        "<b>Перевірте дані запису:</b>\n\n"
        f"📅 Дата: <b>{target.strftime('%d.%m.%Y')}</b>\n"
        f"🕒 Час: <b>{time_str}–{end_time}</b>\n"
        f"✂️ Послуга: <b>{service_text}</b>\n"
        f"⏱ Тривалість: ~{duration_minutes} хв\n"
        f"💳 Вартість: {price_text}\n"
        f"👤 ПІБ: {full_name}\n"
        f"📞 Телефон: {phone}\n\n"
        "Підтвердити запис?"
    )

    await state.set_state(BookingState.confirming)
    await _ui_render(
        bot=message.bot,
        chat_id=message.chat.id,
        state=state,
        text=text,
        reply_markup=client_confirm_keyboard(),
    )


@client_router.callback_query(BookingState.confirming, F.data.in_(["cl:book:confirm", "cl:book:cancel"]))
async def confirm_booking(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    if callback.data == "cl:book:cancel":
        await _clear_flow_keep_ui(state)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="Запис скасовано. Якщо потрібно — почніть запис заново.",
            reply_markup=client_main_menu_inline(),
        )
        await callback.answer()
        return

    today_str = date.today().isoformat()
    active_today = await count_client_active_requests_for_day(callback.from_user.id, today_str)
    if active_today >= 1:
        await _clear_flow_keep_ui(state)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text=(
                "У вас вже є активна заявка на сьогодні (очікує або підтверджена).\n"
                "Дочекайтеся відповіді майстра або скасуйте попередню заявку."
            ),
            reply_markup=client_main_menu_inline(),
        )
        await callback.answer()
        return

    data = await state.get_data()
    date_str = data["date_str"]
    time_str = data["time_str"]
    duration_minutes = int(data["duration_minutes"])
    service_code = data["service_code"]
    service_text = data["service_text"]
    price_text = data["price_text"]
    phone = data["phone"]
    client_name = data["client_name"]

    target = date.fromisoformat(date_str)

    if not await _is_working_date(target):
        await _clear_flow_keep_ui(state)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="На жаль, день став неробочим. Оберіть іншу дату/час.",
            reply_markup=client_main_menu_inline(),
        )
        await callback.answer()
        return

    shop = await get_shop_settings()
    short_threshold = int(shop.get("short_service_threshold_minutes", 40))
    rest_after_short = int(shop.get("rest_minutes_after_short", 5))
    extra_round = int(shop.get("extra_round_minutes", 15))

    occupy = _booking_occupy_minutes(
        duration_minutes,
        short_threshold=short_threshold,
        rest_after_short=rest_after_short,
        extra_round=extra_round,
    )

    try:
        booking_id = await create_booking_atomic(
            client_id=callback.from_user.id,
            date_str=date_str,
            time_str=time_str,
            duration_minutes=duration_minutes,
            service_code=service_code,
            service_text=service_text,
            price_text=price_text,
            client_name=client_name,
            phone=phone,
            status="pending",
            occupy_minutes=occupy,
        )
    except ValueError:
        await _clear_flow_keep_ui(state)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="На жаль, цей час щойно зайняли. Оберіть інший час.",
            reply_markup=client_main_menu_inline(),
        )
        await callback.answer()
        return
    except Exception as e:
        log.exception("create_booking_atomic failed: %s", e)
        await _clear_flow_keep_ui(state)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="Сталася помилка при створенні запису. Спробуйте пізніше.",
            reply_markup=client_main_menu_inline(),
        )
        await callback.answer()
        return

    await _clear_flow_keep_ui(state)

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="Ваш запит на запис надіслано майстру. Очікуйте підтвердження 💈",
        reply_markup=client_main_menu_inline(),
    )

    # повідомлення адмінам
    from keyboards import admin_booking_decision_keyboard

    end_time = _minutes_to_time(_time_to_minutes(time_str) + duration_minutes)
    text_for_admin = (
        "<b>Нова заявка на запис:</b>\n\n"
        f"💈 {settings.shop_name} / {settings.master_name}\n"
        f"📅 Дата: <b>{target.strftime('%d.%m.%Y')}</b>\n"
        f"🕒 Час: <b>{time_str}–{end_time}</b>\n"
        f"✂️ Послуга: <b>{service_text}</b>\n"
        f"⏱ Тривалість: ~{duration_minutes} хв\n"
        f"💳 Вартість: {price_text}\n"
        f"👤 ПІБ: {client_name}\n"
        f"📞 Телефон: {phone}\n"
        f"ID заявки: <code>{booking_id}</code>"
    )

    for admin_id in settings.admin_ids:
        try:
            await callback.bot.send_message(
                chat_id=admin_id,
                text=text_for_admin,
                reply_markup=admin_booking_decision_keyboard(booking_id),
                parse_mode="HTML",
            )
        except Exception as e:
            log.warning("Failed to notify admin %s: %s", admin_id, e)

    await callback.answer()


# =======================
#  NAVIGATION (CLIENT)
# =======================

@client_router.callback_query(F.data == "cl:nav:dates")
async def nav_dates(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    await state.set_state(BookingState.choosing_date)
    kb = await _booking_dates_keyboard_filtered(days_ahead=7)
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="Оберіть дату для запису:",
        reply_markup=kb,
    )
    await callback.answer()


@client_router.callback_query(F.data == "cl:nav:svc")
async def nav_services(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    data = await state.get_data()
    date_str = data.get("date_str")
    if not date_str:
        await _clear_flow_keep_ui(state)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="Повертаю в меню.",
            reply_markup=client_main_menu_inline(),
        )
        await callback.answer()
        return

    target = date.fromisoformat(date_str)
    if not await _is_working_date(target):
        await state.set_state(BookingState.choosing_date)
        kb = await _booking_dates_keyboard_filtered(days_ahead=7)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="День став <b>неробочим</b>. Оберіть іншу дату:",
            reply_markup=kb,
        )
        await callback.answer()
        return

    await state.set_state(BookingState.choosing_service)
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=f"Оберіть послугу на <b>{target.strftime('%d.%m.%Y')}</b>:",
        reply_markup=services_keyboard(),
    )
    await callback.answer()


@client_router.callback_query(F.data == "cl:nav:times")
async def nav_times(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    data = await state.get_data()
    date_str = data.get("date_str")
    service_code = data.get("service_code")

    if not date_str or not service_code or service_code not in SERVICE_CATALOG:
        await _clear_flow_keep_ui(state)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="Повертаю в меню.",
            reply_markup=client_main_menu_inline(),
        )
        await callback.answer()
        return

    target = date.fromisoformat(date_str)
    if not await _is_working_date(target):
        await state.set_state(BookingState.choosing_date)
        kb = await _booking_dates_keyboard_filtered(days_ahead=7)
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="День став <b>неробочим</b>. Оберіть іншу дату:",
            reply_markup=kb,
        )
        await callback.answer()
        return

    svc = SERVICE_CATALOG[service_code]
    duration = int(svc["duration"])

    active = await get_active_bookings_for_date(target)
    free_starts = await _generate_free_starts(target, duration, active)

    await state.set_state(BookingState.choosing_time)
    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text=(
            f"Оберіть час на <b>{target.strftime('%d.%m.%Y')}</b>:\n"
            f"Послуга: <b>{svc['name']}</b> — {svc['price_text']} (~{duration} хв)"
        ),
        reply_markup=booking_times_keyboard(date_str, free_starts),
    )
    await callback.answer()


# =======================
#  MY BOOKINGS (INLINE)
# =======================

@client_router.callback_query(F.data == "cl:menu:my")
async def my_bookings(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    await _clear_flow_keep_ui(state)
    bookings = await get_client_bookings(callback.from_user.id, limit=10)

    if not bookings:
        await _ui_render(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            state=state,
            text="У вас поки немає записів.",
            reply_markup=_back_menu_kb(),
        )
        await callback.answer()
        return

    status_map = {
        "pending": "⏳ Очікує підтвердження",
        "approved": "✅ Підтверджено",
        "completed": "🏁 Виконано",
        "rejected": "❌ Відхилено",
        "cancelled_by_client": "🚫 Скасовано вами",
        "cancelled_by_admin": "🚫 Скасовано майстром",
    }

    lines = ["<b>📋 Ваші останні записи:</b>\n"]
    for b in bookings:
        d = date.fromisoformat(b["date"])
        t = b["time"]
        dur = int(b["duration_minutes"])
        end_time = _minutes_to_time(_time_to_minutes(t) + dur)
        lines.append(
            "──────────────\n"
            f"📅 <b>{d.strftime('%d.%m.%Y')}</b>\n"
            f"🕒 {t}–{end_time}\n"
            f"✂️ {b['service_text']} ({b['price_text']})\n"
            f"⏱ ~{dur} хв\n"
            f"Статус: {status_map.get(b['status'], b['status'])}\n"
            f"ID: <code>{b['id']}</code>\n"
        )

    rows: list[list[InlineKeyboardButton]] = []
    for b in bookings:
        if b["status"] in ("pending", "approved"):
            rows.append([InlineKeyboardButton(text=f"❌ Скасувати ID {b['id']}", callback_data=f"cl:my:cancel:{b['id']}")])

    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="cl:nav:menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    await _ui_render(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        state=state,
        text="\n".join(lines).strip(),
        reply_markup=kb,
    )
    await callback.answer()


@client_router.callback_query(F.data.startswith("cl:my:cancel:"))
async def my_cancel_booking(callback: CallbackQuery, state: FSMContext):
    if not callback.message:
        await callback.answer()
        return

    booking_id = int(callback.data.split("cl:my:cancel:", 1)[1])

    ok = await cancel_booking_by_client(booking_id, callback.from_user.id)
    if not ok:
        await callback.answer("Не вдалося скасувати (можливо вже неактивний).", show_alert=True)
        return

    await callback.answer("Запис скасовано ✅", show_alert=True)
    await my_bookings(callback, state)
