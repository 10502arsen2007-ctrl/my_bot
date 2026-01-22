import calendar
from datetime import date, timedelta
from typing import List, Optional

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

UA_WEEKDAYS_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]

UA_MONTHS = [
    "",
    "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
]

# ==========================================================
# CALLBACK DATA CONVENTION (ЄДИНИЙ СТАНДАРТ)
# ==========================================================
# CLIENT:
#   cl:menu
#   cl:contacts
#   cl:my
#   cl:book:date:YYYY-MM-DD
#   cl:book:svc:<code>
#   cl:book:time:YYYY-MM-DD:HH:MM
#   cl:book:confirm
#   cl:book:cancel
#   cl:my:cancel:<booking_id>
#   cl:nav:dates | cl:nav:svc | cl:nav:times | cl:nav:menu
#
# ADMIN:
#   approve:<id> / reject:<id>        (сумісно з admin_handlers.py)
#   admin_cal:day:YYYY-MM-DD
#   admin_cal:month:YYYY-MM
#   admin_cal:noop
#   report:today / report:this_week / report:this_month
# ==========================================================


# =====================
#  CLIENT (INLINE UI)
# =====================

def client_main_menu_inline() -> InlineKeyboardMarkup:
    """Головне меню клієнта через inline-кнопки (не створює повідомлень користувача)."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💈 Записатися", callback_data="cl:menu:book")],
            [InlineKeyboardButton(text="📋 Мої записи", callback_data="cl:menu:my")],
            [InlineKeyboardButton(text="ℹ️ Контакти", callback_data="cl:menu:contacts")],
        ]
    )


def client_nav_row(
    back_to: Optional[str] = None,
    *,
    include_menu: bool = True,
    include_cancel: bool = False,
) -> List[InlineKeyboardButton]:
    """
    Універсальний ряд навігації для клієнтських екранів.
    back_to: 'dates' | 'svc' | 'times' | 'menu' (або None)
    """
    row: List[InlineKeyboardButton] = []

    if back_to:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cl:nav:{back_to}"))

    if include_menu:
        row.append(InlineKeyboardButton(text="🏠 Меню", callback_data="cl:nav:menu"))

    if include_cancel:
        row.append(InlineKeyboardButton(text="❌ Скасувати", callback_data="cl:book:cancel"))

    return row


def booking_dates_keyboard(days_ahead: int = 7) -> InlineKeyboardMarkup:
    today = date.today()
    buttons: list[list[InlineKeyboardButton]] = []

    row: list[InlineKeyboardButton] = []
    for i in range(days_ahead):
        d = today + timedelta(days=i)
        wd = UA_WEEKDAYS_SHORT[d.weekday()]
        text = f"{d.strftime('%d.%m')} ({wd})"
        cb = f"cl:book:date:{d.isoformat()}"
        row.append(InlineKeyboardButton(text=text, callback_data=cb))

        if len(row) == 3:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append(client_nav_row(back_to="menu", include_menu=True, include_cancel=False))
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def services_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📌 Окантовка — 100 грн (10–15 хв)", callback_data="cl:book:svc:lining")],
            [InlineKeyboardButton(text="✂️ Коротка стрижка — 350 грн (30–40 хв)", callback_data="cl:book:svc:short")],
            [InlineKeyboardButton(text="✂️ Середня стрижка — 350–400 грн (40–50 хв)", callback_data="cl:book:svc:medium")],
            [InlineKeyboardButton(text="✂️ Подовжена стрижка — 450 грн (1 год)", callback_data="cl:book:svc:long")],
            [InlineKeyboardButton(text="🧔 Борода — 150 грн (20–30 хв)", callback_data="cl:book:svc:beard")],
            client_nav_row(back_to="dates", include_menu=True, include_cancel=True),
        ]
    )


def booking_times_keyboard(date_str: str, time_slots: list[str]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []

    for t in time_slots:
        cb = f"cl:book:time:{date_str}:{t}"
        row.append(InlineKeyboardButton(text=t, callback_data=cb))

        if len(row) == 4:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append(client_nav_row(back_to="svc", include_menu=True, include_cancel=True))
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def client_confirm_keyboard() -> InlineKeyboardMarkup:
    """Підтвердження заявки без booking_id (створиться після confirm у БД)."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Підтвердити", callback_data="cl:book:confirm"),
                InlineKeyboardButton(text="❌ Скасувати", callback_data="cl:book:cancel"),
            ],
            client_nav_row(back_to="times", include_menu=True, include_cancel=False),
        ]
    )


def my_booking_actions_keyboard(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"❌ Скасувати запис ID {booking_id}", callback_data=f"cl:my:cancel:{booking_id}")],
            client_nav_row(back_to="menu", include_menu=True, include_cancel=False),
        ]
    )


# ==================================================
#  LEGACY (ReplyKeyboard) — не використовуй, якщо хочеш "0 повідомлень від кнопок"
# ==================================================

def client_main_menu_reply() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="💈 Записатися")],
        [KeyboardButton(text="📋 Мої записи")],
        [KeyboardButton(text="ℹ️ Контакти")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder="Оберіть дію")


# =====================
#  ADMIN
# =====================

def admin_calendar_keyboard(year: int | None = None, month: int | None = None) -> InlineKeyboardMarkup:
    if year is None or month is None:
        today = date.today()
        year = today.year
        month = today.month

    kb: list[list[InlineKeyboardButton]] = []

    kb.append([InlineKeyboardButton(text=f"{UA_MONTHS[month]} {year}", callback_data="admin_cal:noop")])
    kb.append([InlineKeyboardButton(text=wd, callback_data="admin_cal:noop") for wd in UA_WEEKDAYS_SHORT])

    cal = calendar.Calendar(firstweekday=0)
    for week in cal.monthdayscalendar(year, month):
        row: list[InlineKeyboardButton] = []
        for day_num in week:
            if day_num == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="admin_cal:noop"))
            else:
                d = date(year, month, day_num)
                row.append(InlineKeyboardButton(text=str(day_num), callback_data=f"admin_cal:day:{d.isoformat()}"))
        kb.append(row)

    prev_month, prev_year = month - 1, year
    next_month, next_year = month + 1, year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    if next_month == 13:
        next_month = 1
        next_year += 1

    kb.append(
        [
            InlineKeyboardButton(text="« Попередній", callback_data=f"admin_cal:month:{prev_year}-{prev_month:02d}"),
            InlineKeyboardButton(text="Наступний »", callback_data=f"admin_cal:month:{next_year}-{next_month:02d}"),
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=kb)


def admin_booking_decision_keyboard(booking_id: int) -> InlineKeyboardMarkup:
    # Сумісно з admin_handlers.py (approve:/reject:)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Підтвердити", callback_data=f"approve:{booking_id}"),
                InlineKeyboardButton(text="❌ Відхилити", callback_data=f"reject:{booking_id}"),
            ]
        ]
    )


def admin_reports_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📆 За сьогодні", callback_data="report:today")],
            [InlineKeyboardButton(text="📅 За цей тиждень", callback_data="report:this_week")],
            [InlineKeyboardButton(text="🗓 За цей місяць", callback_data="report:this_month")],
        ]
    )
