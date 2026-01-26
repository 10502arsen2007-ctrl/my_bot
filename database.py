from __future__ import annotations

import aiosqlite
from datetime import date, datetime
from typing import List, Dict, Optional, Tuple, Any

DB_PATH = "barber.db"


# =======================
#  TIME HELPERS
# =======================

def _parse_hhmm(t: str) -> Tuple[int, int]:
    h, m = t.split(":")
    return int(h), int(m)


def _to_minutes(t: str) -> int:
    h, m = _parse_hhmm(t)
    return h * 60 + m


def _intervals_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    # [a_start, a_end) overlaps [b_start, b_end)
    return a_start < b_end and b_start < a_end


def _iso_utc_now() -> str:
    return datetime.utcnow().isoformat()


# =======================
#  MIGRATION HELPERS
# =======================

async def _ensure_column(db: aiosqlite.Connection, table: str, column: str, ddl_fragment: str) -> None:
    """
    ddl_fragment example: "ui_message_id INTEGER"
    """
    cur = await db.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in await cur.fetchall()]
    if column not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {ddl_fragment}")


# =======================
#  INIT / MIGRATIONS
# =======================

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")

        # users
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,          -- tg_id
                full_name TEXT,
                phone TEXT,
                is_admin INTEGER DEFAULT 0,
                ui_message_id INTEGER            -- для концепції "одного повідомлення"
            )
            """
        )
        # migrations for users (older DBs may not have ui_message_id)
        await _ensure_column(db, "users", "ui_message_id", "ui_message_id INTEGER")

        # bookings
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                date TEXT NOT NULL,                 -- 'YYYY-MM-DD'
                time TEXT NOT NULL,                 -- 'HH:MM'
                duration_minutes INTEGER NOT NULL,  -- тривалість послуги
                occupy_minutes INTEGER,             -- фактична зайнятість (duration + пауза/округлення), може бути NULL
                service_code TEXT,                  -- 'short','beard', ...
                service_text TEXT NOT NULL,         -- назва послуги
                price_text TEXT NOT NULL,           -- '350 грн', '350–400 грн'
                client_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                status TEXT NOT NULL,               -- pending/approved/completed/rejected/cancelled_*
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (client_id) REFERENCES users(id)
            )
            """
        )

        # migrations for bookings (older DBs may not have occupy_minutes)
        await _ensure_column(db, "bookings", "occupy_minutes", "occupy_minutes INTEGER")

        # indices
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bookings_date ON bookings(date)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bookings_client ON bookings(client_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bookings_status ON bookings(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bookings_created ON bookings(created_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_bookings_date_status_time ON bookings(date, status, time)")

        # days_off (вихідні по датах)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS days_off (
                date TEXT PRIMARY KEY  -- 'YYYY-MM-DD'
            )
            """
        )

        # shop_settings (1 row table id=1)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS shop_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),

                base_grid_minutes INTEGER NOT NULL DEFAULT 60,
                short_service_threshold_minutes INTEGER NOT NULL DEFAULT 40,
                rest_minutes_after_short INTEGER NOT NULL DEFAULT 5,
                extra_round_minutes INTEGER NOT NULL DEFAULT 15,

                min_lead_minutes INTEGER NOT NULL DEFAULT 0,
                default_work_start TEXT NOT NULL DEFAULT '09:00',
                default_work_end   TEXT NOT NULL DEFAULT '19:00'
            )
            """
        )
        await db.execute("INSERT OR IGNORE INTO shop_settings (id) VALUES (1)")

        # migrations for shop_settings (older DBs may not have new columns)
        await _ensure_column(db, "shop_settings", "base_grid_minutes", "base_grid_minutes INTEGER NOT NULL DEFAULT 60")
        await _ensure_column(
            db,
            "shop_settings",
            "short_service_threshold_minutes",
            "short_service_threshold_minutes INTEGER NOT NULL DEFAULT 40",
        )
        await _ensure_column(
            db,
            "shop_settings",
            "rest_minutes_after_short",
            "rest_minutes_after_short INTEGER NOT NULL DEFAULT 5",
        )
        await _ensure_column(
            db,
            "shop_settings",
            "extra_round_minutes",
            "extra_round_minutes INTEGER NOT NULL DEFAULT 15",
        )
        await _ensure_column(
            db,
            "shop_settings",
            "min_lead_minutes",
            "min_lead_minutes INTEGER NOT NULL DEFAULT 0",
        )
        await _ensure_column(
            db,
            "shop_settings",
            "default_work_start",
            "default_work_start TEXT NOT NULL DEFAULT '09:00'",
        )
        await _ensure_column(
            db,
            "shop_settings",
            "default_work_end",
            "default_work_end TEXT NOT NULL DEFAULT '19:00'",
        )

        # schedule by weekday
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_schedule (
                weekday INTEGER PRIMARY KEY,         -- 0=Mon ... 6=Sun
                is_working INTEGER NOT NULL DEFAULT 1,
                work_start TEXT NOT NULL DEFAULT '09:00',
                work_end   TEXT NOT NULL DEFAULT '19:00'
            )
            """
        )

        # seed weekly schedule if empty
        cur = await db.execute("SELECT COUNT(*) FROM weekly_schedule")
        cnt = int((await cur.fetchone())[0] or 0)
        if cnt == 0:
            # default: Mon-Sat working, Sun off
            for wd in range(7):
                is_working = 0 if wd == 6 else 1
                await db.execute(
                    """
                    INSERT INTO weekly_schedule (weekday, is_working, work_start, work_end)
                    VALUES (?, ?, '09:00', '19:00')
                    """,
                    (wd, is_working),
                )

        # breaks (перерви)
        # weekday NULL = для всіх днів
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_breaks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                weekday INTEGER NULL,
                start_time TEXT NOT NULL,   -- 'HH:MM'
                end_time   TEXT NOT NULL,   -- 'HH:MM'
                is_enabled INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_breaks_weekday ON schedule_breaks(weekday)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_breaks_enabled ON schedule_breaks(is_enabled)")

        # reminders (нагадування)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_id INTEGER,
                target TEXT NOT NULL,        -- 'client' | 'master'
                tg_id INTEGER NOT NULL,
                remind_at TEXT NOT NULL,     -- ISO datetime (UTC recommended)
                type TEXT NOT NULL,          -- '2h' | '30m' | ...
                status TEXT NOT NULL DEFAULT 'pending',  -- 'pending' | 'sent' | 'canceled' | 'failed'
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (booking_id) REFERENCES bookings(id)
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_reminders_status_time ON reminders(status, remind_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_reminders_booking ON reminders(booking_id)")

        await db.commit()


# =======================
#  USERS
# =======================

async def upsert_user(tg_id: int, full_name: str | None = None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (id, full_name)
            VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET
                full_name = COALESCE(?, full_name)
            """,
            (tg_id, full_name, full_name),
        )
        await db.commit()


async def update_user_phone(tg_id: int, phone: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE users
            SET phone = ?
            WHERE id = ?
            """,
            (phone, tg_id),
        )
        await db.commit()


async def set_user_ui_message_id(tg_id: int, message_id: int | None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE users
            SET ui_message_id = ?
            WHERE id = ?
            """,
            (message_id, tg_id),
        )
        await db.commit()


async def get_user(tg_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, full_name, phone, is_admin, ui_message_id FROM users WHERE id = ?",
            (tg_id,),
        )
        row = await cursor.fetchone()

    if not row:
        return None

    return {
        "id": int(row[0]),
        "full_name": row[1],
        "phone": row[2],
        "is_admin": bool(row[3]),
        "ui_message_id": (int(row[4]) if row[4] is not None else None),
    }


# =======================
#  SHOP SETTINGS
# =======================

async def get_shop_settings() -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT
                base_grid_minutes,
                short_service_threshold_minutes,
                rest_minutes_after_short,
                extra_round_minutes,
                min_lead_minutes,
                default_work_start,
                default_work_end
            FROM shop_settings
            WHERE id = 1
            """
        )
        row = await cur.fetchone()

    return {
        "base_grid_minutes": int(row[0]),
        "short_service_threshold_minutes": int(row[1]),
        "rest_minutes_after_short": int(row[2]),
        "extra_round_minutes": int(row[3]),
        "min_lead_minutes": int(row[4]),
        "default_work_start": row[5],
        "default_work_end": row[6],
    }


async def set_base_grid_minutes(minutes: int) -> None:
    minutes = int(minutes)
    if minutes not in (30, 60, 90, 120):
        raise ValueError("base_grid_minutes must be one of: 30, 60, 90, 120")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE shop_settings SET base_grid_minutes = ? WHERE id = 1", (minutes,))
        await db.commit()


async def set_short_service_threshold_minutes(minutes: int) -> None:
    minutes = int(minutes)
    if minutes < 5 or minutes > 120:
        raise ValueError("short_service_threshold_minutes must be between 5 and 120")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE shop_settings SET short_service_threshold_minutes = ? WHERE id = 1", (minutes,))
        await db.commit()


async def set_rest_minutes_after_short(minutes: int) -> None:
    minutes = int(minutes)
    if minutes < 0 or minutes > 60:
        raise ValueError("rest_minutes_after_short must be between 0 and 60")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE shop_settings SET rest_minutes_after_short = ? WHERE id = 1", (minutes,))
        await db.commit()


async def set_extra_round_minutes(minutes: int) -> None:
    minutes = int(minutes)
    if minutes not in (5, 10, 15, 20, 30):
        raise ValueError("extra_round_minutes must be one of: 5, 10, 15, 20, 30")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE shop_settings SET extra_round_minutes = ? WHERE id = 1", (minutes,))
        await db.commit()


# Backward-compat: if somewhere in code you still call set_slot_step_minutes,
# we map it to extra_round_minutes (closest concept to "step" for extra slot rounding).
async def set_slot_step_minutes(minutes: int) -> None:
    await set_extra_round_minutes(minutes)


async def set_min_lead_minutes(minutes: int) -> None:
    minutes = int(minutes)
    if minutes < 0 or minutes > 24 * 60:
        raise ValueError("min_lead_minutes must be between 0 and 1440")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE shop_settings SET min_lead_minutes = ? WHERE id = 1", (minutes,))
        await db.commit()


async def set_shop_setting(key: str, value: int) -> None:
    """
    Універсальний setter для адмінки (admin_handlers.py).
    """
    key = str(key).strip()
    value = int(value)

    mapping = {
        "base_grid_minutes": set_base_grid_minutes,
        "short_service_threshold_minutes": set_short_service_threshold_minutes,
        "rest_minutes_after_short": set_rest_minutes_after_short,
        "extra_round_minutes": set_extra_round_minutes,
        "min_lead_minutes": set_min_lead_minutes,
        # legacy
        "slot_step_minutes": set_slot_step_minutes,
    }
    fn = mapping.get(key)
    if not fn:
        raise ValueError(f"Unknown shop setting key: {key}")
    await fn(value)


async def get_weekly_schedule() -> Dict[int, Dict[str, Any]]:
    """
    {weekday: {"is_working": bool, "work_start": "HH:MM", "work_end": "HH:MM"}}
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT weekday, is_working, work_start, work_end
            FROM weekly_schedule
            ORDER BY weekday
            """
        )
        rows = await cur.fetchall()

    out: Dict[int, Dict[str, Any]] = {}
    for wd, is_working, ws, we in rows:
        out[int(wd)] = {"is_working": bool(is_working), "work_start": ws, "work_end": we}
    return out


async def get_day_schedule(weekday: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT weekday, is_working, work_start, work_end
            FROM weekly_schedule
            WHERE weekday = ?
            """,
            (int(weekday),),
        )
        row = await cur.fetchone()

    if not row:
        return None

    return {"weekday": int(row[0]), "is_working": bool(row[1]), "work_start": row[2], "work_end": row[3]}


async def set_day_schedule(
    weekday: int,
    *,
    is_working: Optional[bool] = None,
    work_start: Optional[str] = None,
    work_end: Optional[str] = None,
) -> None:
    weekday = int(weekday)
    fields: List[str] = []
    values: List[Any] = []

    if is_working is not None:
        fields.append("is_working = ?")
        values.append(1 if is_working else 0)
    if work_start is not None:
        fields.append("work_start = ?")
        values.append(work_start)
    if work_end is not None:
        fields.append("work_end = ?")
        values.append(work_end)

    if not fields:
        return

    values.append(weekday)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"""
            UPDATE weekly_schedule
            SET {", ".join(fields)}
            WHERE weekday = ?
            """,
            tuple(values),
        )
        await db.commit()


async def get_breaks_for_weekday(weekday: int) -> List[Dict[str, Any]]:
    """
    Повертає перерви:
      - weekday == конкретний
      - weekday IS NULL (для всіх днів)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT id, weekday, start_time, end_time, is_enabled
            FROM schedule_breaks
            WHERE is_enabled = 1 AND (weekday = ? OR weekday IS NULL)
            ORDER BY COALESCE(weekday, -1), start_time
            """,
            (int(weekday),),
        )
        rows = await cur.fetchall()

    return [
        {
            "id": int(r[0]),
            "weekday": (int(r[1]) if r[1] is not None else None),
            "start_time": r[2],
            "end_time": r[3],
            "is_enabled": bool(r[4]),
        }
        for r in rows
    ]


async def add_break(weekday: Optional[int], start_time: str, end_time: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO schedule_breaks (weekday, start_time, end_time, is_enabled)
            VALUES (?, ?, ?, 1)
            """,
            (int(weekday) if weekday is not None else None, start_time, end_time),
        )
        await db.commit()
        return int(cur.lastrowid)


async def remove_break(break_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM schedule_breaks WHERE id = ?", (int(break_id),))
        await db.commit()


async def set_break_enabled(break_id: int, is_enabled: bool) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE schedule_breaks SET is_enabled = ? WHERE id = ?",
            (1 if is_enabled else 0, int(break_id)),
        )
        await db.commit()


# =======================
#  DAYS OFF
# =======================

async def add_day_off(date_str: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO days_off(date) VALUES (?)", (date_str,))
        await db.commit()


async def remove_day_off(date_str: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM days_off WHERE date = ?", (date_str,))
        await db.commit()


async def is_day_off(date_str: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM days_off WHERE date = ? LIMIT 1", (date_str,))
        row = await cur.fetchone()
        return row is not None


async def get_work_context_for_date(date_str: str) -> Dict[str, Any]:
    """
    Уніфікований контекст робочого дня для генерації слотів:
    - враховує days_off
    - weekly_schedule
    - breaks
    """
    dt = datetime.fromisoformat(date_str).date()
    wd = dt.weekday()

    if await is_day_off(date_str):
        return {"is_working": False, "work_start": None, "work_end": None, "breaks": []}

    day = await get_day_schedule(wd)
    if not day or not day["is_working"]:
        return {"is_working": False, "work_start": None, "work_end": None, "breaks": []}

    breaks = await get_breaks_for_weekday(wd)
    return {"is_working": True, "work_start": day["work_start"], "work_end": day["work_end"], "breaks": breaks}


# =======================
#  BOOKINGS
# =======================

async def create_booking(
    client_id: int,
    date_str: str,
    time_str: str,
    duration_minutes: int,
    service_code: str,
    service_text: str,
    price_text: str,
    client_name: str,
    phone: str,
    status: str = "pending",
    occupy_minutes: Optional[int] = None,
) -> int:
    """
    НЕ атомарно. Залишено для сумісності.
    Для “без гонок” використовуй create_booking_atomic().
    """
    now = _iso_utc_now()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO bookings
            (client_id, date, time, duration_minutes, occupy_minutes, service_code, service_text, price_text,
             client_name, phone, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                date_str,
                time_str,
                int(duration_minutes),
                (int(occupy_minutes) if occupy_minutes is not None else None),
                service_code,
                service_text,
                price_text,
                client_name,
                phone,
                status,
                now,
                now,
            ),
        )
        await db.commit()
        return int(cursor.lastrowid)


async def create_booking_atomic(
    client_id: int,
    date_str: str,
    time_str: str,
    duration_minutes: int,
    service_code: str,
    service_text: str,
    price_text: str,
    client_name: str,
    phone: str,
    *,
    status: str = "pending",
    occupy_minutes: Optional[int] = None,
) -> int:
    """
    Атомарне створення броні в SQLite:
    - BEGIN IMMEDIATE
    - перевірка перетинів по активних бронях (pending/approved), з урахуванням occupy_minutes існуючих записів
    - вставка

    occupy_minutes:
      якщо передано, для конфлікту беремо саме його (duration + rest, наприклад).
      якщо None — беремо duration_minutes.
    """
    now = _iso_utc_now()
    new_start = _to_minutes(time_str)
    occ_new = int(occupy_minutes) if occupy_minutes is not None else int(duration_minutes)
    new_end = new_start + occ_new

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        await db.execute("BEGIN IMMEDIATE")

        cur = await db.execute(
            """
            SELECT time, duration_minutes, occupy_minutes
            FROM bookings
            WHERE date = ?
              AND status IN ('pending','approved')
            """,
            (date_str,),
        )
        rows = await cur.fetchall()

        for t, dur, occ_db in rows:
            s = _to_minutes(t)
            occ_existing = int(occ_db) if occ_db is not None else int(dur)
            e = s + occ_existing
            if _intervals_overlap(new_start, new_end, s, e):
                await db.execute("ROLLBACK")
                raise ValueError("TIME_SLOT_ALREADY_TAKEN")

        cursor = await db.execute(
            """
            INSERT INTO bookings
            (client_id, date, time, duration_minutes, occupy_minutes, service_code, service_text, price_text,
             client_name, phone, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                date_str,
                time_str,
                int(duration_minutes),
                (int(occupy_minutes) if occupy_minutes is not None else None),
                service_code,
                service_text,
                price_text,
                client_name,
                phone,
                status,
                now,
                now,
            ),
        )
        await db.commit()
        return int(cursor.lastrowid)


async def get_active_bookings_for_date(target_date: date) -> List[Dict[str, Any]]:
    """
    Активні записи (pending/approved) на дату.
    Використовується для генерації слотів / перевірки перетинів.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id, time, duration_minutes, occupy_minutes, status
            FROM bookings
            WHERE date = ?
              AND status IN ('pending', 'approved')
            """,
            (target_date.isoformat(),),
        )
        rows = await cursor.fetchall()

    return [
        {
            "id": int(r[0]),
            "time": r[1],
            "duration_minutes": int(r[2]),
            "occupy_minutes": (int(r[3]) if r[3] is not None else None),
            "status": r[4],
        }
        for r in rows
    ]


async def count_client_active_requests_for_day(client_id: int, day_str: str) -> int:
    """
    М'який ліміт: рахуємо активні заявки (pending/approved) за день створення.
    day_str: 'YYYY-MM-DD'
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM bookings
            WHERE client_id = ?
              AND substr(created_at, 1, 10) = ?
              AND status IN ('pending', 'approved')
            """,
            (client_id, day_str),
        )
        row = await cursor.fetchone()

    return int(row[0] or 0)


async def get_bookings_for_date(target_date: date) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id, date, time, duration_minutes, occupy_minutes, client_name, phone, service_text, price_text, status
            FROM bookings
            WHERE date = ?
            ORDER BY time
            """,
            (target_date.isoformat(),),
        )
        rows = await cursor.fetchall()

    return [
        {
            "id": int(r[0]),
            "date": r[1],
            "time": r[2],
            "duration_minutes": int(r[3]),
            "occupy_minutes": (int(r[4]) if r[4] is not None else None),
            "client_name": r[5],
            "phone": r[6],
            "service_text": r[7],
            "price_text": r[8],
            "status": r[9],
        }
        for r in rows
    ]


async def get_booking_by_id(booking_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id, client_id, date, time, duration_minutes, occupy_minutes, service_code, service_text, price_text,
                   client_name, phone, status
            FROM bookings
            WHERE id = ?
            """,
            (int(booking_id),),
        )
        row = await cursor.fetchone()

    if not row:
        return None

    return {
        "id": int(row[0]),
        "client_id": int(row[1]),
        "date": row[2],
        "time": row[3],
        "duration_minutes": int(row[4]),
        "occupy_minutes": (int(row[5]) if row[5] is not None else None),
        "service_code": row[6],
        "service_text": row[7],
        "price_text": row[8],
        "client_name": row[9],
        "phone": row[10],
        "status": row[11],
    }


async def update_booking_status(booking_id: int, status: str) -> None:
    now = _iso_utc_now()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE bookings
            SET status = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, now, int(booking_id)),
        )
        await db.commit()


async def get_pending_bookings() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id, date, time, duration_minutes, occupy_minutes, client_name, phone, service_text, price_text
            FROM bookings
            WHERE status = 'pending'
            ORDER BY date, time
            """
        )
        rows = await cursor.fetchall()

    return [
        {
            "id": int(r[0]),
            "date": r[1],
            "time": r[2],
            "duration_minutes": int(r[3]),
            "occupy_minutes": (int(r[4]) if r[4] is not None else None),
            "client_name": r[5],
            "phone": r[6],
            "service_text": r[7],
            "price_text": r[8],
        }
        for r in rows
    ]


async def get_client_bookings(client_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id, date, time, duration_minutes, occupy_minutes, service_text, price_text, status
            FROM bookings
            WHERE client_id = ?
            ORDER BY date DESC, time DESC
            LIMIT ?
            """,
            (int(client_id), int(limit)),
        )
        rows = await cursor.fetchall()

    return [
        {
            "id": int(r[0]),
            "date": r[1],
            "time": r[2],
            "duration_minutes": int(r[3]),
            "occupy_minutes": (int(r[4]) if r[4] is not None else None),
            "service_text": r[5],
            "price_text": r[6],
            "status": r[7],
        }
        for r in rows
    ]


async def cancel_booking_by_client(booking_id: int, client_id: int) -> bool:
    """
    Клієнт може скасувати тільки активний запис: pending або approved.
    """
    now = _iso_utc_now()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            UPDATE bookings
            SET status = 'cancelled_by_client', updated_at = ?
            WHERE id = ?
              AND client_id = ?
              AND status IN ('pending','approved')
            """,
            (now, int(booking_id), int(client_id)),
        )
        await db.commit()
        return cursor.rowcount > 0


# =======================
#  REMINDERS (DB LAYER)
# =======================

async def add_reminder(
    *,
    booking_id: int | None,
    target: str,
    tg_id: int,
    remind_at_iso: str,
    reminder_type: str,
) -> int:
    now = _iso_utc_now()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO reminders (booking_id, target, tg_id, remind_at, type, status, attempts, last_error, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', 0, NULL, ?)
            """,
            (int(booking_id) if booking_id is not None else None, target, int(tg_id), remind_at_iso, reminder_type, now),
        )
        await db.commit()
        return int(cur.lastrowid)


async def cancel_pending_reminders_for_booking(booking_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            UPDATE reminders
            SET status = 'canceled'
            WHERE booking_id = ?
              AND status = 'pending'
            """,
            (int(booking_id),),
        )
        await db.commit()
        return int(cur.rowcount or 0)


async def get_due_reminders(now_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Забирає reminders, які треба відправити (pending + remind_at <= now).
    now_iso: ISO UTC timestamp
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT id, booking_id, target, tg_id, remind_at, type, attempts
            FROM reminders
            WHERE status = 'pending'
              AND remind_at <= ?
            ORDER BY remind_at ASC
            LIMIT ?
            """,
            (now_iso, int(limit)),
        )
        rows = await cur.fetchall()

    return [
        {
            "id": int(r[0]),
            "booking_id": (int(r[1]) if r[1] is not None else None),
            "target": r[2],
            "tg_id": int(r[3]),
            "remind_at": r[4],
            "type": r[5],
            "attempts": int(r[6] or 0),
        }
        for r in rows
    ]


async def mark_reminder_sent(reminder_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE reminders SET status = 'sent' WHERE id = ?", (int(reminder_id),))
        await db.commit()


async def mark_reminder_failed(reminder_id: int, error_text: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE reminders
            SET status = 'failed',
                attempts = attempts + 1,
                last_error = ?
            WHERE id = ?
            """,
            (error_text[:500], int(reminder_id)),
        )
        await db.commit()


# =======================
#  REPORTS
# =======================

async def get_report_for_period(start_date: date, end_date: date) -> Dict[str, Any]:
    """
    Повертає:
      - total_bookings
      - finished_bookings (approved+completed)
      - busiest_day (date|None)
      - busiest_day_count
    """
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM bookings
            WHERE date BETWEEN ? AND ?
            """,
            (start_str, end_str),
        )
        total_bookings = int((await cursor.fetchone())[0] or 0)

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM bookings
            WHERE date BETWEEN ? AND ?
              AND status IN ('approved','completed')
            """,
            (start_str, end_str),
        )
        finished_bookings = int((await cursor.fetchone())[0] or 0)

        cursor = await db.execute(
            """
            SELECT date, COUNT(*) as cnt
            FROM bookings
            WHERE date BETWEEN ? AND ?
              AND status IN ('approved','completed')
            GROUP BY date
            ORDER BY cnt DESC
            LIMIT 1
            """,
            (start_str, end_str),
        )
        row = await cursor.fetchone()

    busiest_day = None
    busiest_day_count = 0
    if row:
        busiest_day = row[0]
        busiest_day_count = int(row[1] or 0)

    return {
        "total_bookings": total_bookings,
        "finished_bookings": finished_bookings,
        "busiest_day": busiest_day,
        "busiest_day_count": busiest_day_count,
    }


# =======================
#  ADMIN HELPERS
# =======================

async def get_bookings_for_date_admin(date_str: str) -> List[Tuple[int, str, str, str, str]]:
    """
    (booking_id, time_str, service_text, client_name, status)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT id, time, service_text, client_name, status
            FROM bookings
            WHERE date = ?
            ORDER BY time
            """,
            (date_str,),
        )
        rows = await cur.fetchall()
    return [(int(r[0]), r[1], r[2], r[3], r[4]) for r in rows]


async def get_bookings_for_period_admin(start_date: str, end_date: str) -> List[Tuple[str, str, str, str, str]]:
    """
    (date_str, time_str, service_text, client_name, status)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT date, time, service_text, client_name, status
            FROM bookings
            WHERE date BETWEEN ? AND ?
            ORDER BY date, time
            """,
            (start_date, end_date),
        )
        rows = await cur.fetchall()
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]


async def get_pending_bookings_admin() -> List[Tuple[int, str, str, str, str]]:
    """
    (bid, date_str, time_str, service_text, client_name)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT id, date, time, service_text, client_name
            FROM bookings
            WHERE status = 'pending'
            ORDER BY date, time
            """
        )
        rows = await cur.fetchall()
    return [(int(r[0]), r[1], r[2], r[3], r[4]) for r in rows]


async def get_booking_with_client_admin(booking_id: int):
    """
    (
      booking_id, date_str, time_str, status,
      service_text, price_text, duration_minutes,
      client_tg_id, client_full_name
    )
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT
                b.id, b.date, b.time, b.status,
                b.service_text, b.price_text, b.duration_minutes,
                u.id as tg_id, u.full_name
            FROM bookings b
            LEFT JOIN users u ON u.id = b.client_id
            WHERE b.id = ?
            """,
            (int(booking_id),),
        )
        row = await cur.fetchone()

    if not row:
        return None

    client_tg_id = int(row[7]) if row[7] is not None else 0
    client_name = row[8] if row[8] is not None else ""

    return (
        int(row[0]),
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        int(row[6]),
        client_tg_id,
        client_name,
    )


async def get_booked_times_for_date_admin(date_str: str) -> List[str]:
    """
    Старти часу, які вже зайняті активними записами (pending/approved) + completed.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT time
            FROM bookings
            WHERE date = ?
              AND status IN ('pending','approved','completed')
            """,
            (date_str,),
        )
        rows = await cur.fetchall()
    return [r[0] for r in rows]


async def get_report_overview_admin(start_date: str, end_date: str) -> Tuple[int, int]:
    """
    total_bookings, unique_clients
    рахуємо записи зі статусом approved/completed
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT client_id) as uniq
            FROM bookings
            WHERE date BETWEEN ? AND ?
              AND status IN ('approved','completed')
            """,
            (start_date, end_date),
        )
        row = await cur.fetchone()
    return int(row[0] or 0), int(row[1] or 0)


async def get_report_by_period_admin(start_date: str, end_date: str) -> List[Tuple[str, int]]:
    """
    (service_text, cnt)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT service_text, COUNT(*) as cnt
            FROM bookings
            WHERE date BETWEEN ? AND ?
              AND status IN ('approved','completed')
            GROUP BY service_text
            ORDER BY cnt DESC
            """,
            (start_date, end_date),
        )
        rows = await cur.fetchall()
    return [(r[0], int(r[1] or 0)) for r in rows]


async def get_all_client_tg_ids() -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id FROM users")
        rows = await cur.fetchall()
    return [int(r[0]) for r in rows]


async def get_clients_with_stats_admin(limit: int = 50):
    """
    (tg_id, full_name, phone, total_all, total_approved)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT
                u.id,
                COALESCE(u.full_name,'') as full_name,
                u.phone,
                COALESCE(COUNT(b.id),0) as total_all,
                COALESCE(SUM(CASE WHEN b.status IN ('approved','completed') THEN 1 ELSE 0 END),0) as total_ok
            FROM users u
            LEFT JOIN bookings b ON b.client_id = u.id
            GROUP BY u.id
            ORDER BY total_ok DESC, total_all DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = await cur.fetchall()

    return [(int(r[0]), r[1], r[2], int(r[3] or 0), int(r[4] or 0)) for r in rows]


async def get_client_stats_admin(tg_id: int | None = None, username: str | None = None):
    """
    Під username у твоїй схемі поля немає.
    Тому: працюємо по tg_id, або повертаємо None якщо username передали.
    Формат:
    (full_name, phone, total_all, total_approved, first_date, last_date)
    """
    if username is not None:
        return None
    if tg_id is None:
        return None

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT
                COALESCE(u.full_name,'') as full_name,
                u.phone,
                COALESCE(COUNT(b.id),0) as total_all,
                COALESCE(SUM(CASE WHEN b.status IN ('approved','completed') THEN 1 ELSE 0 END),0) as total_ok,
                MIN(b.date) as first_date,
                MAX(b.date) as last_date
            FROM users u
            LEFT JOIN bookings b ON b.client_id = u.id
            WHERE u.id = ?
            GROUP BY u.id
            LIMIT 1
            """,
            (int(tg_id),),
        )
        r = await cur.fetchone()

    if not r:
        return None

    return (r[0], r[1], int(r[2] or 0), int(r[3] or 0), r[4], r[5])
