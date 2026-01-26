from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Tuple

# Optional local .env support (Render uses Dashboard env vars)
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None


def _load_dotenv_if_present() -> None:
    if load_dotenv is not None:
        load_dotenv()


def _get_env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    return (val or "").strip()


def _parse_admin_ids(raw: str) -> Tuple[int, ...]:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("ADMIN_IDS is not set. Example: ADMIN_IDS=123,456")

    ids: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit():
            raise RuntimeError(f"ADMIN_IDS contains non-numeric value: {part}")
        ids.append(int(part))
    if not ids:
        raise RuntimeError("ADMIN_IDS is empty after parsing")
    return tuple(ids)


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_ids: Tuple[int, ...]

    # UI / branding
    shop_name: str = "CYRULNYA"
    master_name: str = "Майстер"

    # schedule defaults (fallback if DB schedule not set)
    work_start_hour: int = 10
    work_end_hour: int = 19
    slot_minutes: int = 45

    @classmethod
    def from_env(cls) -> "Settings":
        _load_dotenv_if_present()

        token = _get_env("BOT_TOKEN")
        if not token:
            raise RuntimeError("BOT_TOKEN is not set")

        admin_ids = _parse_admin_ids(_get_env("ADMIN_IDS"))

        shop_name = _get_env("SHOP_NAME", "CYRULNYA")
        master_name = _get_env("MASTER_NAME", "Майстер")

        # these are optional; safe defaults if missing
        wsh = int(_get_env("WORK_START_HOUR", "10") or "10")
        weh = int(_get_env("WORK_END_HOUR", "19") or "19")
        sm = int(_get_env("SLOT_MINUTES", "45") or "45")

        return cls(
            bot_token=token,
            admin_ids=admin_ids,
            shop_name=shop_name,
            master_name=master_name,
            work_start_hour=wsh,
            work_end_hour=weh,
            slot_minutes=sm,
        )


settings = Settings.from_env()
