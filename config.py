from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    bot_token: str
    admin_ids: tuple[int, ...]

    # ДОДАЙ ОЦЕ:
    shop_name: str = "CYRULNYA"
    master_name: str = "Майстер"

    # базові налаштування графіку (поки без UI редагування)
    work_start_hour: int = 10
    work_end_hour: int = 19
    slot_minutes: int = 45

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.getenv("BOT_TOKEN", "")
        if not token:
            raise RuntimeError("BOT_TOKEN не задано у .env файлі або змінних середовища")

        raw_admins = os.getenv("ADMIN_IDS", "")
        if not raw_admins:
            raise RuntimeError("ADMIN_IDS не задано у .env")

        admin_ids = tuple(int(x.strip()) for x in raw_admins.split(",") if x.strip())

        # ДОДАЙ ОЦЕ:
        shop_name = os.getenv("SHOP_NAME", "CYRULNYA").strip()
        master_name = os.getenv("MASTER_NAME", "Майстер").strip()

        return cls(
            bot_token=token,
            admin_ids=admin_ids,
            # ДОДАЙ ОЦЕ:
            shop_name=shop_name,
            master_name=master_name,
        )


settings = Settings.from_env()
