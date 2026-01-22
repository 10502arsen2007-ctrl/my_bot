import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Update

from config import settings
from database import init_db
from handlers.client_handlers import client_router
from handlers.admin_handlers import admin_router


async def on_unhandled_update(update: Update) -> None:
    """
    Корисно під час дебагу, коли бачиш:
    'Update is not handled'
    Тут можна швидко подивитись тип/дані апдейту в логах.
    """
    try:
        logging.getLogger("unhandled").warning("Unhandled update: %s", update.model_dump())
    except Exception:
        logging.getLogger("unhandled").warning("Unhandled update (failed to dump)")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    # Важливо: спочатку БД, потім старт бота
    await init_db()

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()

    # Підключення роутерів
    dp.include_router(client_router)
    dp.include_router(admin_router)

    # Ловимо невловлені апдейти (для дебага)
    dp.errors.register(lambda e: logging.getLogger("dp.error").exception("Dispatcher error", exc_info=e.exception))
    dp.update.register(on_unhandled_update)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
