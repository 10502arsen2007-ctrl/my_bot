import os
import asyncio
import logging
from aiohttp import web

from aiogram import Bot, Dispatcher

from config import settings
from handlers.admin_handlers import admin_router
from handlers.client_handlers import client_router


log = logging.getLogger(__name__)


async def start_web_server() -> web.AppRunner:
    """HTTP сервер потрібен Render Web Service (Free), щоб був відкритий PORT."""
    app = web.Application()

    async def root(_request: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def healthz(_request: web.Request) -> web.Response:
        return web.Response(text="healthy")

    app.router.add_get("/", root)
    app.router.add_get("/healthz", healthz)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    log.info("WEB server started on port %s", port)
    return runner


async def start_bot() -> None:
    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()

    dp.include_router(client_router)
    dp.include_router(admin_router)

    log.info("BOT polling started")
    await dp.start_polling(bot)


async def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    runner = await start_web_server()

    bot_task = asyncio.create_task(start_bot(), name="bot_polling")

    try:
        await bot_task
    except asyncio.CancelledError:
        # нормальний шлях при зупинці
        pass
    finally:
        log.info("Shutting down WEB server...")
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
