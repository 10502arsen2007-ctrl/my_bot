import os
import asyncio
from aiohttp import web

from aiogram import Bot, Dispatcher
from handlers.admin_handlers import admin_router
from handlers.client_handlers import client_router


async def start_web_server():
    app = web.Application()

    async def health(request):
        return web.Response(text="ok")

    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    print(f"WEB: server started on port {port}")


async def start_bot():
    print("BOT: starting...")

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    print("BOT_TOKEN present:", True)

    bot = Bot(token=token)
    dp = Dispatcher()

    dp.include_router(client_router)
    dp.include_router(admin_router)

    print("BOT: routers included")
    print("BOT: polling started")

    await dp.start_polling(bot)


async def main():
    await asyncio.gather(
        start_web_server(),
        start_bot(),
    )


if __name__ == "__main__":
    asyncio.run(main())
