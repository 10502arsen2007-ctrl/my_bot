import os
import asyncio
from aiohttp import web

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message


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


async def start_bot():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    bot = Bot(token=token)
    dp = Dispatcher()

    @dp.message(Command("start"))
    async def cmd_start(message: Message):
        await message.answer("Бот працює на Render (Free).")

    await dp.start_polling(bot)


async def main():
    await asyncio.gather(
        start_web_server(),
        start_bot(),
    )
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


if __name__ == "__main__":
    asyncio.run(main())
