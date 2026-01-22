# utils/ui.py
from __future__ import annotations

from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest


async def show_screen_message(
    message: Message,
    state,
    text: str,
    reply_markup=None,
    parse_mode: str | None = "HTML",
):
    """
    Показати/оновити 'екран' через Message (команди, кнопки ReplyKeyboard).
    Зберігаємо last_screen_message_id у FSM state.
    """
    data = await state.get_data()
    last_id = data.get("last_screen_message_id")

    # пробуємо відредагувати попереднє повідомлення
    if last_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=last_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            # не надсилаємо нових повідомлень
            return last_id
        except TelegramBadRequest:
            # не вдалося відредагувати -> створимо нове
            pass

    sent = await message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
    await state.update_data(last_screen_message_id=sent.message_id)
    return sent.message_id


async def show_screen_callback(
    callback: CallbackQuery,
    state,
    text: str,
    reply_markup=None,
    parse_mode: str | None = "HTML",
):
    """
    Показати/оновити 'екран' через CallbackQuery (inline-кнопки).
    Редагуємо саме повідомлення, по якому натиснули.
    """
    try:
        await callback.message.edit_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        await state.update_data(last_screen_message_id=callback.message.message_id)
    except TelegramBadRequest:
        # fallback: надіслати нове
        sent = await callback.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
        await state.update_data(last_screen_message_id=sent.message_id)

    await callback.answer()
