from aiogram.fsm.state import State, StatesGroup


class BookingState(StatesGroup):
    choosing_date = State()
    choosing_service = State()
    choosing_time = State()
    waiting_phone = State()
    waiting_full_name = State()
    confirming = State()
