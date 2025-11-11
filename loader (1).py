# loader.py
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode

import os

BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
