import os
import logging
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update
import httpx

logging.basicConfig(level=logging.INFO)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "savol_secret")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
app = FastAPI()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("👋 Привет! Я SavolBot. Задай вопрос — получи ответ!")

@dp.message(F.text)
async def echo(message: Message):
    await message.answer(f"Вы спросили: {message.text}")

@app.post("/webhook")
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return {"ok": False}
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    async with httpx.AsyncClient() as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
            json={"url": WEBHOOK_URL, "secret_token": WEBHOOK_SECRET}
        )

@app.get("/health")
async def health():
    return {"status": "ok"}
