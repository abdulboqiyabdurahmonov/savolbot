import os
import logging
import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update

logging.basicConfig(level=logging.INFO)

# === ENV ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # e.g. https://savolbot.onrender.com/webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "savol_secret")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # можно менять в Env

# Проверка обязательных переменных (без паники — просто лог)
if not TELEGRAM_TOKEN:
    logging.warning("TELEGRAM_TOKEN is not set")
if not WEBHOOK_URL:
    logging.warning("WEBHOOK_URL is not set")
if not OPENAI_API_KEY:
    logging.warning("OPENAI_API_KEY is not set (бот будет отвечать эхо-текстом)")

# === APP/AIROGRAM ===
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
app = FastAPI()

# Короткий системный промпт: аккуратные, понятные ответы
SYSTEM_PROMPT = (
    "Ты — SavolBot, дружелюбный консультант. Отвечай кратко, по делу и понятным языком. "
    "Избегай опасных медицинских и юридических советов, предлагай обратиться к специалисту при необходимости. "
    "Если вопрос на узбекском — отвечай на узбекском; если на русском — на русском."
)

async def ask_gpt(user_text: str) -> str:
    """Запрос к OpenAI Chat Completions через httpx."""
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0.6,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text}
        ]
    }
    async with httpx.AsyncClient(timeout=30.0, base_url=OPENAI_API_BASE) as client:
        r = await client.post("/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"].strip()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    text = (
        "👋 Привет! Я SavolBot.\n"
        "Задай любой вопрос — постараюсь ответить понятно и по делу."
    )
    await message.answer(text)

@dp.message(F.text)
async def handle_text(message: Message):
    user_text = message.text.strip()
    # Если нет ключа — простой эхо, чтобы бот не молчал
    if not OPENAI_API_KEY:
        await message.answer(f"Вы спросили: {user_text}")
        return
    try:
        reply = await ask_gpt(user_text)
        await message.answer(reply)
    except Exception as e:
        logging.exception("OpenAI error: %s", e)
        await message.answer("Извини, сейчас перегружен. Попробуй ещё раз через минуту.")

# === Webhook endpoints ===
@app.post("/webhook")
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return {"ok": False, "error": "bad secret"}
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    # Ставим вебхук при старте приложения
    if TELEGRAM_TOKEN and WEBHOOK_URL:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
                json={"url": WEBHOOK_URL, "secret_token": WEBHOOK_SECRET}
            )
            logging.info("setWebhook status: %s %s", resp.status_code, resp.text)
    else:
        logging.warning("Webhook not set: no TELEGRAM_TOKEN or WEBHOOK_URL")

@app.get("/health")
async def health():
    return {"status": "ok"}
