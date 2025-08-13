import os
import re
import logging
import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update

logging.basicConfig(level=logging.INFO)

# === ENV ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # https://savolbot.onrender.com/webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "savol_secret")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # можно поменять в Env

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
app = FastAPI()

# ====== Модерация: «в рамках законодательства РУз» ======
# Простые правила: баны на взлом, подделки, наркотики, оружие, коррупцию,
# обход гос.систем, утечки персональных данных, насилие и т.п.
ILLEGAL_PATTERNS = [
    # взлом / киберпреступления
    r"\b(взлом|взламыв|хак|хакер|кейлоггер|кряк|фишинг|ботнет|ддос|ddos)\b",
    r"\b(как\s+получить\s+доступ|обойти|взять\s+пароль)\b.*\b(аккаунт|телеграм|инстаграм|почт|банк)\b",
    # подделка документов
    r"\b(поддел(ать|ка)|фальсифицир|липов(ый|ые))\b.*\b(паспорт|справк|диплом|договор|печать|штамп)\b",
    # наркотики / взрывчатка / оружие незаконно
    r"\b(наркотик|спайс|амфетамин|опиум|марихуан|каннабис)\b.*\b(купить|сделать|вырастить)\b",
    r"\b(оружие|пистолет|автомат|нож\b|взрывчатк|тротил|бомбу)\b.*\b(сделать|купить|достать)\b",
    # коррупция / уход от налогов / обнал
    r"\b(взятк|откат|обнал|обналич|уход\s+от\s+налогов|серая\s+зарплата)\b",
    # персональные данные «пробить по базе»
    r"\b(пробить\s+по\s+базе|слить\s+базу|базу\s+клиентов|как\s+найти\s+по\s+паспорт)\b",
    # насилие/вред
    r"\b(отравить|взорвать|убить|нанести\s+вред)\b",
    # узбекские варианты (минимальный набор для MVP)
    r"\b(soxta|soxtalashtir|qalbakilashtir)\b.*\b(diplom|pasport|spravka|shartnoma)\b",
    r"\b(sol(iq)dan\s+qochish|pora|otkat)\b",
    r"\b(hack|xak|parolni\s+olish|akkauntga\s+k(i|e)rish)\b",
]

DENY_TEXT_RU = (
    "⛔ Запрос отклонён. Я консультирую только в рамках законодательства Республики Узбекистан "
    "и не даю инструкций по незаконным действиям. Сформулируйте другой вопрос."
)
DENY_TEXT_UZ = (
    "⛔ So‘rov rad etildi. Men faqat O‘zbekiston qonunchiligi doirasida maslahat beraman "
    "va noqonuniy harakatlar bo‘yicha ko‘rsatmalar bermayman. Iltimos, boshqa savol bering."
)

def is_uzbek(text: str) -> bool:
    t = text.lower()
    return bool(
        re.search(r"[ғқҳў]", t) or
        re.search(r"\b(ha|yo'q|iltimos|rahmat|salom)\b", t)
    )

def violates_policy(text: str) -> bool:
    t = text.lower()
    for rx in ILLEGAL_PATTERNS:
        if re.search(rx, t):
            return True
    return False
# ====== конец блока модерации ======

# Короткий системный промпт
SYSTEM_PROMPT = (
    "Ты — SavolBot, дружелюбный консультант. Отвечай кратко и ясно (до 6–8 предложений). "
    "Всегда соблюдай законы Узбекистана. Не давай инструкции по незаконным действиям, подделкам, взломам, "
    "обходу гос.систем и т.п. По медицине — только общие справки и рекомендация обратиться к врачу. "
    "Если вопрос на узбекском — отвечай на узбекском; если на русском — на русском. "
    "В конце давай 1–2 чётких следующих шага, если это уместно."
)

async def ask_gpt(user_text: str) -> str:
    if not OPENAI_API_KEY:
        return f"Вы спросили: {user_text}"
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
        "👋 Привет! / Assalomu alaykum!\n"
        "Я — SavolBot. Отвечаю на ваши вопросы в рамках законодательства РУз: быт, финансы, госуслуги, бизнес, учёба и т.д.\n"
        "Пишите вопрос — отвечу кратко и по делу."
    )
    await message.answer(text)

@dp.message(F.text)
async def handle_text(message: Message):
    user_text = message.text.strip()

    # 1) Модерация до обращения к ИИ
    if violates_policy(user_text):
        await message.answer(DENY_TEXT_UZ if is_uzbek(user_text) else DENY_TEXT_RU)
        return

    # 2) Нормальный ответ
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
