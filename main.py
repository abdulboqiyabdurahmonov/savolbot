import os
import re
import logging
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update, InlineKeyboardMarkup, InlineKeyboardButton

# ================== ЛОГИ ==================
logging.basicConfig(level=logging.INFO)

# ================== ENV ==================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # https://<your-app>.onrender.com/webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "savol_secret")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")  # например 123456789

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
app = FastAPI()

# ================== МОДЕРАЦИЯ ==================
ILLEGAL_PATTERNS = [
    r"\b(взлом|хак|кейлоггер|фишинг|ботнет|ддос|ddos)\b",
    r"\b(как\s+получить\s+доступ|обойти|взять\s+пароль)\b.*\b(аккаунт|телеграм|инстаграм|банк)\b",
    r"\b(поддел(ать|ка)|фальсифицир|липов(ый|ые))\b.*\b(паспорт|справк|диплом|договор)\b",
    r"\b(наркотик|амфетамин|марихуан|каннабис)\b.*\b(купить|вырастить)\b",
    r"\b(оружие|пистолет|автомат|взрывчатк|бомбу)\b.*\b(сделать|купить)\b",
    r"\b(взятк|откат|обналич|уход\s+от\s+налогов)\b",
    r"\b(пробить\s+по\s+базе|слить\s+базу)\b",
    r"\b(отравить|взорвать|убить)\b",
    r"\b(soxta|qalbakilashtir)\b.*\b(diplom|pasport|spravka)\b",
    r"\b(soliqdan\s+qochish|pora|otkat)\b",
    r"\b(hack|xak|parolni\s+olish)\b",
]
DENY_TEXT_RU = "⛔ Запрос отклонён. Я отвечаю только в рамках законодательства РУз."
DENY_TEXT_UZ = "⛔ So‘rov rad etildi. Men faqat O‘zbekiston qonunchiligi doirasida javob beraman."

def is_uzbek(text: str) -> bool:
    t = text.lower()
    return bool(re.search(r"[ғқҳў]", t) or re.search(r"\b(ha|yo'q|iltimos|rahmat)\b", t))

def violates_policy(text: str) -> bool:
    t = text.lower()
    return any(re.search(rx, t) for rx in ILLEGAL_PATTERNS)

# ================== ТАРИФЫ ==================
FREE_LIMIT = 2
TARIFFS = {
    "start": {
        "title": "Старт",
        "price_uzs": 49_000,
        "desc": ["До 100 сообщений/мес", "Краткие и понятные ответы", "Без генерации файлов и картинок"],
        "active": True,
        "duration_days": 30,
    },
    "business": {"title": "Бизнес", "price_uzs": 119_000, "desc": [], "active": False, "duration_days": 30},
    "pro": {"title": "PRO", "price_uzs": 249_000, "desc": [], "active": False, "duration_days": 30},
}

USERS = {}  # tg_id -> {"free_used": int, "plan": str, "paid_until": datetime|None, "lang": "ru"|"uz"}

def get_user(tg_id: int):
    if tg_id not in USERS:
        USERS[tg_id] = {"free_used": 0, "plan": "free", "paid_until": None, "lang": "ru"}
    return USERS[tg_id]

def has_active_sub(user: dict) -> bool:
    return user["plan"] in ("start", "business", "pro") and user["paid_until"] and user["paid_until"] > datetime.utcnow()

def pay_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оформить «Старт»", callback_data="subscribe_start")],
        [InlineKeyboardButton(text="ℹ️ Тарифы", callback_data="show_tariffs")]
    ])

def tariffs_text(lang='ru'):
    def bullet(lines): return "\n".join(f"• {x}" for x in lines)
    txt = []
    for key in ("start", "business", "pro"):
        t = TARIFFS[key]
        badge = "(доступен)" if t["active"] else "(скоро)"
        if lang == 'uz':
            badge = "(faol)" if t["active"] else "(tez orada)"
            txt.append(f"⭐ {t['title']} {badge}\nNarx: {t['price_uzs']:,} so‘m/oy\n{bullet(t['desc'])}")
        else:
            txt.append(f"⭐ {t['title']} {badge}\nЦена: {t['price_uzs']:,} сум/мес\n{bullet(t['desc'])}")
    return "\n\n".join(txt)

# ================== ИИ ==================
SYSTEM_PROMPT = (
    "Ты — SavolBot, дружелюбный консультант. Отвечай кратко и ясно (до 6–8 предложений). "
    "Соблюдай законы Узбекистана. Не давай инструкции по незаконным действиям. "
    "Если вопрос на узбекском — отвечай на узбекском; если на русском — на русском."
)

async def ask_gpt(user_text: str) -> str:
    if not OPENAI_API_KEY:
        return f"Вы спросили: {user_text}"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {"model": OPENAI_MODEL, "temperature": 0.6,
               "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": user_text}]}
    async with httpx.AsyncClient(timeout=30.0, base_url=OPENAI_API_BASE) as client:
        r = await client.post("/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()

# ================== КОМАНДЫ ==================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    u = get_user(message.from_user.id)
    u["lang"] = "uz" if is_uzbek(message.text or "") else "ru"
    await message.answer("👋 Привет! / Assalomu alaykum!\n"
                         "Я — SavolBot. Первые 2 ответа — бесплатно, далее по подписке.\n"
                         "Напишите вопрос, и я отвечу кратко и по делу.")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer("ℹ️ Как пользоваться ботом:\n"
                         "1. Отправьте свой вопрос текстом.\n"
                         "2. Первые 2 ответа — бесплатно.\n"
                         "3. Для продолжения оформите подписку через /tariffs.\n"
                         "4. Соблюдайте законодательство РУз.")

@dp.message(Command("about"))
async def cmd_about(message: Message):
    await message.answer("🤖 SavolBot от TripleA — это умный помощник для быстрых ответов.\n"
                         "Работает на базе GPT. Всегда в рамках закона.")

@dp.message(Command("tariffs"))
async def cmd_tariffs(message: Message):
    u = get_user(message.from_user.id)
    await message.answer(tariffs_text(u["lang"]), reply_markup=pay_kb())

@dp.message(Command("myplan"))
async def cmd_myplan(message: Message):
    u = get_user(message.from_user.id)
    status = "активна" if has_active_sub(u) else "нет"
    until = u["paid_until"].isoformat() if u["paid_until"] else "—"
    await message.answer(f"Ваш план: {u['plan']}\nПодписка: {status}\nОплачено до: {until}\n"
                         f"Бесплатных использовано: {u['free_used']}/{FREE_LIMIT}")

# ================== КНОПКИ ==================
@dp.callback_query(F.data == "show_tariffs")
async def cb_show_tariffs(call):
    u = get_user(call.from_user.id)
    await call.message.edit_text(tariffs_text(u["lang"]), reply_markup=pay_kb())
    await call.answer()

@dp.callback_query(F.data == "subscribe_start")
async def cb_subscribe_start(call):
    pay_link = "https://pay.example.com/savolbot/start"  # заглушка
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Готово (я оплатил)", callback_data="paid_start_done")]
    ])
    await call.message.answer(f"💳 «Старт» — 49 000 сум/мес.\nОплата: {pay_link}", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "paid_start_done")
async def cb_paid_done(call):
    if ADMIN_CHAT_ID:
        await bot.send_message(int(ADMIN_CHAT_ID),
                               f"👤 @{call.from_user.username or call.from_user.id} запросил активацию «Старт».\n"
                               f"TG ID: {call.from_user.id}\n"
                               f"Используйте /grant_start {call.from_user.id}")
    await call.message.answer("Спасибо! Мы проверим оплату и активируем подписку.")
    await call.answer()

@dp.message(Command("grant_start"))
async def cmd_grant_start(message: Message):
    if str(message.from_user.id) != str(ADMIN_CHAT_ID):
        return await message.answer("Команда недоступна.")
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Использование: /grant_start <tg_id>")
    target_id = int(parts[1])
    u = get_user(target_id)
    u["plan"] = "start"
    u["paid_until"] = datetime.utcnow() + timedelta(days=TARIFFS["start"]["duration_days"])
    await message.answer(f"✅ Активирован «Старт» для {target_id}")
    await bot.send_message(target_id, "✅ Подписка «Старт» активирована.")

# ================== ОБРАБОТКА ВОПРОСОВ ==================
@dp.message(F.text)
async def handle_text(message: Message):
    text = message.text.strip()
    u = get_user(message.from_user.id)
    if is_uzbek(text):
        u["lang"] = "uz"
    if violates_policy(text):
        return await message.answer(DENY_TEXT_UZ if u["lang"] == "uz" else DENY_TEXT_RU)
    if not has_active_sub(u) and u["free_used"] >= FREE_LIMIT:
        return await message.answer("💳 Доступ к ответам ограничен. Оформите подписку:", reply_markup=pay_kb())
    try:
        reply = await ask_gpt(text)
        await message.answer(reply)
    except Exception as e:
        logging.exception("OpenAI error: %s", e)
        return await message.answer("Извини, сервер перегружен. Попробуйте позже.")
    if not has_active_sub(u):
        u["free_used"] += 1

# ================== WEBHOOK ==================
@app.post("/webhook")
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return {"ok": False, "error": "bad secret"}
    update = Update.model_validate(await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    if TELEGRAM_TOKEN and WEBHOOK_URL:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
                                     json={"url": WEBHOOK_URL, "secret_token": WEBHOOK_SECRET})
            logging.info("setWebhook: %s %s", resp.status_code, resp.text)

@app.get("/health")
async def health():
    return {"status": "ok"}
