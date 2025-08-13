import os
import re
import logging
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update, InlineKeyboardMarkup, InlineKeyboardButton

logging.basicConfig(level=logging.INFO)

# === ENV ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # https://savolbot.onrender.com/webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "savol_secret")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Необязательно, но удобно для ручной активации после оплаты
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")  # например 123456789 (строкой ок)

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
app = FastAPI()

# ====== Модерация: в рамках законодательства РУз ======
ILLEGAL_PATTERNS = [
    r"\b(взлом|взламыв|хак|хакер|кейлоггер|кряк|фишинг|ботнет|ддос|ddos)\b",
    r"\b(как\s+получить\s+доступ|обойти|взять\s+пароль)\b.*\b(аккаунт|телеграм|инстаграм|почт|банк)\b",
    r"\b(поддел(ать|ка)|фальсифицир|липов(ый|ые))\b.*\b(паспорт|справк|диплом|договор|печать|штамп)\b",
    r"\b(наркотик|спайс|амфетамин|опиум|марихуан|каннабис)\b.*\b(купить|сделать|вырастить)\b",
    r"\b(оружие|пистолет|автомат|нож\b|взрывчатк|тротил|бомбу)\b.*\b(сделать|купить|достать)\b",
    r"\b(взятк|откат|обнал|обналич|уход\s+от\s+налогов|серая\s+зарплата)\b",
    r"\b(пробить\s+по\s+базе|слить\s+базу|базу\s+клиентов|как\s+найти\s+по\s+паспорт)\b",
    r"\b(отравить|взорвать|убить|нанести\s+вред)\b",
    r"\b(soxta|soxtalashtir|qalbakilashtir)\b.*\b(diplom|pasport|spravka|shartnoma)\b",
    r"\b(soliqdan\s+qochish|pora|otkat)\b",
    r"\b(hack|xak|parolni\s+olish|akkauntga\s+k(i|e)rish)\b",
]

DENY_TEXT_RU = ("⛔ Запрос отклонён. Консультирую только в рамках законодательства РУз "
                "и не даю инструкций по незаконным действиям. Сформулируйте другой вопрос.")
DENY_TEXT_UZ = ("⛔ So‘rov rad etildi. Men faqat O‘zbekiston qonunchiligi doirasida maslahat beraman "
                "va noqonuniy ko‘rsatmalar bermayman. Iltimos, boshqa savol bering.")

def is_uzbek(text: str) -> bool:
    t = text.lower()
    return bool(
        re.search(r"[ғқҳў]", t) or
        re.search(r"\b(ha|yo'q|iltimos|rahmat|salom)\b", t)
    )

def violates_policy(text: str) -> bool:
    t = text.lower()
    return any(re.search(rx, t) for rx in ILLEGAL_PATTERNS)

# ====== Тарифы и лимиты ======
FREE_LIMIT = 2  # бесплатных ответа до пэйволла

TARIFFS = {
    "start": {
        "title": "Старт",
        "price_uzs": 49_000,
        "desc": [
            "До 100 сообщений/мес",
            "Краткие и понятные ответы",
            "Без генерации файлов и картинок",
        ],
        "active": True,
        "duration_days": 30,
    },
    "business": {
        "title": "Бизнес",
        "price_uzs": 119_000,
        "desc": [
            "До 500 сообщений/мес",
            "Инструкции и чек-листы",
            "Простые документы (docx/pdf)",
        ],
        "active": False,  # скоро
        "duration_days": 30,
    },
    "pro": {
        "title": "PRO",
        "price_uzs": 249_000,
        "desc": [
            "Высокие лимиты",
            "Картинки и сложные документы",
            "Приоритетная очередь",
        ],
        "active": False,  # скоро
        "duration_days": 30,
    },
}

# Память процесса: для MVP хватит (после рестарта/деплоя всё сбросится — ок для старта)
USERS = {}  # tg_id -> {"free_used": int, "plan": str, "paid_until": datetime|None, "lang": "ru"|"uz"}

def get_user(tg_id: int):
    u = USERS.get(tg_id)
    if not u:
        u = {"free_used": 0, "plan": "free", "paid_until": None, "lang": "ru"}
        USERS[tg_id] = u
    return u

def has_active_sub(user: dict) -> bool:
    if user["plan"] in ("start", "business", "pro") and user["paid_until"]:
        return user["paid_until"] > datetime.utcnow()
    return False

def paywall_text(lang='ru'):
    if lang == 'uz':
        return ("💳 SavolBot Premium: obuna orqali cheklanmagan javoblarga yaqin tajriba.\n"
                f"• Start — {TARIFFS['start']['price_uzs']:,} so‘m/oy (faol)\n"
                "• Business — tez orada\n"
                "• PRO — tez orada\n\n"
                "Birinchi 2 javob — bepul. Davom etish uchun obuna rasmiylashtiring.")
    return ("💳 SavolBot Premium: больше возможностей с подпиской.\n"
            f"• Старт — {TARIFFS['start']['price_uzs']:,} сум/мес (доступен)\n"
            "• Бизнес — скоро\n"
            "• PRO — скоро\n\n"
            "Первые 2 ответа — бесплатно. Для продолжения оформите подписку.")

def pay_kb():
    # Сейчас только Старт доступен
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оформить «Старт»", callback_data="subscribe_start")],
        [InlineKeyboardButton(text="ℹ️ Тарифы", callback_data="show_tariffs")]
    ])

def tariffs_text(lang='ru'):
    def bullet(lines): return "\n".join(f"• {x}" for x in lines)
    if lang == 'uz':
        txt = []
        for key in ("start","business","pro"):
            t = TARIFFS[key]
            badge = "(faol)" if t["active"] else "(tez orada)"
            txt.append(f"⭐ {t['title']} {badge}\nNarx: {t['price_uzs']:,} so‘m/oy\n{bullet(t['desc'])}")
        return "\n\n".join(txt)
    else:
        txt = []
        for key in ("start","business","pro"):
            t = TARIFFS[key]
            badge = "(доступен)" if t["active"] else "(скоро)"
            txt.append(f"⭐ {t['title']} {badge}\nЦена: {t['price_uzs']:,} сум/мес\n{bullet(t['desc'])}")
        return "\n\n".join(txt)

# ====== Ответ ИИ ======
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

# ====== Команды ======
@dp.message(Command("start"))
async def cmd_start(message: Message):
    u = get_user(message.from_user.id)
    u["lang"] = "uz" if is_uzbek(message.text or "") else "ru"
    text = (
        "👋 Привет! / Assalomu alaykum!\n"
        "Я — SavolBot от TripleA. Отвечаю на ваши вопросы в рамках законодательства РУз.\n"
        "🎁 Первые 2 ответа — бесплатно. Далее — подписка.\n\n"
        "Напишите вопрос — отвечу кратко и по делу."
    )
    await message.answer(text)

@dp.message(Command("tariffs"))
async def cmd_tariffs(message: Message):
    u = get_user(message.from_user.id)
    await message.answer(tariffs_text(u["lang"]), reply_markup=pay_kb())

@dp.message(Command("myplan"))
async def cmd_myplan(message: Message):
    u = get_user(message.from_user.id)
    status = "активна" if has_active_sub(u) else "нет"
    until = u["paid_until"].isoformat() if u["paid_until"] else "—"
    await message.answer(f"Ваш план: {u['plan']}\nПодписка: {status}\nОплачено до: {until}\nБесплатных использовано: {u['free_used']}/{FREE_LIMIT}")

# ====== Inline callbacks (оплата/тарифы) ======
@dp.callback_query(F.data == "show_tariffs")
async def cb_show_tariffs(call):
    u = get_user(call.from_user.id)
    await call.message.edit_text(tariffs_text(u["lang"]), reply_markup=pay_kb())
    await call.answer()

@dp.callback_query(F.data == "subscribe_start")
async def cb_subscribe_start(call):
    # MVP: даём ссылку на оплату и инструкцию.
    # Позже подключим Click/Payme вебхуки и автопродление.
    pay_link = "https://pay.example.com/savolbot/start"  # заглушка
    msg = (
        "💳 «Старт» — 49 000 сум/мес.\n\n"
        f"Оплатите по ссылке: {pay_link}\n"
        "После оплаты нажмите кнопку «Готово», и мы активируем подписку.\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Готово (я оплатил)", callback_data="paid_start_done")]
    ])
    await call.message.answer(msg, reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "paid_start_done")
async def cb_paid_done(call):
    u = get_user(call.from_user.id)
    # Уведомим админа, чтобы он проверил оплату и активировал план.
    if ADMIN_CHAT_ID:
        try:
            await bot.send_message(
                int(ADMIN_CHAT_ID),
                f"👤 Пользователь @{call.from_user.username or call.from_user.id} запросил активацию «Старт».\n"
                f"TG ID: {call.from_user.id}\n"
                f"Нажмите /grant_start {call.from_user.id} после проверки оплаты."
            )
        except Exception as e:
            logging.warning("Failed to notify admin: %s", e)
    await call.message.answer("Спасибо! Мы проверим оплату и активируем «Старт» в течение короткого времени.")
    await call.answer()

# Команда для админа: ручная активация «Старт» (MVP)
@dp.message(Command("grant_start"))
async def cmd_grant_start(message: Message):
    if not ADMIN_CHAT_ID or str(message.from_user.id) != str(ADMIN_CHAT_ID):
        await message.answer("Команда недоступна.")
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /grant_start <tg_id>")
        return
    target_id = int(parts[1])
    u = get_user(target_id)
    u["plan"] = "start"
    u["paid_until"] = datetime.utcnow() + timedelta(days=TARIFFS["start"]["duration_days"])
    await message.answer(f"✅ Активирован «Старт» для {target_id} до {u['paid_until'].isoformat()}")
    try:
        await bot.send_message(target_id, "✅ Подписка «Старт» активирована. Приятного использования!")
    except Exception as e:
        logging.warning("Notify user failed: %s", e)

# ====== Обработка вопросов ======
@dp.message(F.text)
async def handle_text(message: Message):
    text = message.text.strip()
    u = get_user(message.from_user.id)

    # Обновим язык по сообщению
    if is_uzbek(text):
        u["lang"] = "uz"

    # 1) Модерация
    if violates_policy(text):
        await message.answer(DENY_TEXT_UZ if u["lang"] == "uz" else DENY_TEXT_RU)
        return

    # 2) Проверка подписки / бесплатных
    if not has_active_sub(u):
        if u["free_used"] >= FREE_LIMIT:
            await message.answer(paywall_text(u["lang"]), reply_markup=pay_kb())
            return

    # 3) Запрос к ИИ
    try:
        reply = await ask_gpt(text)
        await message.answer(reply)
    except Exception as e:
        logging.exception("OpenAI error: %s", e)
        await message.answer("Извини, сейчас перегружен. Попробуй ещё раз через минуту.")
        return

    # 4) Списываем бесплатную попытку, если нет подписки
    if not has_active_sub(u):
        u["free_used"] += 1

# ====== Webhook ======
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
