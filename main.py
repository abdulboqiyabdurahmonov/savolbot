import os
import re
import logging
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

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
    r"\b(как\s+получить\s+доступ|обойти|взять\s+пароль)\b.*\b(аккаунт|телеграм|инстаграм|банк|почт)\b",
    r"\b(поддел(ать|ка)|фальсифицир|липов(ый|ые))\b.*\b(паспорт|справк|диплом|договор|печать|штамп)\b",
    r"\b(наркотик|амфетамин|марихуан|каннабис|опиум|спайс)\b.*\b(купить|вырастить|сделать)\b",
    r"\b(оружие|пистолет|автомат|взрывчатк|бомбу|тротил)\b.*\b(сделать|купить|достать)\b",
    r"\b(взятк|откат|обналич|обнал|уход\s+от\s+налогов|серая\s+зарплата)\b",
    r"\b(пробить\s+по\s+базе|слить\s+базу|базу\s+клиентов|найти\s+по\s+паспорт)\b",
    r"\b(отравить|взорвать|убить|нанести\s+вред)\b",
    r"\b(soxta|qalbakilashtir|soxtalashtir)\b.*\b(diplom|pasport|spravka|shartnoma)\b",
    r"\b(soliqdan\s+qochish|pora|otkat)\b",
    r"\b(hack|xak|parolni\s+olish|akkauntga\s+k(i|e)rish)\b",
]
DENY_TEXT_RU = "⛔ Запрос отклонён. Я отвечаю только в рамках законодательства РУз."
DENY_TEXT_UZ = "⛔ So‘rov rad etildi. Men faqat O‘zbekiston qonunchiligi doirasida javob beraman."

def is_uzbek(text: str) -> bool:
    t = text.lower()
    return bool(re.search(r"[ғқҳў]", t) or re.search(r"\b(ha|yo'q|iltimos|rahmat|salom)\b", t))

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
    "business": {"title": "Бизнес", "price_uzs": 119_000, "desc": [
        "До 500 сообщений/мес", "Инструкции и чек-листы", "Простые документы (docx/pdf)"
    ], "active": False, "duration_days": 30},
    "pro": {"title": "PRO", "price_uzs": 249_000, "desc": [
        "Высокие лимиты", "Картинки и сложные документы", "Приоритетная очередь"
    ], "active": False, "duration_days": 30},
}

# ================== ТЕМЫ (категории) ==================
TOPICS = {
    "daily": {"title_ru": "Быт", "title_uz": "Maishiy", "hint": "Даёшь простые практичные советы, чек-листы и шаги."},
    "finance": {"title_ru": "Финансы", "title_uz": "Moliya", "hint": "Объясняй спокойно, с цифрами и примерами. Без персональных финсоветов высокого риска."},
    "gov": {"title_ru": "Госуслуги", "title_uz": "Davlat xizmatlari", "hint": "Ссылайся на общие процедуры, документы и шаги подачи."},
    "biz": {"title_ru": "Бизнес", "title_uz": "Biznes", "hint": "Давай краткие инструкции по регистрации/отчётности/документам."},
    "edu": {"title_ru": "Учёба", "title_uz": "Ta’lim", "hint": "Объясняй возможности и шаги поступления/обучения."},
    "it": {"title_ru": "IT", "title_uz": "IT", "hint": "Будь конкретным, избегай незаконного софта/взлома."},
    "health": {"title_ru": "Здоровье (общ.)", "title_uz": "Sog‘liq (umumiy)", "hint": "Только общая информация. Рекомендуй обратиться к врачу при симптомах."},
}

def topic_kb(lang="ru", current=None):
    rows = []
    for key, t in TOPICS.items():
        label = t["title_uz"] if lang == "uz" else t["title_ru"]
        if current == key:
            label = f"✅ {label}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"topic:{key}")])
    rows.append([InlineKeyboardButton(text="↩️ Закрыть / Yopish", callback_data="topic:close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ================== ПАМЯТЬ (MVP, без БД) ==================
USERS = {}  # tg_id -> {"free_used": int, "plan": str, "paid_until": datetime|None, "lang": "ru"|"uz", "topic": str|None}

def get_user(tg_id: int):
    if tg_id not in USERS:
        USERS[tg_id] = {"free_used": 0, "plan": "free", "paid_until": None, "lang": "ru", "topic": None}
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
BASE_SYSTEM_PROMPT = (
    "Ты — SavolBot, дружелюбный консультант. Отвечай кратко и ясно (до 6–8 предложений). "
    "Соблюдай законы Узбекистана. Не давай инструкции по незаконным действиям, подделкам, взломам, обходу систем. "
    "По медицине — только общая справка и совет обратиться к врачу. "
    "Язык ответа = язык вопроса (RU/UZ)."
)

async def ask_gpt(user_text: str, topic_hint: str | None) -> str:
    if not OPENAI_API_KEY:
        return f"Вы спросили: {user_text}"
    system = BASE_SYSTEM_PROMPT
    if topic_hint:
        system += f" Учитывай контекст темы: {topic_hint}"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {"model": OPENAI_MODEL, "temperature": 0.6,
               "messages": [{"role": "system", "content": system},
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
    await message.answer(
        "👋 Привет! / Assalomu alaykum!\n"
        "Я — SavolBot от TripleA. Первые 2 ответа — бесплатно, дальше по подписке «Старт».\n"
        "Напишите вопрос или выберите тему через /topics."
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ Как пользоваться ботом:\n"
        "1) Просто задайте вопрос текстом.\n"
        "2) Выберите тему через /topics — так ответы будут ещё точнее.\n"
        "3) Первые 2 ответа — бесплатно; затем оформите подписку через /tariffs.\n"
        "4) Всё строго в рамках законодательства РУз."
    )

@dp.message(Command("about"))
async def cmd_about(message: Message):
    await message.answer(
        "🤖 SavolBot — проект TripleA. Цель — быстрые и понятные ответы 24/7.\n"
        "Работает на базе GPT. Темы и двуязычность RU/UZ.\n"
        "Поддержать проект и снять лимиты можно подпиской: /tariffs."
    )

@dp.message(Command("tariffs"))
async def cmd_tariffs(message: Message):
    u = get_user(message.from_user.id)
    await message.answer(tariffs_text(u["lang"]), reply_markup=pay_kb())

@dp.message(Command("myplan"))
async def cmd_myplan(message: Message):
    u = get_user(message.from_user.id)
    status = "активна" if has_active_sub(u) else "нет"
    until = u["paid_until"].isoformat() if u["paid_until"] else "—"
    topic = u["topic"] or "—"
    await message.answer(
        f"Ваш план: {u['plan']}\nПодписка: {status}\nОплачено до: {until}\n"
        f"Выбранная тема: {topic}\n"
        f"Бесплатных использовано: {u['free_used']}/{FREE_LIMIT}"
    )

@dp.message(Command("topics"))
async def cmd_topics(message: Message):
    u = get_user(message.from_user.id)
    lang = u["lang"]
    head = "🗂 Выберите тему (не влияет на цену, только на стиль ответа):" if lang == "ru" \
        else "🗂 Mavzuni tanlang (narxga ta’sir qilmaydi, faqat javob ohangiga):"
    await message.answer(head, reply_markup=topic_kb(lang, current=u.get("topic")))

# ================== CALLBACKS ==================
@dp.callback_query(F.data == "show_tariffs")
async def cb_show_tariffs(call: CallbackQuery):
    u = get_user(call.from_user.id)
    await call.message.edit_text(tariffs_text(u["lang"]), reply_markup=pay_kb())
    await call.answer()

@dp.callback_query(F.data == "subscribe_start")
async def cb_subscribe_start(call: CallbackQuery):
    pay_link = "https://pay.example.com/savolbot/start"  # заглушка
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Готово (я оплатил)", callback_data="paid_start_done")]
    ])
    await call.message.answer(f"💳 «Старт» — {TARIFFS['start']['price_uzs']:,} сум/мес.\nОплата: {pay_link}", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "paid_start_done")
async def cb_paid_done(call: CallbackQuery):
    if ADMIN_CHAT_ID:
        await bot.send_message(
            int(ADMIN_CHAT_ID),
            f"👤 @{call.from_user.username or call.from_user.id} запросил активацию «Старт».\n"
            f"TG ID: {call.from_user.id}\n"
            f"Используйте /grant_start {call.from_user.id}"
        )
    await call.message.answer("Спасибо! Мы проверим оплату и активируем подписку.")
    await call.answer()

@dp.callback_query(F.data.startswith("topic:"))
async def cb_topic(call: CallbackQuery):
    u = get_user(call.from_user.id)
    _, key = call.data.split(":", 1)
    if key == "close":
        try:
            await call.message.delete()
        except Exception:
            pass
        return await call.answer("OK")
    if key in TOPICS:
        u["topic"] = key
        lang = u["lang"]
        title = TOPICS[key]["title_uz"] if lang == "uz" else TOPICS[key]["title_ru"]
        await call.message.edit_reply_markup(reply_markup=topic_kb(lang, current=key))
        await call.answer(f"Выбрана тема: {title}" if lang == "ru" else f"Mavzu tanlandi: {title}")

# админская ручная активация (MVP)
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
    await message.answer(f"✅ Активирован «Старт» для {target_id} до {u['paid_until'].isoformat()}")
    try:
        await bot.send_message(target_id, "✅ Подписка «Старт» активирована. Приятного использования!")
    except Exception as e:
        logging.warning("Notify user failed: %s", e)

# ================== ОБРАБОТЧИК ВОПРОСОВ ==================
@dp.message(F.text)
async def handle_text(message: Message):
    text = message.text.strip()
    u = get_user(message.from_user.id)

    # Язык
    if is_uzbek(text):
        u["lang"] = "uz"

    # Модерация
    if violates_policy(text):
        return await message.answer(DENY_TEXT_UZ if u["lang"] == "uz" else DENY_TEXT_RU)

    # Лимиты / пэйволл
    if not has_active_sub(u) and u["free_used"] >= FREE_LIMIT:
        return await message.answer(
            "💳 Доступ к ответам ограничен. Оформите подписку:",
            reply_markup=pay_kb()
        )

    # Тема → намёк системе
    topic_hint = TOPICS.get(u["topic"], {}).get("hint") if u.get("topic") else None

    # Ответ ИИ
    try:
        reply = await ask_gpt(text, topic_hint)
        await message.answer(reply)
    except Exception as e:
        logging.exception("OpenAI error: %s", e)
        return await message.answer("Извини, сервер перегружен. Попробуйте позже.")

    # Списываем бесплатный, если без подписки
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
            resp = await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
                json={"url": WEBHOOK_URL, "secret_token": WEBHOOK_SECRET}
            )
            logging.info("setWebhook: %s %s", resp.status_code, resp.text)

@app.get("/health")
async def health():
    return {"status": "ok"}
