import os
import re
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, Update, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)

# ================== ЛОГИ ==================
logging.basicConfig(level=logging.INFO)

# ================== ENV ==================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # https://<app>.onrender.com/webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "savol_secret")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Live-поиск
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")  # если нет — фолбек без поиска

# Админ для ручной активации (MVP)
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")  # например "123456789"

# Персистентное хранилище пользователей
USERS_DB_PATH = os.getenv("USERS_DB_PATH", "data/users.json")

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

# ================== ТАРИФЫ/ЛИМИТЫ ==================
FREE_LIMIT = 2
TARIFFS = {
    "start": {
        "title": "Старт",
        "price_uzs": 49_000,
        "desc": ["До 100 сообщений/мес", "Краткие и понятные ответы", "Без генерации файлов и картинок"],
        "active": True,
        "duration_days": 30,
    },
    "business": {"title": "Бизнес", "price_uzs": 119_000,
                 "desc": ["До 500 сообщений/мес", "Инструкции и чек-листы", "Простые документы (docx/pdf)"],
                 "active": False, "duration_days": 30},
    "pro": {"title": "PRO", "price_uzs": 249_000,
            "desc": ["Высокие лимиты", "Картинки и сложные документы", "Приоритетная очередь"],
            "active": False, "duration_days": 30},
}

# ================== ТЕМЫ ==================
TOPICS = {
    "daily":   {"title_ru": "Быт",              "title_uz": "Maishiy",            "hint": "Практичные советы, чек-листы и шаги."},
    "finance": {"title_ru": "Финансы",          "title_uz": "Moliya",             "hint": "Объясняй с цифрами и примерами. Без рискованных персональных рекомендаций."},
    "gov":     {"title_ru": "Госуслуги",        "title_uz": "Davlat xizmatlari", "hint": "Опиши процедуру, документы и шаги подачи."},
    "biz":     {"title_ru": "Бизнес",           "title_uz": "Biznes",             "hint": "Краткие инструкции по регистрации/отчётности/документам."},
    "edu":     {"title_ru": "Учёба",            "title_uz": "Ta’lim",             "hint": "Расскажи про поступление/обучение и шаги."},
    "it":      {"title_ru": "IT",               "title_uz": "IT",                 "hint": "Технически и конкретно. Не советуй ничего незаконного."},
    "health":  {"title_ru": "Здоровье (общ.)",  "title_uz": "Sog‘liq (umumiy)",   "hint": "Только общая информация. Советуй обращаться к врачу."},
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

# ================== ПЕРСИСТЕНТНОЕ ХРАНИЛИЩЕ USERS ==================
# tg_id -> {"free_used": int, "plan": str, "paid_until": dt|None, "lang": "ru"/"uz", "topic": str|None, "live": bool}
USERS: dict[int, dict] = {}

def _serialize_user(u: dict) -> dict:
    return {
        "free_used": int(u.get("free_used", 0)),
        "plan": u.get("plan", "free"),
        "paid_until": u["paid_until"].isoformat() if u.get("paid_until") else None,
        "lang": u.get("lang", "ru"),
        "topic": u.get("topic"),
        "live": bool(u.get("live", False)),
    }

def save_users():
    try:
        path = Path(USERS_DB_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        to_dump = {str(k): _serialize_user(v) for k, v in USERS.items()}
        with path.open("w", encoding="utf-8") as f:
            json.dump(to_dump, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning("save_users failed: %s", e)

def load_users():
    global USERS
    path = Path(USERS_DB_PATH)
    if not path.exists():
        USERS = {}
        return
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        USERS = {}
        for k, v in data.items():
            pu = v.get("paid_until")
            try:
                paid_until = datetime.fromisoformat(pu) if pu else None
            except Exception:
                paid_until = None
            USERS[int(k)] = {
                "free_used": int(v.get("free_used", 0)),
                "plan": v.get("plan", "free"),
                "paid_until": paid_until,
                "lang": v.get("lang", "ru"),
                "topic": v.get("topic"),
                "live": bool(v.get("live", False)),
            }
        logging.info("Users loaded: %d", len(USERS))
    except Exception as e:
        logging.exception("load_users failed: %s")
        USERS = {}

def get_user(tg_id: int):
    u = USERS.get(tg_id)
    if not u:
        u = {"free_used": 0, "plan": "free", "paid_until": None, "lang": "ru", "topic": None, "live": False}
        USERS[tg_id] = u
        save_users()
    return u

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
    for key in ("start","business","pro"):
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
    system = BASE_SYSTEM_PROMPT + (f" Учитывай контекст темы: {topic_hint}" if topic_hint else "")
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {"model": OPENAI_MODEL, "temperature": 0.6,
               "messages": [{"role": "system", "content": system},
                            {"role": "user", "content": user_text}]}
    async with httpx.AsyncClient(timeout=30.0, base_url=OPENAI_API_BASE) as client:
        r = await client.post("/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()

# ---- Live-поиск ----
TIME_SENSITIVE_PATTERNS = [
    r"\b(сегодня|сейчас|на данный момент|актуальн|в \d{4} году|в 20\d{2})\b",
    r"\b(курс|зарплат|инфляц|ставк|цена|новост|статистик|прогноз)\b",
    r"\b(bugun|hozir|narx|kurs|yangilik)\b",
    r"\b(кто|как зовут|фамилия|председател[ья]?|директор|гендиректор|ceo|chairman|руководител[ья]?)\b",
    r"\b(банк|акб|ооо|ао|компания|министерств[оа])\b.*",
]
def is_time_sensitive(q: str) -> bool:
    t = q.lower()
    return any(re.search(rx, t) for rx in TIME_SENSITIVE_PATTERNS)

# ====== КЭШ ДЛЯ LIVE-ПОИСКА ======
CACHE_TTL_SECONDS = int(os.getenv("LIVE_CACHE_TTL", "86400"))  # 24 часа
CACHE_MAX_ENTRIES = int(os.getenv("LIVE_CACHE_MAX", "500"))
LIVE_CACHE: dict[str, dict] = {}  # key -> {"ts": float, "answer": str}

def _norm_query(q: str) -> str:
    return re.sub(r"\s+", " ", q.strip().lower())

def cache_get(q: str) -> str | None:
    k = _norm_query(q)
    item = LIVE_CACHE.get(k)
    if not item:
        return None
    if time.time() - item["ts"] > CACHE_TTL_SECONDS:
        LIVE_CACHE.pop(k, None)
        return None
    return item["answer"]

def cache_set(q: str, answer: str):
    if len(LIVE_CACHE) >= CACHE_MAX_ENTRIES:
        oldest_key = min(LIVE_CACHE, key=lambda kk: LIVE_CACHE[kk]["ts"])
        LIVE_CACHE.pop(oldest_key, None)
    LIVE_CACHE[_norm_query(q)] = {"ts": time.time(), "answer": answer}

async def web_search_tavily(query: str, max_results: int = 5) -> dict | None:
    if not TAVILY_API_KEY:
        return None
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "max_results": max_results,
        "include_answer": True,
        "include_domains": [],
    }
    async with httpx.AsyncClient(timeout=25.0) as client:
        r = await client.post("https://api.tavily.com/search", json=payload)
        r.raise_for_status()
        return r.json()

async def answer_with_live_search(user_text: str, topic_hint: str | None) -> str:
    cached = cache_get(user_text)
    if cached:
        return cached + "\n\n(из кэша за последние 24 часа)"

    data = await web_search_tavily(user_text)
    if not data:
        return await ask_gpt(user_text, topic_hint)

    snippets, sources_for_user = [], []
    for item in (data.get("results") or [])[:5]:
        title = (item.get("title") or "")[:120]
        url = item.get("url") or ""
        content = (item.get("content") or "")[:500]
        snippets.append(f"- {title}\n{content}\nИсточник: {url}")
        sources_for_user.append(f"• {title} — {url}")

    system = BASE_SYSTEM_PROMPT + " Отвечай, опираясь на предоставленные источники. Кратко, по делу."
    if topic_hint:
        system += f" Учитывай контекст темы: {topic_hint}"
    user_augmented = f"{user_text}\n\nИСТОЧНИКИ:\n" + "\n\n".join(snippets)

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {"model": OPENAI_MODEL, "temperature": 0.4,
               "messages": [{"role": "system", "content": system},
                            {"role": "user", "content": user_augmented}]}
    async with httpx.AsyncClient(timeout=30.0, base_url=OPENAI_API_BASE) as client:
        r = await client.post("/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        answer = r.json()["choices"][0]["message"]["content"].strip()

    tail = "\n\nИсточники:\n" + "\n".join(sources_for_user)
    final_answer = answer + tail

    cache_set(user_text, final_answer)
    return final_answer

# ================== КОМАНДЫ ==================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    u = get_user(message.from_user.id)
    # Обновим язык и сохраним
    u["lang"] = "uz" if is_uzbek(message.text or "") else "ru"
    save_users()
    await message.answer(
        "👋 Привет! / Assalomu alaykum!\n"
        "Первые 2 ответа — бесплатно, дальше подписка «Старт».\n"
        "Задайте вопрос или выберите тему: /topics\n"
        "Для свежих данных включите live-поиск: /live_on"
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ Как пользоваться:\n"
        "1) Пишите вопрос (RU/UZ). Язык ответа = язык вопроса.\n"
        "2) /topics — выберите тему для более точных ответов.\n"
        "3) Первые 2 ответа — бесплатно; дальше /tariffs.\n"
        "4) /live_on — включить интернет-поиск; /live_off — выключить.\n"
        "5) /asklive вопрос — разовый live-поиск."
    )

@dp.message(Command("about"))
async def cmd_about(message: Message):
    await message.answer(
        "🤖 SavolBot — проект TripleA. Ответы на базе GPT, строго в рамках закона РУз.\n"
        "Поддержать проект и снять лимиты — /tariffs."
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
    topic = u.get("topic") or "—"
    live = "вкл" if u.get("live") else "выкл"
    await message.answer(
        f"Ваш план: {u['plan']} | Live-поиск: {live}\nПодписка: {status} (до {until})\n"
        f"Тема: {topic}\nБесплатных использовано: {u['free_used']}/{FREE_LIMIT}"
    )

@dp.message(Command("topics"))
async def cmd_topics(message: Message):
    u = get_user(message.from_user.id)
    lang = u["lang"]
    head = "🗂 Выберите тему (не влияет на цену, только на стиль ответа):" if lang == "ru" \
        else "🗂 Mavzuni tanlang (narxga ta’sir qilmaydi, faqat javob ohangiga):"
    await message.answer(head, reply_markup=topic_kb(lang, current=u.get("topic")))

@dp.message(Command("asklive"))
async def cmd_asklive(message: Message):
    u = get_user(message.from_user.id)
    q = message.text.replace("/asklive", "", 1).strip()
    if not q:
        return await message.answer("Напишите так: /asklive ваш вопрос")
    if not has_active_sub(u) and u["free_used"] >= FREE_LIMIT:
        return await message.answer("💳 Доступ к ответам ограничен. Оформите подписку:", reply_markup=pay_kb())
    topic_hint = TOPICS.get(u.get("topic"), {}).get("hint") if u.get("topic") else None
    try:
        reply = await answer_with_live_search(q, topic_hint)
        await message.answer(reply)
    except Exception as e:
        logging.exception("Live error: %s", e)
        return await message.answer("Не получилось получить актуальные данные. Попробуйте позже.")
    if not has_active_sub(u):
        u["free_used"] += 1
        save_users()

@dp.message(Command("live_on"))
async def cmd_live_on(message: Message):
    u = get_user(message.from_user.id)
    u["live"] = True
    save_users()
    await message.answer("✅ Live-поиск включён. Все ваши вопросы будут проверяться по интернет-источникам.")

@dp.message(Command("live_off"))
async def cmd_live_off(message: Message):
    u = get_user(message.from_user.id)
    u["live"] = False
    save_users()
    await message.answer("⏹ Live-поиск выключён. Вернулись к обычным ответам модели.")

# ---- cache utils ----
@dp.message(Command("cache_info"))
async def cmd_cache_info(message: Message):
    size = len(LIVE_CACHE)
    ttl_h = round(CACHE_TTL_SECONDS / 3600, 1)
    await message.answer(f"🗄 Кэш live-поиска: {size} записей, TTL ≈ {ttl_h} ч., max {CACHE_MAX_ENTRIES}")

@dp.message(Command("cache_clear"))
async def cmd_cache_clear(message: Message):
    if ADMIN_CHAT_ID and str(message.from_user.id) != str(ADMIN_CHAT_ID):
        return await message.answer("Команда доступна администратору.")
    LIVE_CACHE.clear()
    await message.answer("🧹 Кэш live-поиска очищен.")

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
    await call.message.answer(
        f"💳 «Старт» — {TARIFFS['start']['price_uzs']:,} сум/мес.\nОплата: {pay_link}", reply_markup=kb
    )
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
        save_users()
        lang = u["lang"]
        title = TOPICS[key]["title_uz"] if lang == "uz" else TOPICS[key]["title_ru"]
        await call.message.edit_reply_markup(reply_markup=topic_kb(lang, current=key))
        await call.answer(f"Выбрана тема: {title}" if lang == "ru" else f"Mavzu tanlandi: {title}")

# Админ: ручная активация (MVP)
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
    save_users()
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

    # язык
    if is_uzbek(text):
        u["lang"] = "uz"
        save_users()

    # модерация
    if violates_policy(text):
        return await message.answer(DENY_TEXT_UZ if u["lang"] == "uz" else DENY_TEXT_RU)

    # лимиты
    if not has_active_sub(u) and u["free_used"] >= FREE_LIMIT:
        return await message.answer("💳 Доступ к ответам ограничен. Оформите подписку:", reply_markup=pay_kb())

    # тема → подсказка
    topic_hint = TOPICS.get(u.get("topic"), {}).get("hint") if u.get("topic") else None

    # если включён per-user live или вопрос «про сейчас» → live-поиск
    try:
        if u.get("live") or is_time_sensitive(text):
            reply = await answer_with_live_search(text, topic_hint)
        else:
            reply = await ask_gpt(text, topic_hint)
        await message.answer(reply)
    except Exception as e:
        logging.exception("OpenAI error: %s", e)
        return await message.answer("Извини, сервер перегружен. Попробуйте позже.")

    if not has_active_sub(u):
        u["free_used"] += 1
        save_users()

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
    # Загружаем пользователей с диска
    load_users()
    logging.info("Users DB path: %s", USERS_DB_PATH)

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
