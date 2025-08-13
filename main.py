import os
import re
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ---- Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# ============== LOGS ==============
logging.basicConfig(level=logging.INFO)

# ============== ENV ==============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # https://<app>.onrender.com/webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "savol_secret")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Live-поиск (через Tavily REST)
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

# Админ
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")  # str

# Персистентные файлы
USERS_DB_PATH = os.getenv("USERS_DB_PATH", "users_limits.json")
ANALYTICS_DB_PATH = os.getenv("ANALYTICS_DB_PATH", "analytics_events.jsonl")

# --- Google Sheets ENV ---
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")  # JSON одной строкой
SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
SHEETS_WORKSHEET = os.getenv("SHEETS_WORKSHEET", "Events")

# Белый список (VIP) — пользователи без лимитов
# Можно задавать через ENV WHITELIST_USERS="557891018,1942344627"
WL_RAW = os.getenv("WHITELIST_USERS", "557891018,1942344627")
try:
    WHITELIST_USERS = {int(x) for x in WL_RAW.split(",") if x.strip().isdigit()}
except Exception:
    WHITELIST_USERS = set()

def is_whitelisted(uid: int) -> bool:
    return uid in WHITELIST_USERS

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
app = FastAPI()

# ============== МОДЕРАЦИЯ ==============
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

# ============== ТАРИФЫ/ЛИМИТЫ ==============
FREE_LIMIT = 2
TARIFFS = {
    "start": {"title": "Старт", "price_uzs": 49_000,
              "desc": ["До 100 сообщений/мес", "Краткие и понятные ответы", "Без генерации файлов и картинок"],
              "active": True, "duration_days": 30},
    "business": {"title": "Бизнес", "price_uzs": 119_000,
                 "desc": ["До 500 сообщений/мес", "Инструкции и чек-листы", "Простые документы (docx/pdf)"],
                 "active": False, "duration_days": 30},
    "pro": {"title": "PRO", "price_uzs": 249_000,
            "desc": ["Высокие лимиты", "Картинки и сложные документы", "Приоритетная очередь"],
            "active": False, "duration_days": 30},
}

TOPICS = {
    "daily":   {"title_ru": "Быт", "title_uz": "Maishiy", "hint": "Практичные советы, чек-листы и шаги."},
    "finance": {"title_ru": "Финансы", "title_uz": "Moliya", "hint": "Объясняй с цифрами и примерами. Без рискованных персональных рекомендаций."},
    "gov":     {"title_ru": "Госуслуги", "title_uz": "Davlat xizmatlari", "hint": "Опиши процедуру, документы и шаги подачи."},
    "biz":     {"title_ru": "Бизнес", "title_uz": "Biznes", "hint": "Краткие инструкции по регистрации/отчётности/документам."},
    "edu":     {"title_ru": "Учёба", "title_uz": "Ta’lim", "hint": "Расскажи про поступление/обучение и шаги."},
    "it":      {"title_ru": "IT", "title_uz": "IT", "hint": "Технически и конкретно. Не советуй ничего незаконного."},
    "health":  {"title_ru": "Здоровье (общ.)", "title_uz": "Sog‘liq (umumiy)", "hint": "Только общая информация. Советуй обращаться к врачу."},
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

# ============== USERS (персистентно) ==============
# tg_id -> {...}
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
        path = Path(USERS_DB_PATH); path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump({str(k): _serialize_user(v) for k, v in USERS.items()}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning("save_users failed: %s", e)

def load_users():
    global USERS
    p = Path(USERS_DB_PATH)
    if not p.exists(): USERS = {}; return
    try:
        data = json.loads(p.read_text("utf-8"))
        USERS = {}
        for k, v in data.items():
            pu = v.get("paid_until")
            try: paid_until = datetime.fromisoformat(pu) if pu else None
            except Exception: paid_until = None
            USERS[int(k)] = {
                "free_used": int(v.get("free_used", 0)),
                "plan": v.get("plan", "free"),
                "paid_until": paid_until,
                "lang": v.get("lang", "ru"),
                "topic": v.get("topic"),
                "live": bool(v.get("live", False)),
            }
    except Exception:
        logging.exception("load_users failed")
        USERS = {}

def get_user(tg_id: int):
    u = USERS.get(tg_id)
    if not u:
        u = {"free_used": 0, "plan": "free", "paid_until": None, "lang": "ru", "topic": None, "live": False}
        USERS[tg_id] = u
        save_users()
    return u

def has_active_sub(u: dict) -> bool:
    return u["plan"] in ("start","business","pro") and u["paid_until"] and u["paid_until"] > datetime.utcnow()

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

# ============== ИИ ==============
BASE_SYSTEM_PROMPT = (
    "Ты — SavolBot, дружелюбный консультант. Отвечай кратко и ясно (до 6–8 предложений). "
    "Соблюдай законы Узбекистана. Не давай инструкции по незаконным действиям, подделкам, взломам, обходу систем. "
    "По медицине — только общая справка и совет обратиться к врачу. Язык ответа = язык вопроса (RU/UZ)."
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

# ============== LIVE SEARCH ==============
TIME_SENSITIVE_PATTERNS = [
    r"\b(сегодня|сейчас|на данный момент|актуальн|в \d{4} году|в 20\d{2})\b",
    r"\b(курс|зарплат|инфляц|ставк|цена|новост|статистик|прогноз)\b",
    r"\b(bugun|hozir|narx|kurs|yangilik)\b",
    r"\b(кто|как зовут|председател|директор|ceo|руководител)\b",
]
def is_time_sensitive(q: str) -> bool:
    return any(re.search(rx, q.lower()) for rx in TIME_SENSITIVE_PATTERNS)

CACHE_TTL_SECONDS = int(os.getenv("LIVE_CACHE_TTL", "86400"))
CACHE_MAX_ENTRIES = int(os.getenv("LIVE_CACHE_MAX", "500"))
LIVE_CACHE: dict[str, dict] = {}

def _norm_query(q: str) -> str: return re.sub(r"\s+", " ", q.strip().lower())
def cache_get(q: str):
    k=_norm_query(q); it=LIVE_CACHE.get(k)
    if not it: return None
    if time.time()-it["ts"]>CACHE_TTL_SECONDS: LIVE_CACHE.pop(k,None); return None
    return it["answer"]
def cache_set(q: str, a: str):
    if len(LIVE_CACHE)>=CACHE_MAX_ENTRIES:
        oldest=min(LIVE_CACHE,key=lambda x:LIVE_CACHE[x]["ts"]); LIVE_CACHE.pop(oldest,None)
    LIVE_CACHE[_norm_query(q)]={"ts":time.time(),"answer":a}

async def web_search_tavily(query: str, max_results: int = 5) -> dict | None:
    if not TAVILY_API_KEY: return None
    payload={"api_key":TAVILY_API_KEY,"query":query,"search_depth":"advanced","max_results":max_results,
             "include_answer":True,"include_domains":[]}
    async with httpx.AsyncClient(timeout=25.0) as client:
        r=await client.post("https://api.tavily.com/search", json=payload); r.raise_for_status(); return r.json()

async def answer_with_live_search(user_text: str, topic_hint: str | None) -> str:
    c=cache_get(user_text)
    if c: return c+"\n\n(из кэша за последние 24 часа)"
    data=await web_search_tavily(user_text)
    if not data: return await ask_gpt(user_text, topic_hint)
    snippets=[]; sources=[]
    for it in (data.get("results") or [])[:5]:
        title=(it.get("title") or "")[:120]; url=it.get("url") or ""; content=(it.get("content") or "")[:500]
        snippets.append(f"- {title}\n{content}\nИсточник: {url}")
        sources.append(f"• {title} — {url}")
    system=BASE_SYSTEM_PROMPT+" Отвечай, опираясь на источники. Кратко, по делу."
    if topic_hint: system+=f" Учитывай контекст темы: {topic_hint}"
    user_aug=f"{user_text}\n\nИСТОЧНИКИ:\n"+"\n\n".join(snippets)
    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload={"model":OPENAI_MODEL,"temperature":0.4,
             "messages":[{"role":"system","content":system},{"role":"user","content":user_aug}]}
    async with httpx.AsyncClient(timeout=30.0, base_url=OPENAI_API_BASE) as client:
        r=await client.post("/chat/completions", headers=headers, json=payload); r.raise_for_status()
        answer=r.json()["choices"][0]["message"]["content"].strip()
    final=answer+"\n\nИсточники:\n"+"\n".join(sources)
    cache_set(user_text, final); return final

# ============== АНАЛИТИКА: FILE + SHEETS ==============
_sheets_client = None
_sheets_ws = None

def _ts() -> str: return datetime.utcnow().isoformat()

_sheets_client = None
_sheets_ws = None

def _init_sheets():
    global _sheets_client, _sheets_ws

    if not (GOOGLE_CREDENTIALS and SHEETS_SPREADSHEET_ID and SHEETS_WORKSHEET):
        logging.error("Sheets env not set: GOOGLE_CREDENTIALS / SHEETS_SPREADSHEET_ID / SHEETS_WORKSHEET")
        return

    # 2.1 Парсим JSON и чётко логируем, если что-то не так
    try:
        creds_info = json.loads(GOOGLE_CREDENTIALS)
        # sanity-check
        assert "client_email" in creds_info and "private_key" in creds_info
    except Exception:
        logging.exception("Sheets creds JSON parse failed")
        return

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
        _sheets_client = gspread.authorize(creds)

        sh = _sheets_client.open_by_key(SHEETS_SPREADSHEET_ID)
        try:
            _sheets_ws = sh.worksheet(SHEETS_WORKSHEET)
        except gspread.WorksheetNotFound:
            _sheets_ws = sh.add_worksheet(title=SHEETS_WORKSHEET, rows=2000, cols=20)
            _sheets_ws.append_row(
                ["ts","user_id","event","topic","live","time_sensitive","mode","extra"],
                value_input_option="RAW"
            )
        logging.info("Sheets OK: spreadsheet=%s worksheet=%s", SHEETS_SPREADSHEET_ID, SHEETS_WORKSHEET)
    except Exception:
        logging.exception("Sheets init failed")
        _sheets_client = _sheets_ws = None

def _sheets_append(row: dict):
    if not _sheets_ws: return
    try:
        _sheets_ws.append_row(
            [
                row.get("ts",""),
                str(row.get("user_id","")),
                row.get("event",""),
                str(row.get("topic","")),
                "1" if row.get("live") else "0",
                "1" if row.get("time_sensitive") else "0",
                row.get("mode",""),
                json.dumps({k:v for k,v in row.items() if k not in {"ts","user_id","event","topic","live","time_sensitive","mode"}}, ensure_ascii=False)
            ],
            value_input_option="RAW"
        )
    except Exception as e:
        logging.warning("Sheets append failed: %s", e)

def log_event(user_id: int, name: str, **payload):
    row={"ts":_ts(), "user_id":user_id, "event":name, **payload}
    # файл JSONL
    try:
        p=Path(ANALYTICS_DB_PATH); p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False)+"\n")
    except Exception as e:
        logging.warning("log_event file failed: %s", e)
    # Google Sheets
    _sheets_append(row)

def format_stats(days: int | None = 7):
    p=Path(ANALYTICS_DB_PATH)
    if not p.exists(): return "Пока нет событий."
    cutoff = datetime.utcnow()-timedelta(days=days) if days else None
    evs=[]
    for line in p.read_text("utf-8").splitlines():
        try:
            e=json.loads(line)
            if cutoff:
                ts=datetime.fromisoformat(e.get("ts","").split("+")[0])
                if ts<cutoff: continue
            evs.append(e)
        except Exception:
            continue
    total=len(evs); users=len({e["user_id"] for e in evs})
    qs=[e for e in evs if e["event"]=="question"]
    topics=Counter((e.get("topic") or "—") for e in qs)
    grants=sum(1 for e in evs if e["event"]=="subscription_granted")
    paid_clicks=sum(1 for e in evs if e["event"]=="paid_done_click")
    active_now=sum(1 for u in USERS.values() if has_active_sub(u))
    lines=[
        f"📊 Статистика за {days} дн.",
        f"• Событий: {total} | Уник. пользователей: {users}",
        f"• Вопросов: {len(qs)} | Live-использований: {sum(1 for e in qs if e.get('live'))}",
        f"• Топ тем: "+(", ".join(f"{k}:{v}" for k,v in topics.most_common(6)) if topics else "—"),
        f"• Кнопка «Оплатил»: {paid_clicks} | Активаций подписки: {grants}",
        f"• Активных подписок сейчас: {active_now}"
    ]
    return "\n".join(lines)

# ============== КОМАНДЫ ==============
@dp.message(Command("start"))
async def cmd_start(message: Message):
    u=get_user(message.from_user.id)
    u["lang"]="uz" if is_uzbek(message.text or "") else "ru"; save_users()
    log_event(message.from_user.id,"start",lang=u["lang"])
    await message.answer(
        "👋 Привет! / Assalomu alaykum!\n"
        "Первые 2 ответа — бесплатно, дальше подписка «Старт».\n"
        "Выберите тему: /topics\n"
        "Для свежих данных включите live-поиск: /live_on"
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    log_event(message.from_user.id,"help")
    await message.answer(
        "ℹ️ Пишите вопрос (RU/UZ). /topics — выбор темы. /live_on — включить интернет-поиск.\n"
        "Первые 2 ответа — бесплатно; дальше /tariffs."
    )

@dp.message(Command("about"))
async def cmd_about(message: Message):
    log_event(message.from_user.id,"about")
    await message.answer("🤖 SavolBot от TripleA. В рамках закона РУз. Поддержать проект — /tariffs.")

@dp.message(Command("tariffs"))
async def cmd_tariffs(message: Message):
    u=get_user(message.from_user.id); log_event(message.from_user.id,"view_tariffs")
    await message.answer(tariffs_text(u["lang"]), reply_markup=pay_kb())

@dp.message(Command("myplan"))
async def cmd_myplan(message: Message):
    u=get_user(message.from_user.id)
    status="активна" if has_active_sub(u) else "нет"
    until=u["paid_until"].isoformat() if u["paid_until"] else "—"
    topic=u.get("topic") or "—"; live="вкл" if u.get("live") else "выкл"
    is_wl = is_whitelisted(message.from_user.id)
    plan_label = "whitelist (безлимит)" if is_wl else u["plan"]
    free_info = "безлимит" if is_wl else f"{u['free_used']}/{FREE_LIMIT}"
    log_event(message.from_user.id,"myplan_open", whitelisted=is_wl)
    await message.answer(
        f"Ваш план: {plan_label} | Live: {live}\n"
        f"Подписка: {status} (до {until})\n"
        f"Тема: {topic}\n"
        f"Бесплатно: {free_info}"
    )

@dp.message(Command("topics"))
async def cmd_topics(message: Message):
    u=get_user(message.from_user.id); lang=u["lang"]; log_event(message.from_user.id,"topics_open")
    head="🗂 Выберите тему:" if lang=="ru" else "🗂 Mavzuni tanlang:"
    await message.answer(head, reply_markup=topic_kb(lang, current=u.get("topic")))

@dp.message(Command("asklive"))
async def cmd_asklive(message: Message):
    u=get_user(message.from_user.id); q=message.text.replace("/asklive","",1).strip()
    if not q: return await message.answer("Напишите так: /asklive ваш вопрос")
    if (not is_whitelisted(message.from_user.id)) and (not has_active_sub(u)) and u["free_used"]>=FREE_LIMIT:
        log_event(message.from_user.id,"paywall_shown"); 
        return await message.answer("💳 Доступ ограничен. Оформите подписку:", reply_markup=pay_kb())
    topic_hint = TOPICS.get(u.get("topic"),{}).get("hint")
    try:
        reply=await answer_with_live_search(q, topic_hint); await message.answer(reply)
        log_event(message.from_user.id,"question",mode="asklive",topic=u.get("topic"),live=True,time_sensitive=True,
                  whitelisted=is_whitelisted(message.from_user.id))
    except Exception:
        logging.exception("Live error"); return await message.answer("Не получилось получить актуальные данные. Попробуйте позже.")
    if (not is_whitelisted(message.from_user.id)) and (not has_active_sub(u)):
        u["free_used"]+=1; save_users()

@dp.message(Command("live_on"))
async def cmd_live_on(message: Message):
    u=get_user(message.from_user.id); u["live"]=True; save_users(); log_event(message.from_user.id,"live_on")
    await message.answer("✅ Live-поиск включён.")

@dp.message(Command("live_off"))
async def cmd_live_off(message: Message):
    u=get_user(message.from_user.id); u["live"]=False; save_users(); log_event(message.from_user.id,"live_off")
    await message.answer("⏹ Live-поиск выключён.")

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if ADMIN_CHAT_ID and str(message.from_user.id)!=str(ADMIN_CHAT_ID):
        return await message.answer("Команда доступна администратору.")
    parts=message.text.strip().split()
    days=int(parts[1]) if len(parts)>1 and parts[1].isdigit() else 7
    await message.answer(format_stats(days))

@dp.message(Command("gs_test"))
async def cmd_gs_test(message: Message):
    if ADMIN_CHAT_ID and str(message.from_user.id) != str(ADMIN_CHAT_ID):
        return await message.answer("Команда доступна администратору.")

    try:
        if _sheets_ws is None:
            return await message.answer(
                "❌ Sheets не инициализирован. "
                "Проверь ENV: GOOGLE_CREDENTIALS, SHEETS_SPREADSHEET_ID, SHEETS_WORKSHEET. "
                "См. логи: 'Sheets init failed' или 'env not set'."
            )

        _sheets_ws.append_row(
            [datetime.utcnow().isoformat(), str(message.from_user.id), "gs_test", "", "0", "0", "manual", "{}"],
            value_input_option="RAW"
        )
        await message.answer(f"✅ Google Sheets OK: запись в '{SHEETS_WORKSHEET}' прошла успешно.")
    except Exception as e:
        await message.answer(f"❌ Ошибка записи в Google Sheets: {e}\n"
                             f"Проверь доступ сервис-аккаунта к таблице и корректность GOOGLE_CREDENTIALS.")

@dp.message(Command("gs_info"))
async def cmd_gs_info(message: Message):
    ok = _sheets_ws is not None
    short = (GOOGLE_CREDENTIALS or "")[:40]
    await message.answer(
        "Sheets: {ok}\nWS: {ws}\nSPREADSHEET_ID: {sid}\nCRED startswith: {short}".format(
            ok="OK" if ok else "NOT INIT",
            ws=SHEETS_WORKSHEET,
            sid=SHEETS_SPREADSHEET_ID,
            short=short.replace("\n", "\\n")
        )
    )

@dp.message(Command("gs_test"))
async def cmd_gs_test(message: Message):
    if not _sheets_ws:
        return await message.answer("❌ Sheets не инициализирован. Сначала почини ENV и перезапусти.")
    try:
        _sheets_ws.append_row(
            [datetime.utcnow().isoformat(), str(message.from_user.id), "gs_test", "", "0", "0", "manual", "{}"],
            value_input_option="RAW"
        )
        await message.answer("✅ Записал тестовую строку в Google Sheets.")
    except Exception:
        logging.exception("gs_test append failed")
        await message.answer("❌ Не смог записать в Google Sheets. Смотри логи.")

@dp.message(Command("gs_debug"))
async def cmd_gs_debug(message: Message):
    # Печатаем, что видит процесс — без секретов
    has_env = all([GOOGLE_CREDENTIALS, SHEETS_SPREADSHEET_ID, SHEETS_WORKSHEET])
    msg = [
        f"ENV OK: {has_env}",
        f"ID: {SHEETS_SPREADSHEET_ID or '-'}",
        f"WS: {SHEETS_WORKSHEET or '-'}",
        f"Cred len: {len(GOOGLE_CREDENTIALS or '')}",
        f"Sheets inited: {bool(_sheets_ws)}",
    ]
    await message.answer("\n".join(msg))

# ============== CALLBACKS ==============
@dp.callback_query(F.data=="show_tariffs")
async def cb_show_tariffs(call: CallbackQuery):
    u=get_user(call.from_user.id); log_event(call.from_user.id,"view_tariffs_click")
    await call.message.edit_text(tariffs_text(u["lang"]), reply_markup=pay_kb()); await call.answer()

@dp.callback_query(F.data=="subscribe_start")
async def cb_subscribe_start(call: CallbackQuery):
    log_event(call.from_user.id,"subscribe_start_open")
    pay_link="https://pay.example.com/savolbot/start"  # заглушка
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Готово (я оплатил)", callback_data="paid_start_done")]])
    await call.message.answer(f"💳 «Старт» — {TARIFFS['start']['price_uzs']:,} сум/мес.\nОплата: {pay_link}", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data=="paid_start_done")
async def cb_paid_done(call: CallbackQuery):
    log_event(call.from_user.id,"paid_done_click")
    if ADMIN_CHAT_ID:
        await bot.send_message(int(ADMIN_CHAT_ID),
            f"👤 @{call.from_user.username or call.from_user.id} запросил активацию «Старт».\n"
            f"TG ID: {call.from_user.id}\nИспользуйте /grant_start {call.from_user.id}")
    await call.message.answer("Спасибо! Мы проверим оплату и активируем подписку."); await call.answer()

@dp.callback_query(F.data.startswith("topic:"))
async def cb_topic(call: CallbackQuery):
    u=get_user(call.from_user.id); _, key = call.data.split(":",1)
    if key=="close":
        try: await call.message.delete()
        except Exception: pass
        return await call.answer("OK")
    if key in TOPICS:
        u["topic"]=key; save_users()
        lang=u["lang"]; title=TOPICS[key]["title_uz"] if lang=="uz" else TOPICS[key]["title_ru"]
        log_event(call.from_user.id,"topic_select",topic=key)
        await call.message.edit_reply_markup(reply_markup=topic_kb(lang, current=key))
        await call.answer(f"Выбрана тема: {title}" if lang=="ru" else f"Mavzu tanlandi: {title}")

@dp.message(Command("grant_start"))
async def cmd_grant_start(message: Message):
    if str(message.from_user.id)!=str(ADMIN_CHAT_ID): return await message.answer("Команда недоступна.")
    parts=message.text.strip().split()
    if len(parts)!=2 or not parts[1].isdigit(): return await message.answer("Использование: /grant_start <tg_id>")
    target_id=int(parts[1]); u=get_user(target_id)
    u["plan"]="start"; u["paid_until"]=datetime.utcnow()+timedelta(days=TARIFFS["start"]["duration_days"]); save_users()
    log_event(message.from_user.id,"subscription_granted",target=target_id,plan="start",paid_until=u["paid_until"].isoformat())
    await message.answer(f"✅ Активирован «Старт» для {target_id} до {u['paid_until'].isoformat()}")
    try: await bot.send_message(target_id,"✅ Подписка «Старт» активирована. Приятного использования!")
    except Exception: logging.warning("Notify user failed")

# ============== ОБРАБОТЧИК ВОПРОСОВ ==============
@dp.message(F.text)
async def handle_text(message: Message):
    text=message.text.strip(); u=get_user(message.from_user.id)
    if is_uzbek(text): u["lang"]="uz"; save_users()
    if violates_policy(text):
        log_event(message.from_user.id,"question_blocked",reason="policy")
        return await message.answer(DENY_TEXT_UZ if u["lang"]=="uz" else DENY_TEXT_RU)
    if (not is_whitelisted(message.from_user.id)) and (not has_active_sub(u)) and u["free_used"]>=FREE_LIMIT:
        log_event(message.from_user.id,"paywall_shown")
        return await message.answer("💳 Доступ к ответам ограничен. Оформите подписку:", reply_markup=pay_kb())
    topic_hint = TOPICS.get(u.get("topic"),{}).get("hint")
    time_sens=is_time_sensitive(text); use_live = u.get("live") or time_sens
    try:
        reply = await (answer_with_live_search(text, topic_hint) if use_live else ask_gpt(text, topic_hint))
        await message.answer(reply)
        log_event(message.from_user.id,"question",topic=u.get("topic"),live=use_live,time_sensitive=time_sens,
                  whitelisted=is_whitelisted(message.from_user.id))
    except Exception:
        logging.exception("OpenAI error"); return await message.answer("Извини, сервер перегружен. Попробуйте позже.")
    if (not is_whitelisted(message.from_user.id)) and (not has_active_sub(u)):
        u["free_used"]+=1; save_users()

# ============== WEBHOOK/STARTUP ==============
@app.post("/webhook")
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return {"ok": False, "error": "bad secret"}
    update = Update.model_validate(await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    load_users()
    logging.info("Users DB: %s", USERS_DB_PATH)
    logging.info("Analytics file: %s", ANALYTICS_DB_PATH)
    _init_sheets()  # подключаем Google Sheets
    if TELEGRAM_TOKEN and WEBHOOK_URL:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp=await client.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
                                   json={"url": WEBHOOK_URL, "secret_token": WEBHOOK_SECRET})
            logging.info("setWebhook: %s %s", resp.status_code, resp.text)

@app.get("/health")
async def health(): return {"status": "ok"}
