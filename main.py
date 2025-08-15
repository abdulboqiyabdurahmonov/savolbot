import os
import re
import json
import time
import logging
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ---- Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# ================== LOGS ==================
logging.basicConfig(level=logging.INFO)

# ================== ENV ===================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # https://<app>.onrender.com/webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "savol_secret")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Live-поиск (через Tavily)
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

# Админ
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")  # str

# Персистентные файлы
USERS_DB_PATH = os.getenv("USERS_DB_PATH", "users_limits.json")
ANALYTICS_DB_PATH = os.getenv("ANALYTICS_DB_PATH", "analytics_events.jsonl")
HISTORY_DB_PATH = os.getenv("HISTORY_DB_PATH", "chat_history.json")

# --- Google Sheets ENV ---
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")  # JSON одной строкой (или base64)
SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")

# --- HTTPX clients & timeouts (reuse) ---
HTTPX_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=15.0)
client_openai: httpx.AsyncClient | None = None
client_http: httpx.AsyncClient | None = None

# Белый список (VIP) — пользователи без лимитов
WL_RAW = os.getenv("WHITELIST_USERS", "557891018,1942344627")
try:
    WHITELIST_USERS = {int(x) for x in WL_RAW.split(",") if x.strip().isdigit()}
except Exception:
    WHITELIST_USERS = set()

def is_whitelisted(uid: int) -> bool:
    return uid in WHITELIST_USERS

# ================== TG/APP =================
bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
dp = Dispatcher()

# ================== USERS (персистентно) =================
USERS: dict[int, dict] = {}

def _serialize_user(u: dict) -> dict:
    return {
        "free_used": int(u.get("free_used", 0)),
        "plan": u.get("plan", "free"),
        "paid_until": u["paid_until"].isoformat() if u.get("paid_until") else None,
        "lang": u.get("lang", "ru"),
        "topic": u.get("topic"),
        "live": bool(u.get("live", False)),  # совместимость
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
    if not p.exists():
        USERS = {}; return
    try:
        data = json.loads(p.read_text("utf-8"))
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
    except Exception:
        logging.exception("load_users failed")
        USERS = {}

# ====== ТАРИФЫ ======
# Оставили один тариф — Creative (10$).
FREE_LIMIT = 5
TARIFFS = {
    "creative": {"title": "Creative", "price_usd": 10,
                 "desc": ["GPT-помощник в Telegram", "Создание картинок и базовых документов", "Без лимита сообщений*"],
                 "active": True, "duration_days": 30},
}
DEFAULT_PLAN = "creative"

def has_active_sub(u: dict) -> bool:
    return u["plan"] in ("creative",) and u.get("paid_until") and u["paid_until"] > datetime.utcnow()

def get_user(tg_id: int):
    is_new = tg_id not in USERS
    if is_new:
        USERS[tg_id] = {"free_used": 0, "plan": "free", "paid_until": None, "lang": "ru", "topic": None, "live": False}
        save_users()
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_metrics_users_count_async())
        except RuntimeError:
            pass
    return USERS[tg_id]

def pay_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оформить «Creative»", callback_data="subscribe_creative")],
        [InlineKeyboardButton(text="ℹ️ Тариф", callback_data="show_tariffs")]
    ])

# ================== ИСТОРИЯ ДИАЛОГА =================
HISTORY: dict[int, list[dict]] = {}

def _hist_path() -> Path:
    p = Path(HISTORY_DB_PATH); p.parent.mkdir(parents=True, exist_ok=True); return p

def load_history():
    global HISTORY
    p = _hist_path()
    if p.exists():
        try:
            HISTORY = {int(k): v for k, v in json.loads(p.read_text("utf-8")).items()}
        except Exception:
            logging.exception("load_history failed"); HISTORY = {}
    else:
        HISTORY = {}

def save_history():
    try:
        _hist_path().write_text(json.dumps({str(k): v for k, v in HISTORY.items()}, ensure_ascii=False, indent=2), "utf-8")
    except Exception:
        logging.exception("save_history failed")

def reset_history(user_id: int):
    HISTORY.pop(user_id, None); save_history()

def append_history(user_id: int, role: str, content: str):
    lst = HISTORY.setdefault(user_id, [])
    lst.append({"role": role, "content": content, "ts": datetime.utcnow().isoformat()})
    if len(lst) > 20:
        del lst[: len(lst) - 20]
    save_history()
    # Запись в Sheets → неблокирующе
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_sheets_append_history_async(user_id, role, content))
    except RuntimeError:
        pass

def get_recent_history(user_id: int, max_chars: int = 6000) -> list[dict]:
    total = 0; picked = []
    for item in reversed(HISTORY.get(user_id, [])):
        c = item.get("content") or ""
        total += len(c)
        if total > max_chars:
            break
        picked.append({"role": item["role"], "content": c})
    return list(reversed(picked))

def build_messages(user_id: int, system: str, user_text: str) -> list[dict]:
    msgs = [{"role": "system", "content": system}]
    msgs.extend(get_recent_history(user_id))
    msgs.append({"role": "user", "content": user_text})
    return msgs

# ================== ЯЗЫК / МОДЕРАЦИЯ =================
def is_uzbek(text: str) -> bool:
    t = (text or "").lower()
    return bool(re.search(r"[ғқҳў]", t) or re.search(r"\b(ha|yo[’']q|iltimos|rahmat|salom)\b", t))

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

# ================== АНТИССЫЛКИ =================
LINK_PAT = re.compile(r"https?://\S+")
MD_LINK_PAT = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)")
SOURCES_BLOCK_PAT = re.compile(r"(?is)\n+источники:\s*.*$")

def strip_links(text: str) -> str:
    if not text:
        return text
    text = MD_LINK_PAT.sub(r"\1", text)
    text = LINK_PAT.sub("", text)
    text = SOURCES_BLOCK_PAT.sub("", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

# ================== ТЕКСТЫ ПО ЯЗЫКУ =================
def t(lang: str, key: str) -> str:
    ru = {
        "welcome": ("👋 Привет! Я SavolBot от TripleA.\n\n"
                    "Мы делаем автообзвоны, чат-боты и GPT прямо в Telegram.\n"
                    "Наша фишка: почти как ChatGPT по возможностям, но в 2 раза дешевле и прямо в мессенджере.\n\n"
                    f"Первые {FREE_LIMIT} ответов — бесплатно. Для безлимита и генерации картинок/документов оформите тариф «Creative».\n"
                    "Команды: /topics /tariffs /help"),
        "help": ("ℹ️ Напишите вопрос (RU/UZ). Если нужно — подключу интернет-поиск. "
                 f"Первые {FREE_LIMIT} ответов — бесплатно; дальше /tariffs."),
        "thinking": "🤔 Думаю… собираю информацию…",
        "ready": "✅ Готово. Отправляю ответ…",
        "paywall": "💳 Доступ к ответам ограничен. Оформите подписку:",
        "plan_title": "Ваш план",
        "choose_topic": "🗂 Выберите тему:",
        "tariff_title": "⭐ Creative (доступен)\nЦена: 10 $/мес\n• GPT в Telegram\n• Картинки и базовые документы\n• Сообщения без лимитов*",
        "thanks_paid": "Спасибо! Мы проверим оплату и активируем подписку.",
        "sub_activated_user": "✅ Подписка «Creative» активирована. Приятного использования!",
        "admin_only": "Команда доступна администратору.",
        "context_cleared": "🧹 Контекст очищен. Начинаем новую тему.",
    }
    uz = {
        "welcome": ("👋 Assalomu alaykum! Men SavolBot, TripleA jamoasidan.\n\n"
                    "Biz autoqo‘ng‘iroqlar, chat-botlar va GPT’ni to‘g‘ridan-to‘g‘ri Telegram’da taqdim etamiz.\n"
                    "Afzallik: ChatGPT’ga deyarli teng imkoniyatlar, lekin 2 baravar arzon va o‘sha messenjerda.\n\n"
                    f"Dastlabki {FREE_LIMIT} javob — bepul. Cheksiz va rasm/hujjat yaratish uchun «Creative» tarifini tanlang.\n"
                    "Buyruqlar: /topics /tariffs /help"),
        "help": (f"ℹ️ Savolingizni yozing (RU/UZ). Kerak bo‘lsa, internet-qidiruvni qo‘llayman. "
                 f"Dastlabki {FREE_LIMIT} javob — bepul; keyin /tariffs."),
        "thinking": "🤔 O‘ylayapman… ma’lumot to‘playapman…",
        "ready": "✅ Tayyor. Javobni yuborayapman…",
        "paywall": "💳 Javoblar cheklangan. Obuna rasmiylashtiring:",
        "plan_title": "Sizning rejangiz",
        "choose_topic": "🗂 Mavzuni tanlang:",
        "tariff_title": "⭐ Creative (faol)\nNarx: oyiga 10 $\n• Telegram’da GPT\n• Rasmlar va oddiy hujjatlar\n• Cheksiz xabarlar*",
        "thanks_paid": "Rahmat! To‘lovni tekshiramiz va obunani faollashtiramiz.",
        "sub_activated_user": "✅ «Creative» obunasi faollashtirildi. Omad!",
        "admin_only": "Bu buyruq faqat administrator uchun.",
        "context_cleared": "🧹 Kontekst tozalandi. Yangi mavzuni boshlaymiz.",
    }
    return (uz if lang == "uz" else ru)[key]

# ================== SHEETS =================
_sheets_client: gspread.Client | None = None
LAST_SHEETS_ERROR: str | None = None

def _ts() -> str:
    return datetime.utcnow().isoformat()

def _open_spreadsheet():
    if not _sheets_client:
        return None
    try:
        return _sheets_client.open_by_key(SHEETS_SPREADSHEET_ID)
    except Exception:
        logging.exception("open_by_key failed")
        return None

def _users_ws():
    sh = _open_spreadsheet()
    if not sh:
        return None
    try:
        return sh.worksheet("Users")
    except gspread.WorksheetNotFound:
        try:
            ws = sh.add_worksheet(title="Users", rows=100000, cols=8)
            ws.append_row(["ts","user_id","username","first_name","last_name","lang","plan","free_used"], value_input_option="RAW")
            return ws
        except Exception:
            logging.exception("Create Users ws failed")
            return None
    except Exception:
        logging.exception("_users_ws failed")
        return None

def _history_ws():
    sh = _open_spreadsheet()
    if not sh:
        return None
    try:
        return sh.worksheet("History")
    except gspread.WorksheetNotFound:
        try:
            ws = sh.add_worksheet(title="History", rows=500000, cols=4)
            ws.append_row(["ts", "user_id", "role", "content"], value_input_option="RAW")
            return ws
        except Exception:
            logging.exception("Create History ws failed")
            return None
    except Exception:
        logging.exception("_history_ws failed")
        return None

def _metrics_ws():
    sh = _open_spreadsheet()
    if not sh:
        return None
    try:
        return sh.worksheet("Metrics")
    except gspread.WorksheetNotFound:
        try:
            ws = sh.add_worksheet(title="Metrics", rows=10000, cols=3)
            ws.append_row(["ts", "total_users", "active_subs"], value_input_option="RAW")
            return ws
        except Exception:
            logging.exception("Create Metrics ws failed")
            return None
    except Exception:
        logging.exception("_metrics_ws failed")
        return None

def _init_sheets():
    """
    Жёсткая инициализация Google Sheets:
    - поддержка raw JSON и base64
    - нормализация private_key с \\n -> \n
    - добавлен Drive-scope
    - создаём листы и заголовки при отсутствии
    """
    global _sheets_client, LAST_SHEETS_ERROR

    if not (GOOGLE_CREDENTIALS and SHEETS_SPREADSHEET_ID):
        LAST_SHEETS_ERROR = "Sheets env not set: GOOGLE_CREDENTIALS / SHEETS_SPREADSHEET_ID"
        logging.error(LAST_SHEETS_ERROR)
        return

    try:
        raw = GOOGLE_CREDENTIALS.strip()
        try:
            import base64
            creds_text = base64.b64decode(raw).decode("utf-8") if not raw.lstrip().startswith("{") else raw
        except Exception:
            creds_text = raw

        creds_info = json.loads(creds_text)

        # Нормализуем private_key (если пришёл с литеральными \n)
        if isinstance(creds_info, dict) and creds_info.get("private_key"):
            pk = creds_info["private_key"]
            if "BEGIN PRIVATE KEY" in pk and "\\n" in pk:
                creds_info["private_key"] = pk.replace("\\n", "\n")

        # Базовая валидация
        for field in ("client_email", "private_key"):
            if not creds_info.get(field):
                raise ValueError(f"GOOGLE_CREDENTIALS is missing '{field}'")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
        _sheets_client = gspread.authorize(creds)

        # Убедимся, что листы существуют
        _ = _users_ws()
        _ = _history_ws()
        _ = _metrics_ws()

        LAST_SHEETS_ERROR = None
        logging.info("Sheets OK: spreadsheet=%s", SHEETS_SPREADSHEET_ID)

    except Exception as e:
        LAST_SHEETS_ERROR = f"{type(e).__name__}: {e}"
        logging.exception("Sheets init failed")
        _sheets_client = None

async def _sheets_append_history_async(user_id: int, role: str, content: str):
    """Неблокирующая запись сообщения в лист History."""
    try:
        def _do():
            ws = _history_ws()
            if not ws:
                return
            ws.append_row([datetime.utcnow().isoformat(), str(user_id), role, content], value_input_option="RAW")
        await asyncio.to_thread(_do)
    except Exception:
        logging.exception("append_history: sheets write failed")

async def _sheets_upsert_user_async(m: Message, plan: str):
    """Первичная фиксация пользователя в лист Users (или дубль-строка, это ок)."""
    try:
        def _do():
            ws = _users_ws()
            if not ws:
                return
            from_user = m.from_user
            ws.append_row([
                datetime.utcnow().isoformat(),
                str(from_user.id),
                (from_user.username or "")[:64],
                (from_user.first_name or "")[:64],
                (from_user.last_name or "")[:64],
                "uz" if is_uzbek(m.text or "") else "ru",
                plan,
                str(get_user(from_user.id).get("free_used", 0)),
            ], value_input_option="RAW")
        await asyncio.to_thread(_do)
    except Exception:
        logging.exception("upsert_user: sheets write failed")

async def _metrics_users_count_async():
    """Неблокирующая запись количества пользователей и активных подписок в лист Metrics."""
    try:
        def _do():
            ws = _metrics_ws()
            if not ws:
                return
            total_users = len(USERS)
            active_subs = sum(1 for u in USERS.values() if has_active_sub(u))
            ws.append_row([datetime.utcnow().isoformat(), total_users, active_subs], value_input_option="RAW")
        await asyncio.to_thread(_do)
    except Exception:
        logging.exception("metrics users count failed")

# ================== АНАЛИТИКА (file + Sheets) =================
def _log_to_file(row: dict):
    try:
        p = Path(ANALYTICS_DB_PATH); p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as e:
        logging.warning("log_event file failed: %s", e)

def log_event(user_id: int, name: str, **payload):
    row = {"ts": _ts(), "user_id": user_id, "event": name, **payload}
    _log_to_file(row)
    # В основной аналитический лист можно добавить при желании (оставил только файл)

def format_stats(days: int | None = 7):
    p = Path(ANALYTICS_DB_PATH)
    if not p.exists():
        return "Пока нет событий."
    cutoff = datetime.utcnow() - timedelta(days=days) if days else None
    evs = []
    for line in p.read_text("utf-8").splitlines():
        try:
            e = json.loads(line)
            if cutoff:
                try:
                    ts = datetime.fromisoformat((e.get("ts", "") or "").split("+")[0])
                    if ts < cutoff:
                        continue
                except Exception:
                    pass
            evs.append(e)
        except Exception:
            continue
    total = len(evs)
    users = len({e.get("user_id") for e in evs if "user_id" in e})
    qs = [e for e in evs if e.get("event") == "question"]
    topics = Counter((e.get("topic") or "—") for e in qs)
    grants = sum(1 for e in evs if e.get("event") == "subscription_granted")
    paid_clicks = sum(1 for e in evs if e.get("event") == "paid_done_click")
    active_now = sum(1 for u in USERS.values() if has_active_sub(u))
    lines = [
        f"📊 За {days} дн.",
        f"• Событий: {total} | Уник. пользователей: {users}",
        f"• Вопросов: {len(qs)}",
        f"• Топ тем: " + (", ".join(f"{k}:{v}" for k, v in topics.most_common(6)) if topics else "—"),
        f"• «Оплатил»: {paid_clicks} | Активаций: {grants}",
        f"• Активных сейчас: {active_now}",
    ]
    return "\n".join(lines)

# ================== ИИ =================
BASE_SYSTEM_PROMPT = (
    "Ты — SavolBot, дружелюбный и быстрый собеседник. Отвечай естественно, как человек: кратко и ясно "
    "(обычно до 6–8 предложений), без канцелярита, с примерами и списками, когда уместно. "
    "Можно лёгкий юмор, но по теме. Соблюдай законы Узбекистана. Не давай инструкций для незаконных действий. "
    "По медицине — только общая справка и рекомендация обратиться к врачу. Язык ответа = язык вопроса (RU/UZ). "
    "Никогда не вставляй ссылки и URL. Не говори фразы вроде «я не помню/не сохраняю историю». "
    "Если контекста мало — вежливо попроси напомнить ключевые детали и продолжай. "
    "Если вопрос про актуальные данные — используй сводку из поиска, но отвечай своими словами."
)

def build_messages(user_id: int, system: str, user_text: str) -> list[dict]:
    msgs = [{"role": "system", "content": system}]
    msgs.extend(get_recent_history(user_id))
    msgs.append({"role": "user", "content": user_text})
    return msgs

async def ask_gpt(user_text: str, topic_hint: str | None, user_id: int) -> str:
    if not OPENAI_API_KEY:
        return f"Вы спросили: {user_text}"
    system = BASE_SYSTEM_PROMPT + (f" Учитывай контекст темы: {topic_hint}" if topic_hint else "")
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {"model": OPENAI_MODEL, "temperature": 0.6, "messages": build_messages(user_id, system, user_text)}
    r = await client_openai.post("/chat/completions", headers=headers, json=payload)
    r.raise_for_status()
    raw = r.json()["choices"][0]["message"]["content"].strip()
    return strip_links(raw)

# ================== LIVE SEARCH =================
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

def _norm_query(q: str) -> str:
    return re.sub(r"\s+", " ", q.strip().lower())

def cache_get(q: str):
    k = _norm_query(q); it = LIVE_CACHE.get(k)
    if not it:
        return None
    if time.time() - it["ts"] > CACHE_TTL_SECONDS:
        LIVE_CACHE.pop(k, None); return None
    return it["answer"]

def cache_set(q: str, a: str):
    if len(LIVE_CACHE) >= CACHE_MAX_ENTRIES:
        oldest = min(LIVE_CACHE, key=lambda x: LIVE_CACHE[x]["ts"])
        LIVE_CACHE.pop(oldest, None)
    LIVE_CACHE[_norm_query(q)] = {"ts": time.time(), "answer": a}

async def web_search_tavily(query: str, max_results: int = 3) -> dict | None:
    if not TAVILY_API_KEY:
        return None
    depth = "advanced" if is_time_sensitive(query) else "basic"
    payload = {"api_key": TAVILY_API_KEY, "query": query, "search_depth": depth, "max_results": max_results,
               "include_answer": True, "include_domains": []}
    r = await client_http.post("https://api.tavily.com/search", json=payload)
    r.raise_for_status()
    return r.json()

async def answer_with_live_search(user_text: str, topic_hint: str | None, user_id: int) -> str:
    c = cache_get(user_text)
    if c:
        return c
    data = await web_search_tavily(user_text)
    if not data:
        return await ask_gpt(user_text, topic_hint, user_id)
    snippets = []
    for it in (data.get("results") or [])[:3]:
        title = (it.get("title") or "")[:80]
        content = (it.get("content") or "")[:350]
        snippets.append(f"- {title}\n{content}")
    system = BASE_SYSTEM_PROMPT + " Отвечай, опираясь на источники (но без ссылок). Кратко, по делу."
    if topic_hint:
        system += f" Учитывай контекст темы: {topic_hint}"
    user_aug = f"{user_text}\n\nИСТОЧНИКИ (сводка без URL):\n" + "\n\n".join(snippets)
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {"model": OPENAI_MODEL, "temperature": 0.3, "messages": build_messages(user_id, system, user_aug)}
    r = await client_openai.post("/chat/completions", headers=headers, json=payload)
    r.raise_for_status()
    answer = r.json()["choices"][0]["message"]["content"].strip()
    final = strip_links(answer)
    cache_set(user_text, final)
    return final

# ================== КОМАНДЫ =================
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
    for key, tdef in TOPICS.items():
        label = tdef["title_uz"] if lang == "uz" else tdef["title_ru"]
        if current == key:
            label = f"✅ {label}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"topic:{key}")])
    rows.append([InlineKeyboardButton(text="↩️ Закрыть / Yopish", callback_data="topic:close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def tariffs_text(lang='ru'):
    return t(lang, "tariff_title")

@dp.message(Command("start"))
async def cmd_start(message: Message):
    u = get_user(message.from_user.id)
    u["lang"] = "uz" if is_uzbek(message.text or "") else "ru"; save_users()
    log_event(message.from_user.id, "start", lang=u["lang"])
    # зафиксируем пользователя в новом листе Users
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_sheets_upsert_user_async(message, u["plan"]))
    except RuntimeError:
        pass
    await message.answer(t(u["lang"], "welcome"))

@dp.message(Command("help"))
async def cmd_help(message: Message):
    lang = "uz" if is_uzbek(message.text or "") else "ru"
    log_event(message.from_user.id, "help")
    await message.answer(t(lang, "help"))

@dp.message(Command("about"))
async def cmd_about(message: Message):
    lang = "uz" if is_uzbek(message.text or "") else "ru"
    log_event(message.from_user.id, "about")
    await message.answer(t(lang, "welcome"))

@dp.message(Command("tariffs"))
async def cmd_tariffs(message: Message):
    lang = get_user(message.from_user.id).get("lang", "ru")
    log_event(message.from_user.id, "view_tariffs")
    await message.answer(tariffs_text(lang), reply_markup=pay_kb())

@dp.message(Command("myplan"))
async def cmd_myplan(message: Message):
    u = get_user(message.from_user.id)
    status = "активна" if has_active_sub(u) else "нет"
    until = u["paid_until"].isoformat() if u["paid_until"] else "—"
    topic = u.get("topic") or "—"
    is_wl = is_whitelisted(message.from_user.id)
    plan_label = "whitelist (безлимит)" if is_wl else u["plan"]
    free_info = "безлимит" if is_wl else f"{u['free_used']}/{FREE_LIMIT}"
    log_event(message.from_user.id, "myplan_open", whitelisted=is_wl)
    await message.answer(
        f"{t(u['lang'],'plan_title')}: {plan_label}\n"
        f"Подписка: {status} (до {until})\n"
        f"Тема: {topic}\n"
        f"Бесплатно: {free_info}"
    )

@dp.message(Command("topics"))
async def cmd_topics(message: Message):
    u = get_user(message.from_user.id); lang = u["lang"]
    log_event(message.from_user.id, "topics_open")
    await message.answer(t(lang, "choose_topic"), reply_markup=topic_kb(lang, current=u.get("topic")))

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if ADMIN_CHAT_ID and str(message.from_user.id) != str(ADMIN_CHAT_ID):
        return await message.answer(t(get_user(message.from_user.id)["lang"], "admin_only"))
    parts = message.text.strip().split()
    days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 7
    await message.answer(format_stats(days))

@dp.message(Command("new"))
async def cmd_new(message: Message):
    reset_history(message.from_user.id)
    await message.answer(t(get_user(message.from_user.id)["lang"], "context_cleared"))

# -------- Sheets диагностика ----------
@dp.message(Command("gs_debug"))
async def cmd_gs_debug(message: Message):
    has_env = all([GOOGLE_CREDENTIALS, SHEETS_SPREADSHEET_ID])
    await message.answer(
        "ENV OK: {env}\nID: {sid}\nCred len: {cl}\nErr: {err}".format(
            env=has_env,
            sid=SHEETS_SPREADSHEET_ID or "—",
            cl=len(GOOGLE_CREDENTIALS or ""),
            err=LAST_SHEETS_ERROR or "—",
        )
    )

@dp.message(Command("gs_reinit"))
async def cmd_gs_reinit(message: Message):
    _init_sheets()
    await message.answer("Reinit → " + ("✅ OK" if _sheets_client else f"❌ Fail: {LAST_SHEETS_ERROR}"))

@dp.message(Command("gs_list"))
async def cmd_gs_list(message: Message):
    try:
        if not _sheets_client:
            return await message.answer("❌ Sheets client не инициализирован. /gs_reinit")
        sh = _sheets_client.open_by_key(SHEETS_SPREADSHEET_ID)
        titles = [ws.title for ws in sh.worksheets()]
        await message.answer("Листы:\n" + "\n".join("• " + t for t in titles))
    except Exception as e:
        logging.exception("gs_list failed")
        await message.answer(f"❌ gs_list ошибка: {e}")

# ================== CALLBACKS =================
@dp.callback_query(F.data == "show_tariffs")
async def cb_show_tariffs(call: CallbackQuery):
    u = get_user(call.from_user.id)
    log_event(call.from_user.id, "view_tariffs_click")
    await call.message.edit_text(tariffs_text(u["lang"]), reply_markup=pay_kb())
    await call.answer()

@dp.callback_query(F.data == "subscribe_creative")
async def cb_subscribe_creative(call: CallbackQuery):
    log_event(call.from_user.id, "subscribe_creative_open")
    pay_link = "https://pay.example.com/savolbot/creative"  # TODO: заменить после открытия ИП
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Готово (я оплатил)", callback_data="paid_creative_done")]
    ])
    price = TARIFFS["creative"]["price_usd"]
    await call.message.answer(f"💳 «Creative» — {price} $/мес.\nОплата: {pay_link}", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "paid_creative_done")
async def cb_paid_done(call: CallbackQuery):
    log_event(call.from_user.id, "paid_done_click")
    if ADMIN_CHAT_ID and bot:
        try:
            await bot.send_message(
                int(ADMIN_CHAT_ID),
                f"👤 @{call.from_user.username or call.from_user.id} запросил активацию «Creative».\n"
                f"TG ID: {call.from_user.id}\nИспользуйте /grant_creative {call.from_user.id}"
            )
        except Exception:
            logging.exception("Notify admin failed")
    await call.message.answer(t(get_user(call.from_user.id)["lang"], "thanks_paid"))
    await call.answer()

@dp.message(Command("grant_creative"))
async def cmd_grant_creative(message: Message):
    if str(message.from_user.id) != str(ADMIN_CHAT_ID):
        return await message.answer(t(get_user(message.from_user.id)["lang"], "admin_only"))
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Использование: /grant_creative <tg_id>")
    target_id = int(parts[1]); u = get_user(target_id)
    u["plan"] = "creative"
    u["paid_until"] = datetime.utcnow() + timedelta(days=TARIFFS["creative"]["duration_days"]); save_users()
    log_event(message.from_user.id, "subscription_granted", target=target_id, plan="creative", paid_until=u["paid_until"].isoformat())
    await message.answer(f"✅ Активирован «Creative» для {target_id} до {u['paid_until'].isoformat()}")
    try:
        if bot:
            await bot.send_message(target_id, t(get_user(target_id)["lang"], "sub_activated_user"))
    except Exception:
        logging.warning("Notify user failed")

# ================== ОБРАБОТЧИК ВОПРОСОВ =================
@dp.message(F.text)
async def handle_text(message: Message):
    text = (message.text or "").strip()
    uid = message.from_user.id
    u = get_user(uid)

    # язык по входящему
    if is_uzbek(text):
        u["lang"] = "uz"; save_users()
    else:
        u["lang"] = "ru"; save_users()

    # промежуточный индикатор «думаю/собираю информацию» на языке пользователя
    thinking_msg = await message.answer(t(u["lang"], "thinking"))

    if any(re.search(rx, text.lower()) for rx in [
        r"\b(помни(шь|те)|вспомни(шь|те))\b",
        r"\b(что|о\s*чём|о\s*чем)\b.*\b(спрашивал|говорили|обсуждали)\b",
        r"\b(вчера|сегодня)\b.*\b(диалог|разговор|чат)\b",
    ]):
        # Короткое тематическое резюме (упрощённо)
        recap = "Ha, eslayman." if u["lang"] == "uz" else "Да, помню."
        try:
            await thinking_msg.edit_text(t(u["lang"], "ready"))
        except Exception:
            pass
        await message.answer(recap)
        append_history(uid, "user", text)
        append_history(uid, "assistant", recap)
        return

    # модерация/пейволл
    if any(re.search(rx, text.lower()) for rx in ILLEGAL_PATTERNS):
        try:
            await thinking_msg.delete()
        except Exception:
            pass
        log_event(uid, "question_blocked", reason="policy")
        return await message.answer(DENY_TEXT_UZ if u["lang"] == "uz" else DENY_TEXT_RU)

    if (not is_whitelisted(uid)) and (not has_active_sub(u)) and u["free_used"] >= FREE_LIMIT:
        try:
            await thinking_msg.delete()
        except Exception:
            pass
        log_event(uid, "paywall_shown")
        return await message.answer(t(u["lang"], "paywall"), reply_markup=pay_kb())

    topic_hint = TOPICS.get(u.get("topic"), {}).get("hint")
    use_live = True  # всегда разрешаем live-поиск

    try:
        # сменим «думаю…» на «готово…» перед отправкой ответа
        try:
            await thinking_msg.edit_text(t(u["lang"], "ready"))
        except Exception:
            pass

        reply = await (answer_with_live_search(text, topic_hint, uid) if use_live else ask_gpt(text, topic_hint, uid))
        await message.answer(reply)

        append_history(uid, "user", text)
        append_history(uid, "assistant", reply)

        log_event(uid, "question", topic=u.get("topic"), live=use_live, time_sensitive=is_time_sensitive(text),
                  whitelisted=is_whitelisted(uid))
    except Exception:
        logging.exception("OpenAI error")
        try:
            await thinking_msg.delete()
        except Exception:
            pass
        return await message.answer("Извини, сервер перегружен. Попробуйте позже.")

    if (not is_whitelisted(uid)) and (not has_active_sub(u)):
        u["free_used"] += 1; save_users()

# ================== Lifespan =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_users()
    load_history()
    _init_sheets()

    HTTP2_ENABLED = os.getenv("HTTP2_ENABLED", "0") == "1"
    try:
        import h2  # noqa: F401
        _h2_ok = True
    except Exception:
        _h2_ok = False
    use_http2 = HTTP2_ENABLED and _h2_ok

    global client_openai, client_http
    client_openai = httpx.AsyncClient(base_url=OPENAI_API_BASE, timeout=HTTPX_TIMEOUT, http2=use_http2)
    client_http = httpx.AsyncClient(timeout=HTTPX_TIMEOUT, http2=use_http2)

    # Ставим вебхук
    if TELEGRAM_TOKEN and WEBHOOK_URL and client_http:
        try:
            resp = await client_http.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
                json={
                    "url": WEBHOOK_URL,
                    "secret_token": WEBHOOK_SECRET,
                    "drop_pending_updates": True,
                    "max_connections": 80,
                    "allowed_updates": ["message", "callback_query"],
                },
            )
            logging.info("setWebhook: %s %s", resp.status_code, resp.text)
        except Exception:
            logging.exception("Failed to set webhook")

    try:
        yield
    finally:
        try:
            if client_openai:
                await client_openai.aclose()
        except Exception:
            pass
        try:
            if client_http:
                await client_http.aclose()
        except Exception:
            pass

app = FastAPI(lifespan=lifespan)

# ================== WEBHOOK =================
@app.post("/webhook")
async def telegram_webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        return {"ok": False, "error": "bad secret"}
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "ok"}
