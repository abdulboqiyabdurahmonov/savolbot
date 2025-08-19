import os
import re
import json
import time
import logging
import asyncio
import random
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional
from gspread.utils import rowcol_to_a1

import httpx
from httpx import HTTPError, HTTPStatusError
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

# Live-поиск (через Tavily) — опционально
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

# Принудительно использовать live-поиск для всех вопросов (если 1)
FORCE_LIVE = os.getenv("FORCE_LIVE", "0") == "1"

# Верификация динамичных ответов (цифры/годы/ставки)
VERIFY_DYNAMIC = os.getenv("VERIFY_DYNAMIC", "1") == "1"
VERIFY_TIMEOUT_SEC = int(os.getenv("VERIFY_TIMEOUT_SEC", "10"))

# Админ
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")  # str

# Персистентные файлы
USERS_DB_PATH = os.getenv("USERS_DB_PATH", "users_limits.json")
HISTORY_DB_PATH = os.getenv("HISTORY_DB_PATH", "chat_history.json")

# --- Google Sheets ENV ---
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")  # JSON одной строкой (или base64)
SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")

# Названия листов (можно переопределить через ENV)
USERS_SHEET = os.getenv("USERS_SHEET", "Users")        # лист-реестр пользователей
HISTORY_SHEET = os.getenv("HISTORY_SHEET", "History")  # лог диалога
METRICS_SHEET = os.getenv("METRICS_SHEET", "Metrics")  # события/метрики
FEEDBACK_SHEET = os.getenv("FEEDBACK_SHEET", "Feedback")  # обратная связь

# --- HTTPX clients & timeouts (reuse) ---
HTTPX_TIMEOUT = httpx.Timeout(connect=5.0, read=60.0, write=30.0, pool=30.0)
client_openai: Optional[httpx.AsyncClient] = None
client_http: Optional[httpx.AsyncClient] = None

# Параллелизм запросов к модели (чтобы не ловить 429)
MODEL_CONCURRENCY = int(os.getenv("MODEL_CONCURRENCY", "4"))
_model_sem = asyncio.Semaphore(MODEL_CONCURRENCY)

# Общий таймаут на формирование ответа пользователю (секунды)
REPLY_TIMEOUT_SEC = int(os.getenv("REPLY_TIMEOUT_SEC", "12"))

# Белый список (VIP) — пользователи без ограничений
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
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "7"))

def _serialize_user(u: dict) -> dict:
    return {
        "plan": u.get("plan", "trial"),
        "paid_until": u["paid_until"].isoformat() if u.get("paid_until") else None,
        "lang": u.get("lang", "ru"),
        "topic": u.get("topic"),
        "registered_to_sheets": bool(u.get("registered_to_sheets", False)),
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
                "plan": v.get("plan", "trial"),
                "paid_until": paid_until,
                "lang": v.get("lang", "ru"),
                "topic": v.get("topic"),
                "registered_to_sheets": bool(v.get("registered_to_sheets", False)),
            }
    except Exception:
        logging.exception("load_users failed")
        USERS = {}

def has_active_sub(u: dict) -> bool:
    if u.get("plan") == "creative" and u.get("paid_until") and u["paid_until"] > datetime.utcnow():
        return True
    if u.get("plan", "trial") == "trial" and u.get("paid_until") and u["paid_until"] > datetime.utcnow():
        return True
    return False

def get_user(tg_id: int):
    is_new = tg_id not in USERS
    if is_new:
        USERS[tg_id] = {
            "plan": "trial",
            "paid_until": datetime.utcnow() + timedelta(days=TRIAL_DAYS),
            "lang": "ru",
            "topic": None,
            "registered_to_sheets": False,
        }
        save_users()
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_register_user_async(tg_id))
        except RuntimeError:
            pass
    return USERS[tg_id]

def pay_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить Creative ($10/мес)", callback_data="subscribe_creative")],
        [InlineKeyboardButton(text="ℹ️ О тарифе", callback_data="show_tariffs")]
    ])

# ================== ИСТОРИЯ ДИАЛОГА (локальная) =================
HISTORY: dict[int, list[dict]] = {}  # {user_id: [ {role:"user"/"assistant", content:str, ts:str}, ... ]}

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

# ================== МОДЕРАЦИЯ =================
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
    return bool(re.search(r"[ғқҳў]", t) or re.search(r"\b(ha|yo[’']q|iltimos|rahmat|salom)\b", t))

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

def strip_links_and_cleanup(text: str) -> str:
    return strip_links(text or "")

# ================== ТАРИФ =================
TARIFF = {
    "creative": {
        "title": "Creative",
        "price_usd": 10,
        "desc_ru": [
            "Генерация картинок и помощь с документами",
            "Неограниченное число сообщений",
            "Удобно прямо в Telegram",
        ],
        "desc_uz": [
            "Rasm generatsiyasi va hujjatlar bo‘yicha yordam",
            "Cheklanmagan xabarlar soni",
            "Telegram ichida qulay",
        ],
        "duration_days": 30,
        "active": True,
    }
}

def tariffs_text(lang='ru'):
    t = TARIFF["creative"]
    badge = "(доступен)" if t["active"] else "(скоро)"
    if lang == "uz":
        lines = "\n".join(f"• {x}" for x in t["desc_uz"])
        return (
            f"⭐ {t['title']} {badge}\n"
            f"NARX: ${t['price_usd']}/oy\n"
            f"{lines}\n\n"
            f"7 kunlik BEPUL sinov → keyin ${t['price_usd']}/oy"
        )
    else:
        lines = "\n".join(f"• {x}" for x in t["desc_ru"])
        return (
            f"⭐ {t['title']} {badge}\n"
            f"Цена: ${t['price_usd']}/мес\n"
            f"{lines}\n\n"
            f"7 дней БЕСПЛАТНО → далее ${t['price_usd']}/мес"
        )

# ================== SHEETS (Users + History + Metrics + Feedback) =================
_sheets_client: Optional[gspread.Client] = None
_users_ws: Optional[gspread.Worksheet] = None
LAST_SHEETS_ERROR: Optional[str] = None

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

def _users_ws_get():
    sh = _open_spreadsheet()
    if not sh:
        return None
    try:
        return sh.worksheet(USERS_SHEET)
    except gspread.WorksheetNotFound:
        try:
            ws = sh.add_worksheet(title=USERS_SHEET, rows=100000, cols=8)
            ws.append_row(["ts", "user_id", "username", "first_name", "last_name", "lang", "plan", "paid_until"], value_input_option="RAW")
            return ws
        except Exception:
            logging.exception("Create Users ws failed")
            return None
    except Exception:
        logging.exception("_users_ws failed")
        return None

# --- generic getter для листов с безопасным ensure header (без уменьшения строк) ---
def _ws_get(tab_name: str, headers: list[str]):
    sh = _open_spreadsheet()
    if not sh:
        return None
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        try:
            ws = sh.add_worksheet(title=tab_name, rows=200000, cols=max(len(headers), 6))
            end_a1 = rowcol_to_a1(1, len(headers))
            ws.update(f"A1:{end_a1}", [headers], value_input_option="RAW")
            return ws
        except Exception:
            logging.exception("Create ws '%s' failed", tab_name)
            return None
    except Exception:
        logging.exception("_ws_get(%s) failed", tab_name)
        return None

    try:
        need_cols = max(len(headers), 6)
        if getattr(ws, "col_count", 0) < need_cols:
            ws.resize(cols=need_cols)  # не трогаем rows
        end_a1 = rowcol_to_a1(1, len(headers))
        ws.update(f"A1:{end_a1}", [headers], value_input_option="RAW")
    except Exception:
        logging.exception("ensure header for %s failed", tab_name)

    return ws

def _init_sheets():
    """
    Минимальная инициализация Sheets:
    - поддержка raw JSON и base64
    - нормализация private_key с \\n -> \n
    - Users/History/Metrics/Feedback — создание и заголовки при отсутствии
    """
    global _sheets_client, _users_ws, LAST_SHEETS_ERROR
    if not (GOOGLE_CREDENTIALS and SHEETS_SPREADSHEET_ID and USERS_SHEET):
        LAST_SHEETS_ERROR = "Sheets env not set: GOOGLE_CREDENTIALS / SHEETS_SPREADSHEET_ID / USERS_SHEET"
        logging.warning(LAST_SHEETS_ERROR)
        return
    try:
        raw = GOOGLE_CREDENTIALS.strip()
        try:
            import base64
            creds_text = base64.b64decode(raw).decode("utf-8") if not raw.lstrip().startswith("{") else raw
        except Exception:
            creds_text = raw

        creds_info = json.loads(creds_text)
        if isinstance(creds_info, dict) and creds_info.get("private_key"):
            pk = creds_info["private_key"]
            if "BEGIN PRIVATE KEY" in pk and "\\n" in pk:
                creds_info["private_key"] = pk.replace("\\n", "\n")

        for field in ("client_email", "private_key"):
            if not creds_info.get(field):
                raise ValueError(f"GOOGLE_CREDENTIALS is missing '{field}'")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
        _sheets_client = gspread.authorize(creds)

        sh = _sheets_client.open_by_key(SHEETS_SPREADSHEET_ID)
        # Users
        try:
            _users_ws = sh.worksheet(USERS_SHEET)
        except gspread.WorksheetNotFound:
            logging.warning("Worksheet '%s' not found, creating…", USERS_SHEET)
            _users_ws = sh.add_worksheet(title=USERS_SHEET, rows=100000, cols=8)
            _users_ws.append_row(["ts", "user_id", "username", "first_name", "last_name", "lang", "plan", "paid_until"], value_input_option="RAW")
        # Ensure другие листы
        _ = _ws_get(HISTORY_SHEET, ["ts","user_id","role","content","col1","col2"])
        _ = _ws_get(METRICS_SHEET, ["ts","user_id","event","value","notes"])
        _ = _ws_get(FEEDBACK_SHEET, ["ts","user_id","username","first_name","last_name","feedback","comment"])

        LAST_SHEETS_ERROR = None
        logging.info("Sheets OK: spreadsheet=%s users_sheet=%s", SHEETS_SPREADSHEET_ID, USERS_SHEET)

    except Exception as e:
        LAST_SHEETS_ERROR = f"{type(e).__name__}: {e}"
        logging.exception("Sheets init failed")
        _sheets_client = _users_ws = None

async def _sheets_register_user_async(user_id: int):
    """Разовая запись пользователя в лист Users (если ещё не записан)."""
    u = USERS.get(user_id)
    if not u or not _users_ws:
        return
    if u.get("registered_to_sheets"):
        return
    try:
        def _do():
            ws = _users_ws_get()
            if not ws:
                return
            paid = u['paid_until'].isoformat() if u.get('paid_until') else ""
            ws.append_row(
                [datetime.utcnow().isoformat(), str(user_id), "", "", "", u.get('lang', 'ru'), u.get('plan', 'trial'), paid],
                value_input_option="RAW"
            )
        await asyncio.to_thread(_do)
        u["registered_to_sheets"] = True
        save_users()
    except Exception:
        logging.exception("sheets_register_user failed")

async def _sheets_update_user_row_async(user_id: int, username: str, first_name: str, last_name: str, lang: str, plan: str, paid_until: Optional[datetime]):
    """Упрощённо: добавляем обновлённую строку (последняя версия состояния пользователя)."""
    if not _users_ws:
        return
    try:
        def _do():
            ws = _users_ws_get()
            if not ws:
                return
            paid = paid_until.isoformat() if paid_until else ""
            ws.append_row(
                [datetime.utcnow().isoformat(), str(user_id), username or "", first_name or "", last_name or "", lang or "ru", plan or "", paid],
                value_input_option="RAW"
            )
        await asyncio.to_thread(_do)
    except Exception:
        logging.exception("sheets_update_user_row failed")

# --- append в History/Metrics/Feedback ---
async def _sheets_append_history_async(user_id: int, role: str, content: str, col1: str = "", col2: str = ""):
    if not _sheets_client:
        return
    try:
        def _do():
            ws = _ws_get(HISTORY_SHEET, ["ts","user_id","role","content","col1","col2"])
            if not ws:
                return
            ws.append_row([_ts(), str(user_id), role, content, col1, col2], value_input_option="RAW")
        await asyncio.to_thread(_do)
    except Exception:
        logging.exception("sheets_append_history failed")

async def _sheets_append_feedback_async(
    user_id: int, username: str, first_name: str, last_name: str, feedback: str, comment: str = ""
):
    if not _sheets_client:
        return
    try:
        def _do():
            ws = _ws_get(FEEDBACK_SHEET, ["ts","user_id","username","first_name","last_name","feedback","comment"])
            if not ws:
                return
            ws.append_row([
                _ts(),
                str(user_id),
                username or "",
                first_name or "",
                last_name or "",
                feedback,
                comment or ""
            ], value_input_option="RAW")
        await asyncio.to_thread(_do)
    except Exception:
        logging.exception("sheets_append_feedback failed")

async def _sheets_append_metric_async(user_id: int, event: str, value: str = "", notes: str = ""):
    if not _sheets_client:
        return
    try:
        def _do():
            ws = _ws_get(METRICS_SHEET, ["ts","user_id","event","value","notes"])
            if not ws:
                return
            ws.append_row([_ts(), str(user_id), event, value, notes], value_input_option="RAW")
        await asyncio.to_thread(_do)
    except Exception:
        logging.exception("sheets_append_metric failed")

# ================== БЕЗОТКАЗНОСТЬ: retry + дружелюбные ошибки =================
async def _retry(coro_factory, attempts=3, base_delay=0.8):
    last_exc = None
    for i in range(attempts):
        try:
            return await coro_factory()
        except HTTPStatusError as e:
            if e.response.status_code in (429, 500, 502, 503, 504):
                last_exc = e
            else:
                raise
        except (HTTPError, asyncio.TimeoutError) as e:
            last_exc = e
        await asyncio.sleep(base_delay * (2 ** i) + random.random() * 0.2)
    if last_exc:
        raise last_exc

def _friendly_error_text(e, lang="ru"):
    ru = {
        "timeout": "⌛ Источник долго отвечает. Попробуйте повторить запрос чуть позже.",
        "429": "⏳ Высокая нагрузка на модель. Повторите запрос через минуту.",
        "401": "🔑 Проблема с ключом OpenAI. Сообщите поддержке.",
        "402": "💳 Исчерпан лимит оплаты OpenAI. Сообщите поддержке.",
        "5xx": "☁️ Поставщик временно недоступен. Повторите запрос позже.",
        "generic": "Извини, не получилось получить ответ. Попробуй ещё раз.",
    }
    uz = {
        "timeout": "⌛ Manba javob bermayapti. Birozdan so‘ng qayta urinib ko‘ring.",
        "429": "⏳ Modelga yuklama yuqori. Bir daqiqadan so‘ng urinib ko‘ring.",
        "401": "🔑 OpenAI kaliti muammosi. Texnik yordamga yozing.",
        "402": "💳 OpenAI to‘lovi limiti tugagan. Texnik yordamga yozing.",
        "5xx": "☁️ Xizmat vaqtincha ishlamayapti. Keyinroq urinib ko‘ring.",
        "generic": "Kechirasiz, hozir javob bera olmadim. Yana urinib ko‘ring.",
    }
    M = uz if lang == "uz" else ru
    if isinstance(e, HTTPStatusError):
        code = e.response.status_code
        if code == 429: return M["429"]
        if code == 401: return M["401"]
        if code == 402: return M["402"]
        if 500 <= code <= 599: return M["5xx"]
    if isinstance(e, (HTTPError, asyncio.TimeoutError)):
        return M["timeout"]
    return M["generic"]

# ================== ИИ =================
BASE_SYSTEM_PROMPT = (
    "Ты — SavolBot (часть TripleA). Отвечай естественно и по делу: 6–8 предложений, без канцелярита, "
    "с примерами и списками по месту. Лёгкий юмор допустим. Соблюдай законы Узбекистана. "
    "Не давай инструкций для незаконных действий. По медицине — только общая справка и совет обратиться к врачу. "
    "Язык ответа = язык вопроса (RU/UZ). Никогда не вставляй ссылки и URL. "
    "Если контекста мало — вежливо попроси напомнить ключевые детали и продолжай. "
    "Если вопрос про актуальные данные — используй сводку из поиска, но отвечай своими словами. Никогда не упоминай дату отсечки знаний; не пиши, что данные «актуальны до ...». Если чего-то не знаешь — проверь через поиск."
)

# (повторный импорт asyncio/httpx в исходнике был — не трогаю)
import asyncio as _asyncio_shadow  # не используется, оставлено для совместимости
import httpx as _httpx_shadow     # не используется, оставлено для совместимости

# Семафор для ограничения одновременных запросов к модели
if not isinstance(_model_sem, asyncio.Semaphore):
    _model_sem = asyncio.Semaphore(MODEL_CONCURRENCY)

async def ask_gpt(user_text: str, topic_hint: Optional[str], user_id: int) -> str:
    if not OPENAI_API_KEY:
        return f"Вы спросили: {user_text}"

    system = BASE_SYSTEM_PROMPT + (f" Учитывай контекст темы: {topic_hint}" if topic_hint else "")
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0.6,
        "messages": build_messages(user_id, system, user_text),
    }

    async def _do():
        return await client_openai.post("/chat/completions", headers=headers, json=payload)

    try:
        async with _model_sem:
            r = await _retry(lambda: _do(), attempts=3)
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"].strip()
        # Если модель вдруг вернула дисклеймер про старые знания — принудительно уйдём в live-поиск
        if _has_cutoff_disclaimer(raw) and TAVILY_API_KEY:
            try:
                return await answer_with_live_search(user_text, topic_hint, user_id)
            except Exception:
                pass
        return strip_links_and_cleanup(_sanitize_cutoff(raw))
    except Exception as e:
        logging.exception("ask_gpt failed")
        u = USERS.get(user_id, {"lang": "ru"})
        return _friendly_error_text(e, u.get("lang", "ru"))

TIME_SENSITIVE_PATTERNS = [
    r"\b(сегодня|сейчас|на данный момент|актуальн|в \d{4} году|в 20\d{2})\b",
    r"\b(курс|зарплат|инфляц|ставк|цена|новост|статистик|прогноз)\b",
    r"\b(bugun|hozir|narx|kurs|yangilik)\b",
    r"\b(кто|как зовут|председател|директор|ceo|руководител)\b",
]

def is_time_sensitive(q: str) -> bool:
    return any(re.search(rx, q.lower()) for rx in TIME_SENSITIVE_PATTERNS)

# === Отсекаем устаревшие дисклеймеры про "знания до 2023" ===
_CUTOFF_PATTERNS = [
    r"актуал\w+\s+до\s+\w+\s+20\d{2}",
    r"знан[^\.!\n]*до\s+\w+\s+20\d{2}",
    r"\bknowledge\s+cutoff\b",
    r"\bas of\s+\w+\s+20\d{2}",
]
def _has_cutoff_disclaimer(text: str) -> bool:
    low = (text or "").lower()
    import re as _re
    return any(_re.search(rx, low) for rx in _CUTOFF_PATTERNS)

def _sanitize_cutoff(text: str) -> str:
    import re as _re
    s = text or ""
    for rx in _CUTOFF_PATTERNS:
        s = _re.sub(rx, "", s, flags=_re.IGNORECASE)
    s = _re.sub(r"\n{3,}", "\n\n", s).strip()
    return s



# === Эвристики «динамичности» и верификация ответа ===
_DYNAMIC_KEYWORDS = [
    "курс", "ставк", "инфляц", "зарплат", "налог", "цена", "тариф", "пособи", "пенси", "кредит",
    "новост", "прогноз", "изменени", "обновлени", "statistika", "narx", "stavka", "yangilik", "price", "rate",
]
import re as _re2
def _contains_fresh_year(s: str, window: int = 3) -> bool:
    try:
        y_now = datetime.utcnow().year
    except Exception:
        y_now = 2025
    years = [int(y) for y in _re2.findall(r"\\b(20\\d{2})\\b", s or "")]
    return any(y_now - y <= window for y in years)

def _looks_dynamic(*texts: str) -> bool:
    low = " ".join([t.lower() for t in texts if t])
    return any(k in low for k in _DYNAMIC_KEYWORDS) or _contains_fresh_year(low)

async def verify_with_live_sources(user_text: str, draft_answer: str, topic_hint: Optional[str], user_id: int) -> str:
    \"\"\"Перепроверяет черновик по live-источникам и при необходимости корректирует цифры/даты.\"\"\"
    if not TAVILY_API_KEY:
        return draft_answer

    data = await web_search_tavily(user_text, max_results=5)
    if not data:
        return draft_answer

    snippets = []
    for it in (data.get("results") or [])[:5]:
        title = (it.get("title") or "")[:100]
        content = (it.get("content") or "")[:400]
        snippets.append(f"- {title}\\n{content}")

    system = (
        BASE_SYSTEM_PROMPT
        + " Проверь факты и при необходимости скорректируй числа/даты/ставки на основе источников. "
          "Если исправляешь — чётко перепиши фрагменты, не ссылайся на URL, не упоминай 'источники ниже'. "
          "Пиши компактно, без дисклеймеров об отсечке знаний."
    )
    if topic_hint:
        system += f" Учитывай контекст темы: {topic_hint}"

    user_aug = (\
        "ВОПРОС:\\n" + (user_text or "") + "\\n\\n"
        "ЧЕРНОВИК ОТВЕТА:\\n" + (draft_answer or "") + "\\n\\n"
        "СВОДКА ИСТОЧНИКОВ (без ссылок):\\n" + "\\n\\n".join(snippets)
    )

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {"model": OPENAI_MODEL, "temperature": 0.2, "messages": build_messages(user_id, system, user_aug)}

    async def _do():
        return await client_openai.post("/chat/completions", headers=headers, json=payload)

    try:
        async with _model_sem:
            r = await _retry(lambda: _do(), attempts=2)
        r.raise_for_status()
        corrected = r.json()["choices"][0]["message"]["content"].strip()
        corrected = strip_links_and_cleanup(_sanitize_cutoff(corrected))
        # Если модель вернула пусто — оставим черновик
        return corrected if corrected else draft_answer
    except Exception:
        return draft_answer

# === LIVE CACHE (было) ===
CACHE_TTL_SECONDS = int(os.getenv("LIVE_CACHE_TTL", "86400"))
CACHE_MAX_ENTRIES = int(os.getenv("LIVE_CACHE_MAX", "500"))
LIVE_CACHE: dict[str, dict] = {}

def _norm_query(q: str) -> str:
    return re.sub(r"\s+", " ", q.strip().lower())

def live_cache_get(q: str):
    k = _norm_query(q); it = LIVE_CACHE.get(k)
    if not it:
        return None
    if time.time() - it["ts"] > CACHE_TTL_SECONDS:
        LIVE_CACHE.pop(k, None); return None
    return it["answer"]

def live_cache_set(q: str, a: str):
    if len(LIVE_CACHE) >= CACHE_MAX_ENTRIES:
        oldest = min(LIVE_CACHE, key=lambda x: LIVE_CACHE[x]["ts"])
        LIVE_CACHE.pop(oldest, None)
    LIVE_CACHE[_norm_query(q)] = {"ts": time.time(), "answer": a}

async def web_search_tavily(query: str, max_results: int = 3) -> Optional[dict]:
    if not TAVILY_API_KEY:
        return None
    depth = "advanced" if is_time_sensitive(query) else "basic"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": depth,
        "max_results": max_results,
        "include_answer": True,
        "include_domains": [],
    }

    async def _do():
        return await client_http.post("https://api.tavily.com/search", json=payload)

    try:
        r = await _retry(lambda: _do(), attempts=2)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning("tavily search failed: %s", e)
        return None

async def answer_with_live_search(user_text: str, topic_hint: Optional[str], user_id: int) -> str:
    c = live_cache_get(user_text)
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

    async def _do():
        return await client_openai.post("/chat/completions", headers=headers, json=payload)

    try:
        async with _model_sem:
            r = await _retry(lambda: _do(), attempts=3)
        r.raise_for_status()
        answer = r.json()["choices"][0]["message"]["content"].strip()
        final = strip_links_and_cleanup(answer)
        live_cache_set(user_text, final)
        return final
    except Exception as e:
        logging.exception("live answer failed")
        u = USERS.get(user_id, {"lang": "ru"})
        return _friendly_error_text(e, u.get("lang", "ru"))

# ================== ВСПОМОГАТЕЛЬНОЕ: эффект «думаю…» (оставлено, но не используется в очереди) =================
async def send_thinking_progress(message: Message) -> Message:
    try:
        m = await message.answer("⏳ Думаю…")
        await asyncio.sleep(0.4)
        await m.edit_text("🔎 Собираю информацию…")
        return m
    except Exception:
        return await message.answer("🔎 Собираю информацию…")

# ================== FEEDBACK UI =================
def feedback_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👍 Ок", callback_data="fb:ok"),
            InlineKeyboardButton(text="👎 Не ок", callback_data="fb:bad"),
        ],
        [InlineKeyboardButton(text="✍️ Оставить комментарий", callback_data="fb:comment")],
        [InlineKeyboardButton(text="↩️ Закрыть", callback_data="fb:close")],
    ])

# флаг ожидания комментария: {user_id}
FEEDBACK_PENDING: set[int] = set()

# ================== КОМАНДЫ =================
WELCOME_RU = (
    "👋 Привет! Я — SavolBot, часть команды TripleA.\n"
    "Мы делаем автообзвоны, чат-боты и GPT в Telegram. "
    "Наш плюс: удобный доступ к ChatGPT прямо в Telegram — всего за $10/мес (вместо $20 у официальной подписки).\n\n"
    "Тариф: ⭐ Creative — генерирую картинки, помогаю с документами, без лимитов сообщений. "
    "Сейчас действует 7-дневный бесплатный период. Потом — $10/мес.\n\n"
    "Полезное: /tariffs — про тариф, /myplan — мой план, /topics — выбрать тему.\n"
    "Пиши вопрос — начнём!"
)
WELCOME_UZ = (
    "👋 Salom! Men — SavolBot, TripleA jamoasining qismi.\n"
    "Biz avtoqo‘ng‘iroqlar, chat-botlar va Telegramda GPT xizmatlarini qilamiz. "
    "Afzalligimiz: ChatGPT’ga Telegramning o‘zida qulay kirish — oyiga atigi $10 (rasmiy $20 o‘rniga).\n\n"
    "Tarif: ⭐ Creative — suratlar generatsiyasi, hujjatlar bo‘yicha yordam, cheklanmagan xabarlar. "
    "Hozir 7 kunlik bepul davr. Keyin — $10/oy.\n\n"
    "Foydali: /tariffs — tarif, /myplan — reja, /topics — mavzu tanlash.\n"
    "Savolingizni yozing — boshlaymiz!"
)

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

@dp.message(Command("start"))
async def cmd_start(message: Message):
    u = get_user(message.from_user.id)
    u["lang"] = "uz" if is_uzbek(message.text or "") else "ru"; save_users()
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_sheets_register_user_async(message.from_user.id))
        loop.create_task(_sheets_append_metric_async(message.from_user.id, "cmd", "start"))
        loop.create_task(_sheets_append_history_async(message.from_user.id, "user", "/start"))
    except RuntimeError:
        pass
    hello = WELCOME_UZ if u["lang"] == "uz" else WELCOME_RU
    await message.answer(hello)
    try:
        asyncio.get_running_loop().create_task(_sheets_append_history_async(message.from_user.id, "assistant", hello))
    except RuntimeError:
        pass

@dp.message(Command("help"))
async def cmd_help(message: Message):
    u = get_user(message.from_user.id)
    txt = "ℹ️ Напишите вопрос (RU/UZ). Я умею генерировать картинки и помогать с документами.\n/tariffs — тариф, /myplan — план, /topics — тема." \
        if u["lang"] == "ru" else \
        "ℹ️ Savolingizni yozing (RU/UZ). Surat generatsiyasi va hujjatlar bo‘yicha yordam.\n/tariffs — tarif, /myplan — reja, /topics — mavzu."
    await message.answer(txt)
    try:
        asyncio.get_running_loop().create_task(_sheets_append_history_async(message.from_user.id, "assistant", txt))
    except RuntimeError:
        pass

@dp.message(Command("about"))
async def cmd_about(message: Message):
    u = get_user(message.from_user.id)
    txt = (
        "🤖 SavolBot от TripleA: автообзвоны, чат-боты и GPT в Telegram. "
        "Creative — $10/мес, 7 дней бесплатно. /tariffs"
        if u["lang"] == "ru"
        else "🤖 SavolBot (TripleA): avtoqo‘ng‘iroqlar, chat-botlar, Telegramda GPT. "
             "Creative — $10/oy, 7 kun bepul. /tariffs"
    )
    await message.answer(txt)

@dp.message(Command("tariffs"))
async def cmd_tariffs(message: Message):
    u = get_user(message.from_user.id)
    await message.answer(tariffs_text(u["lang"]), reply_markup=pay_kb())

@dp.message(Command("myplan"))
async def cmd_myplan(message: Message):
    u = get_user(message.from_user.id)
    status = "активна" if has_active_sub(u) else "нет"
    until = u["paid_until"].isoformat() if u.get("paid_until") else "—"
    topic = u.get("topic") or "—"
    is_wl = is_whitelisted(message.from_user.id)
    plan_label = "whitelist (безлимит)" if is_wl else u.get("plan", "trial")
    await message.answer(
        f"Ваш план: {plan_label}\nПодписка активна: {status} (до {until})\nТема: {topic}"
        if u["lang"] == "ru" else
        f"Rejangiz: {plan_label}\nFaollik: {status} (gacha {until})\nMavzu: {topic}"
    )

@dp.message(Command("topics"))
async def cmd_topics(message: Message):
    u = get_user(message.from_user.id); lang = u["lang"]
    head = "🗂 Выберите тему:" if lang == "ru" else "🗂 Mavzuni tanlang:"
    await message.answer(head, reply_markup=topic_kb(lang, current=u.get("topic")))

@dp.message(Command("new"))
async def cmd_new(message: Message):
    reset_history(message.from_user.id)
    txt = "🧹 Контекст очищен. Начинаем новую тему." if get_user(message.from_user.id)["lang"] == "ru" else "🧹 Kontekst tozalandi. Yangi mavzu."
    await message.answer(txt)

# -------- Feedback командой ----------
@dp.message(Command("feedback"))
async def cmd_feedback(message: Message):
    u = get_user(message.from_user.id)
    txt = "Вам удобно пользоваться нашим ботом?" if u["lang"] == "ru" else "Bizning botdan foydalanish qulaymi?"
    await message.answer(txt, reply_markup=feedback_kb())
    try:
        asyncio.get_running_loop().create_task(_sheets_append_metric_async(message.from_user.id, "feedback_prompt", "manual"))
    except RuntimeError:
        pass

# -------- Sheets диагностика ----------
@dp.message(Command("gs_debug"))
async def cmd_gs_debug(message: Message):
    has_env = all([GOOGLE_CREDENTIALS, SHEETS_SPREADSHEET_ID, USERS_SHEET])
    await message.answer(
        "ENV OK: {env}\nID: {sid}\nUsers WS: {ws}\nCred len: {cl}\nUsers inited: {ok}\nErr: {err}".format(
            env=has_env,
            sid=SHEETS_SPREADSHEET_ID or "—",
            ws=USERS_SHEET or "—",
            cl=len(GOOGLE_CREDENTIALS or ""),
            ok=bool(_users_ws),
            err=LAST_SHEETS_ERROR or "—",
        )
    )

@dp.message(Command("gs_reinit"))
async def cmd_gs_reinit(message: Message):
    _init_sheets()
    await message.answer("Reinit → " + ("✅ OK" if _users_ws else f"❌ Fail: {LAST_SHEETS_ERROR}"))

@dp.message(Command("gs_users"))
async def cmd_gs_users(message: Message):
    try:
        if not _sheets_client:
            return await message.answer("❌ Sheets client не инициализирован. /gs_reinit")
        sh = _sheets_client.open_by_key(SHEETS_SPREADSHEET_ID)
        titles = [ws.title for ws in sh.worksheets()]
        await message.answer("Листы в таблице:\n" + "\n".join("• " + t for t in titles))
    except Exception as e:
        logging.exception("gs_users failed")
        await message.answer(f"❌ gs_users ошибка: {e}")

# ================== CALLBACKS (оплата) =================
@dp.callback_query(F.data == "show_tariffs")
async def cb_show_tariffs(call: CallbackQuery):
    u = get_user(call.from_user.id)
    await call.message.edit_text(tariffs_text(u["lang"]), reply_markup=pay_kb())
    await call.answer()

@dp.callback_query(F.data == "subscribe_creative")
async def cb_subscribe_creative(call: CallbackQuery):
    pay_link = "https://pay.example.com/savolbot/creative"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data="paid_creative_done")]
    ])
    txt = (
        f"💳 Тариф ⭐ Creative — ${TARIFF['creative']['price_usd']}/мес.\nОплата: {pay_link}\n"
        f"После оплаты нажмите кнопку ниже."
    )
    await call.message.answer(txt, reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "paid_creative_done")
async def cb_paid_done(call: CallbackQuery):
    if ADMIN_CHAT_ID and bot:
        try:
            await bot.send_message(
                int(ADMIN_CHAT_ID),
                f"👤 @{call.from_user.username or call.from_user.id} запросил активацию «Creative».\n"
                f"TG ID: {call.from_user.id}\nИспользуйте /grant_creative {call.from_user.id}"
            )
        except Exception:
            logging.exception("Notify admin failed")
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
        u["topic"] = key; save_users()
        lang = u["lang"]; title = TOPICS[key]["title_uz"] if lang == "uz" else TOPICS[key]["title_ru"]
        await call.message.edit_reply_markup(reply_markup=topic_kb(lang, current=key))
        await call.answer(f"Выбрана тема: {title}" if lang == "ru" else f"Mavzu tanlandi: {title}")

# ================== CALLBACKS (feedback) =================
@dp.callback_query(F.data == "fb:close")
async def cb_fb_close(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("Спасибо!")

@dp.callback_query(F.data == "fb:ok")
async def cb_fb_ok(call: CallbackQuery):
    uid = call.from_user.id
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_sheets_append_feedback_async(
            uid,
            call.from_user.username or "",
            call.from_user.first_name or "",
            call.from_user.last_name or "",
            "ok",
            ""
        ))
        loop.create_task(_sheets_append_metric_async(uid, "feedback", "ok"))
    except RuntimeError:
        pass
    await call.message.edit_text("Принято: 👍 Ок. Спасибо за отзыв!" if get_user(uid)["lang"]=="ru" else "Qabul qilindi: 👍 Ok. Rahmat!")
    await call.answer()

@dp.callback_query(F.data == "fb:bad")
async def cb_fb_bad(call: CallbackQuery):
    uid = call.from_user.id
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_sheets_append_feedback_async(
            uid,
            call.from_user.username or "",
            call.from_user.first_name or "",
            call.from_user.last_name or "",
            "not_ok",
            ""
        ))
        loop.create_task(_sheets_append_metric_async(uid, "feedback", "not_ok"))
    except RuntimeError:
        pass
    await call.message.edit_text("Зафиксировал: 👎 Не ок. Спасибо!" if get_user(uid)["lang"]=="ru" else "Yozib oldim: 👎 Not ok. Rahmat!")
    await call.answer()

@dp.callback_query(F.data == "fb:comment")
async def cb_fb_comment(call: CallbackQuery):
    uid = call.from_user.id
    FEEDBACK_PENDING.add(uid)
    txt = (
        "Напишите короткий комментарий одним сообщением (или /cancel):"
        if get_user(uid)["lang"] == "ru"
        else "Bitta xabar bilan qisqa izoh yozing (yoki /cancel):"
    )
    await call.message.edit_text(txt)
    await call.answer()

# ================== АДМИН: активация подписки =================
@dp.message(Command("grant_creative"))
async def cmd_grant_creative(message: Message):
    if str(message.from_user.id) != str(ADMIN_CHAT_ID):
        return await message.answer("Команда недоступна.")
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Использование: /grant_creative <tg_id>")
    target_id = int(parts[1]); u = get_user(target_id)
    u["plan"] = "creative"
    u["paid_until"] = datetime.utcnow() + timedelta(days=TARIFF["creative"]["duration_days"]); save_users()
    await message.answer(f"✅ Активирован «Creative» для {target_id} до {u['paid_until'].isoformat()}")
    try:
        if bot:
            await bot.send_message(target_id, "✅ Подписка «Creative» активирована. Приятного использования!")
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_update_user_row_async(
                target_id, "", "", "", get_user(target_id).get("lang","ru"), "creative", u["paid_until"]
            ))
            loop.create_task(_sheets_append_metric_async(target_id, "grant", "creative"))
        except RuntimeError:
            pass
    except Exception:
        logging.warning("Notify user failed")

# ================== QUEUE + CACHE ДЛЯ Q/A ==================
# --- Очередь задач и оценки ETA
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "4"))   # кол-во воркеров
QUEUE_NOTICE_THRESHOLD = int(os.getenv("QUEUE_NOTICE_THRESHOLD", "3"))
ETA_MIN_SEC = int(os.getenv("ETA_MIN_SEC", "5"))
ETA_MAX_SEC = int(os.getenv("ETA_MAX_SEC", "120"))

SAVOL_QUEUE: asyncio.Queue = asyncio.Queue()
_avg_service_sec: float | None = None
_service_alpha = float(os.getenv("SERVICE_ALPHA", "0.2"))

# --- Кэш Q/A (точное совпадение вопроса)
QA_CACHE: dict[str, dict] = {}
QA_CACHE_MAX = int(os.getenv("QA_CACHE_MAX", "1000"))
QA_CACHE_TTL = int(os.getenv("QA_CACHE_TTL", "86400"))  # 24h

def _qa_norm(q: str) -> str:
    return re.sub(r"\s+", " ", q.strip().lower())

def qa_cache_get(q: str) -> Optional[str]:
    k = _qa_norm(q)
    it = QA_CACHE.get(k)
    if not it:
        return None
    if time.time() - it["ts"] > QA_CACHE_TTL:
        QA_CACHE.pop(k, None); return None
    return it["answer"]

def qa_cache_set(q: str, a: str):
    k = _qa_norm(q)
    if len(QA_CACHE) >= QA_CACHE_MAX:
        oldest = min(QA_CACHE, key=lambda x: QA_CACHE[x]["ts"])
        QA_CACHE.pop(oldest, None)
    QA_CACHE[k] = {"ts": time.time(), "answer": a}

def _eta_seconds(pos: int) -> int:
    if pos <= 1:
        return ETA_MIN_SEC
    avg = _avg_service_sec if (_avg_service_sec and _avg_service_sec > 0.1) else 6.0
    est = int(((pos - 1) * avg) / max(1, WORKER_CONCURRENCY))
    return max(ETA_MIN_SEC, min(ETA_MAX_SEC, est))

def _update_avg_service(elapsed: float):
    global _avg_service_sec
    if _avg_service_sec is None:
        _avg_service_sec = float(elapsed)
    else:
        _avg_service_sec = _service_alpha * float(elapsed) + (1 - _service_alpha) * _avg_service_sec

class SavolTask(dict):
    """ chat_id, uid, text, lang, topic_hint, use_live """
    pass

async def _process_task(task: SavolTask):
    uid = task["uid"]; chat_id = task["chat_id"]; text = task["text"]
    lang = task["lang"]; topic_hint = task["topic_hint"]; use_live = task["use_live"]
    t0 = time.time()
    try:
        # 1) Кэш Q/A
        cached = qa_cache_get(text)
        if cached:
            try:
                await bot.send_message(chat_id, f"(из базы) {cached}")
            except Exception:
                pass
            append_history(uid, "assistant", cached)
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_sheets_append_history_async(uid, "assistant", cached))
                loop.create_task(_sheets_append_metric_async(uid, "msg", value=str(len(cached)), notes="assistant_len_cached"))
            except RuntimeError:
                pass
            return

        # 2) GPT запрос (live или обычный)
        async def _get_answer():
            return await (answer_with_live_search(text, topic_hint, uid) if use_live else ask_gpt(text, topic_hint, uid))

        reply = await asyncio.wait_for(_get_answer(), timeout=REPLY_TIMEOUT_SEC)
        reply = strip_links_and_cleanup(reply)

        # 3) Верификация по live-источникам (при необходимости)
        if VERIFY_DYNAMIC and _looks_dynamic(text, reply):
            try:
                reply_v = await asyncio.wait_for(verify_with_live_sources(text, reply, topic_hint, uid), timeout=VERIFY_TIMEOUT_SEC)
                if reply_v and reply_v.strip():
                    reply = reply_v
            except Exception:
                pass

        # 4) Отправка пользователю
        try:
            await bot.send_message(chat_id, reply)
        except Exception:
            pass

        # 5) Кэширование
        qa_cache_set(text, reply)

        # 6) История и метрики
        append_history(uid, "user", text)      # как было раньше (после ответа)
        append_history(uid, "assistant", reply)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_append_history_async(uid, "assistant", reply))
            loop.create_task(_sheets_append_metric_async(uid, "msg", value=str(len(reply)), notes="assistant_len"))
        except RuntimeError:
            pass

    except asyncio.TimeoutError:
        err_txt = _friendly_error_text(asyncio.TimeoutError(), lang)
        try:
            await bot.send_message(chat_id, err_txt)
        except Exception:
            pass
        try:
            asyncio.get_running_loop().create_task(_sheets_append_metric_async(uid, "error", "timeout"))
        except RuntimeError:
            pass
    except Exception as e:
        logging.exception("worker _process_task fatal")
        err_txt = _friendly_error_text(e, lang)
        try:
            await bot.send_message(chat_id, err_txt)
        except Exception:
            pass
        try:
            asyncio.get_running_loop().create_task(_sheets_append_metric_async(uid, "error", "generic", notes=str(e)))
        except RuntimeError:
            pass
    finally:
        _update_avg_service(time.time() - t0)

async def _queue_worker(name: str):
    while True:
        task: SavolTask = await SAVOL_QUEUE.get()
        try:
            await _process_task(task)
        finally:
            SAVOL_QUEUE.task_done()

# ================== ОБРАБОТЧИК ВОПРОСОВ =================
@dp.message(F.text)
async def handle_text(message: Message):
    text = (message.text or "").strip()
    uid = message.from_user.id
    u = get_user(uid)

    # Язык
    if is_uzbek(text):
        u["lang"] = "uz"; save_users()

    # --- перехват комментария по фидбэку ---
    if uid in FEEDBACK_PENDING:
        FEEDBACK_PENDING.discard(uid)
        comment_text = text
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_append_feedback_async(
                uid,
                message.from_user.username or "",
                message.from_user.first_name or "",
                message.from_user.last_name or "",
                "comment_only",
                comment_text
            ))
            loop.create_task(_sheets_append_metric_async(uid, "feedback", "comment"))
        except RuntimeError:
            pass
        ok_txt = "Спасибо! Ваш отзыв записан 🙌" if u["lang"]=="ru" else "Rahmat! Fikringiz yozib olindi 🙌"
        await message.answer(ok_txt)
        append_history(uid, "user", comment_text)
        append_history(uid, "assistant", ok_txt)
        try:
            loop.create_task(_sheets_append_history_async(uid, "user", comment_text))
            loop.create_task(_sheets_append_history_async(uid, "assistant", ok_txt))
        except RuntimeError:
            pass
        return

    # Политика
    low = text.lower()
    if any(re.search(rx, low) for rx in ILLEGAL_PATTERNS):
        deny = DENY_TEXT_UZ if u["lang"] == "uz" else DENY_TEXT_RU
        await message.answer(deny)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_append_history_async(uid, "user", text))
            loop.create_task(_sheets_append_history_async(uid, "assistant", deny))
            loop.create_task(_sheets_append_metric_async(uid, "deny", "policy"))
        except RuntimeError:
            pass
        return

    # Проверка подписки / триала (если не в белом списке)
    if (not is_whitelisted(uid)) and (not has_active_sub(u)):
        txt = "💳 Бесплатный период закончился. Подключите ⭐ Creative, чтобы продолжить:"
        await message.answer(txt, reply_markup=pay_kb())
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_append_history_async(uid, "user", text))
            loop.create_task(_sheets_append_history_async(uid, "assistant", txt))
            loop.create_task(_sheets_append_metric_async(uid, "paywall", "shown"))
        except RuntimeError:
            pass
        return

    # Сохраним идентификацию в Users-реестр (username/имя)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_sheets_update_user_row_async(
            uid,
            (message.from_user.username or ""),
            (message.from_user.first_name or ""),
            (message.from_user.last_name or ""),
            u.get("lang", "ru"),
            u.get("plan", "trial"),
            u.get("paid_until"),
        ))
        loop.create_task(_sheets_append_history_async(uid, "user", text))
        loop.create_task(_sheets_append_metric_async(uid, "msg", value=str(len(text)), notes="user_len"))
    except RuntimeError:
        pass

    # === ОЧЕРЕДЬ: ставим задачу и отвечаем статусом
    topic_hint = TOPICS.get(u.get("topic"), {}).get("hint")
    use_live = (FORCE_LIVE or is_time_sensitive(text))

    task = SavolTask({
        "chat_id": message.chat.id,
        "uid": uid,
        "text": text,
        "lang": u.get("lang","ru"),
        "topic_hint": topic_hint,
        "use_live": use_live,
    })
    SAVOL_QUEUE.put_nowait(task)

    pos = SAVOL_QUEUE.qsize()
    if pos >= QUEUE_NOTICE_THRESHOLD:
        eta = _eta_seconds(pos)
        if u.get("lang","ru") == "uz":
            ack = f"⏳ So‘rov navbatga qo‘yildi (№{pos}). Taxminiy kutish ~ {eta} soniya. Javob shu yerga keladi."
        else:
            ack = f"⏳ Ваш запрос поставлен в очередь (№{pos}). Ожидание ~ {eta} сек. Ответ придёт сюда."
    else:
        if u.get("lang","ru") == "uz":
            ack = "🔎 Qabul qildim! Fikr yuritayapman — javob tez orada keladi."
        else:
            ack = "🔎 Принял! Думаю над ответом — пришлю сообщение чуть позже."

    await message.answer(ack)

    append_history(uid, "assistant", ack)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_sheets_append_history_async(uid, "assistant", ack))
    except RuntimeError:
        pass

    return

# ================== Lifespan (инициализация/закрытие) =================
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

    # Вебхук на старте
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

    # === ОЧЕРЕДЬ: старт воркеров ===
    worker_tasks = []
    try:
        for i in range(WORKER_CONCURRENCY):
            worker_tasks.append(asyncio.create_task(_queue_worker(f"w{i+1}")))
        logging.info("Queue workers started: %s", WORKER_CONCURRENCY)
    except Exception:
        logging.exception("Failed to start workers")

    try:
        yield
    finally:
        try:
            for t in worker_tasks:
                t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        except Exception:
            pass
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
