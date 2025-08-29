# -*- coding: utf-8 -*-
import os
import re
import json
import time
import logging
import asyncio
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from httpx import HTTPError, HTTPStatusError
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

# ---- Google Sheets
import gspread
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

# ================== LOGS ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s:%(lineno)d — %(message)s"
)

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
VERIFY_TIMEOUT_SEC = int(os.getenv("VERIFY_TIMEOUT_SEC", "12"))

# Админ
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")  # str

# Персистентные файлы
USERS_DB_PATH = os.getenv("USERS_DB_PATH", "users_limits.json")
HISTORY_DB_PATH = os.getenv("HISTORY_DB_PATH", "chat_history.json")

# --- Google Sheets ENV ---
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")  # JSON одной строкой (или base64)
SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
USERS_SHEET = os.getenv("USERS_SHEET", "Users")
HISTORY_SHEET = os.getenv("HISTORY_SHEET", "History")
METRICS_SHEET = os.getenv("METRICS_SHEET", "Metrics")
FEEDBACK_SHEET = os.getenv("FEEDBACK_SHEET", "Feedback")

# --- HTTPX clients & timeouts (reuse) ---
HTTPX_TIMEOUT = httpx.Timeout(connect=5.0, read=60.0, write=30.0, pool=30.0)
client_openai: Optional[httpx.AsyncClient] = None
client_http: Optional[httpx.AsyncClient] = None

# Параллелизм запросов к модели (чтобы не ловить 429)
MODEL_CONCURRENCY = int(os.getenv("MODEL_CONCURRENCY", "4"))
_model_sem = asyncio.Semaphore(MODEL_CONCURRENCY)

# Очередь и воркеры
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "2"))
QUEUE_NOTICE_THRESHOLD = int(os.getenv("QUEUE_NOTICE_THRESHOLD", "3"))
SAVOL_QUEUE: "asyncio.Queue[SavolTask]" = asyncio.Queue()
def _eta_seconds(queue_size: int) -> int:
    # простой хелпер оценки ожидания (≈ 6 сек/задачу на каждого воркера)
    per_item = 6
    workers = max(1, WORKER_CONCURRENCY)
    return max(3, int(per_item * queue_size / workers))

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
        "mode": u.get("mode", "gpt"),  # gpt | legal
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
                "mode": v.get("mode", "gpt"),
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
            "mode": "gpt",
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
HISTORY: dict[int, list[dict]] = {}  # {user_id: [ {role, content, ts}, ... ]}

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

# ================== ССЫЛКИ/ОЧИСТКА =================
LINK_PAT = re.compile(r"https?://\S+")
MD_LINK_PAT = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)")
SOURCES_BLOCK_PAT = re.compile(r"(?is)\n+источники:\s*.*$")

def strip_links(text: str, allow_links: bool = False) -> str:
    if not text:
        return text
    if not allow_links:
        text = MD_LINK_PAT.sub(r"\1", text)
        text = LINK_PAT.sub("", text)
        text = SOURCES_BLOCK_PAT.sub("", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

def _sanitize_cutoff(text: str) -> str:
    s = text or ""
    for rx in [
        r"актуал\w+\s+до\s+\w+\s+20\d{2}",
        r"знан[^\.!\n]*до\s+\w+\s+20\d{2}",
        r"\bknowledge\s+cutoff\b",
        r"\bas of\s+\w+\s+20\d{2}",
    ]:
        s = re.sub(rx, "", s, flags=re.IGNORECASE)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s

def strip_links_and_cleanup(text: str, allow_links: bool = False) -> str:
    return strip_links(text or "", allow_links=allow_links)

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

# ================== SHEETS =================
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
            ws = sh.add_worksheet(title=USERS_SHEET, rows=100000, cols=9)
            ws.append_row(["ts", "user_id", "username", "first_name", "last_name", "lang", "plan", "paid_until", "mode"], value_input_option="RAW")
            return ws
        except Exception:
            logging.exception("Create Users ws failed")
            return None
    except Exception:
        logging.exception("_users_ws failed")
        return None

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
            ws.resize(cols=need_cols)
        end_a1 = rowcol_to_a1(1, len(headers))
        ws.update(f"A1:{end_a1}", [headers], value_input_option="RAW")
    except Exception:
        logging.exception("ensure header for %s failed", tab_name)

    return ws

def _init_sheets():
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
        try:
            _users_ws = sh.worksheet(USERS_SHEET)
        except gspread.WorksheetNotFound:
            logging.warning("Worksheet '%s' not found, creating…", USERS_SHEET)
            _users_ws = sh.add_worksheet(title=USERS_SHEET, rows=100000, cols=9)
            _users_ws.append_row(["ts", "user_id", "username", "first_name", "last_name", "lang", "plan", "paid_until", "mode"], value_input_option="RAW")

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
                [datetime.utcnow().isoformat(), str(user_id), "", "", "", u.get('lang', 'ru'), u.get('plan', 'trial'), paid, u.get("mode","gpt")],
                value_input_option="RAW"
            )
        await asyncio.to_thread(_do)
        u["registered_to_sheets"] = True
        save_users()
    except Exception:
        logging.exception("sheets_register_user failed")

async def _sheets_update_user_row_async(user_id: int, username: str, first_name: str, last_name: str, lang: str, plan: str, paid_until: Optional[datetime], mode: str):
    if not _users_ws:
        return
    try:
        def _do():
            ws = _users_ws_get()
            if not ws:
                return
            paid = paid_until.isoformat() if paid_until else ""
            ws.append_row(
                [datetime.utcnow().isoformat(), str(user_id), username or "", first_name or "", last_name or "", lang or "ru", plan or "", paid, mode or "gpt"],
                value_input_option="RAW"
            )
        await asyncio.to_thread(_do)
    except Exception:
        logging.exception("sheets_update_user_row failed")

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

async def _sheets_append_feedback_async(user_id: int, username: str, first_name: str, last_name: str, feedback: str, comment: str = ""):
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

# ================== БЕЗОТКАЗНОСТЬ =================
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
    "Ты — SavolBot (часть TripleA). Режим: повседневный помощник. Отвечай естественно и по делу: 6–8 предложений, "
    "с примерами и короткими списками. Лёгкий юмор допустим. Соблюдай законы Узбекистана. "
    "Не давай инструкций для незаконных действий. По медицине — только общая справка + совет обратиться к врачу. "
    "Язык ответа = язык вопроса (RU/UZ). В этом режиме не вставляй URL и ссылки. "
    "Если вопрос про актуальные данные — используй сводку из поиска, но отвечай своими словами. "
    "Никогда не упоминай дату отсечки знаний. Если чего-то не знаешь — проверь через поиск или честно признайся."
)

LEGAL_SYSTEM_PROMPT = (
    "Ты — SavolBot (TripleA), режим ⚖️ Юридический консультант по законодательству Республики Узбекистан. "
    "Дай обобщённую правовую информацию, НЕ индивидуальную юридическую консультацию. "
    "Обязательно опирайся на Актуальные НПА из lex.uz (закон, кодекс, постановление и др.). "
    "Требования: 1) укажи точные названия актов и номера статей/пунктов; 2) добавь ссылки на lex.uz; "
    "3) не используй сторонние источники; 4) если релевантной нормы не найдено или есть риск устаревания — прямо напиши об этом; "
    "5) не выдумывай. Язык ответа = язык вопроса (RU/UZ). Структура: краткое резюме, что разрешено/запрещено, "
    "процедура (шаги, документы, сроки и органы), ответственность/штрафы (если применимо), 'Источники' (список ссылок lex.uz), "
    "и строка 'Проверено: <дата по Ташкенту>'."
)

# ==== Динамика/актуальность ====
TIME_SENSITIVE_PATTERNS = [
    r"\b(сегодня|сейчас|на данный момент|актуальн|в \d{4} году|в 20\d{2})\b",
    r"\b(курс|зарплат|инфляц|ставк|цена|новост|статистик|прогноз)\b",
    r"\b(bugun|hozir|narx|kurs|yangilik)\b",
    r"\b(кто|как зовут|председател|директор|ceo|руководител)\b",
]
def is_time_sensitive(q: str) -> bool:
    return any(re.search(rx, q.lower()) for rx in TIME_SENSITIVE_PATTERNS)

_DYNAMIC_KEYWORDS = [
    "курс", "ставк", "инфляц", "зарплат", "налог", "цена", "тариф", "пособи", "пенси", "кредит",
    "новост", "прогноз", "изменени", "обновлени", "statistika", "narx", "stavka", "yangilik", "price", "rate",
]
def _contains_fresh_year(s: str, window: int = 3) -> bool:
    try:
        y_now = datetime.utcnow().year
    except Exception:
        y_now = 2025
    years = [int(y) for y in re.findall(r"\b(20\d{2})\b", s or "")]
    return any(y_now - y <= window for y in years)

def _looks_dynamic(*texts: str) -> bool:
    low = " ".join([t.lower() for t in texts if t])
    return any(k in low for k in _DYNAMIC_KEYWORDS) or _contains_fresh_year(low)

def _tz_tashkent_date() -> str:
    dt = datetime.utcnow() + timedelta(hours=5)  # UTC+5
    return dt.strftime("%d.%m.%Y")

# ================== ВНЕШНИЕ ЗАПРОСЫ (GPT + Поиск) =================
async def ask_gpt(user_text: str, topic_hint: Optional[str], user_id: int, system_prompt: str, allow_links: bool) -> str:
    if not OPENAI_API_KEY:
        return f"Вы спросили: {user_text}"

    system = system_prompt + (f" Учитывай контекст темы: {topic_hint}" if topic_hint else "")
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
        if re.search(r"\bknowledge\s+cutoff\b", raw, re.I) and TAVILY_API_KEY:
            try:
                return await answer_with_live_search(user_text, topic_hint, user_id, system_prompt, allow_links=allow_links)
            except Exception:
                pass
        return strip_links_and_cleanup(_sanitize_cutoff(raw), allow_links=allow_links)
    except Exception as e:
        logging.exception("ask_gpt failed")
        u = USERS.get(user_id, {"lang": "ru"})
        return _friendly_error_text(e, u.get("lang", "ru"))

async def web_search_tavily(query: str, max_results: int = 3, include_domains: Optional[list[str]] = None, depth: Optional[str] = None) -> Optional[dict]:
    if not TAVILY_API_KEY or client_http is None:
        return None
    if depth is None:
        depth = "advanced" if is_time_sensitive(query) else "basic"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": depth,
        "max_results": max_results,
        "include_answer": True,
        "include_domains": include_domains or [],
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

async def answer_with_live_search(user_text: str, topic_hint: Optional[str], user_id: int, system_prompt: str, allow_links: bool = False) -> str:
    data = await web_search_tavily(user_text, max_results=4)
    if not data:
        return await ask_gpt(user_text, topic_hint, user_id, system_prompt, allow_links=allow_links)

    snippets = []
    for it in (data.get("results") or [])[:4]:
        title = (it.get("title") or "")[:100]
        content = (it.get("content") or "")[:500]
        snippets.append(f"- {title}\n{content}")

    system = system_prompt + " Отвечай, опираясь на сводку (без ссылок в тексте). Кратко, по делу."
    if topic_hint:
        system += f" Учитывай контекст: {topic_hint}"
    user_aug = f"{user_text}\n\nСВОДКА ИСТОЧНИКОВ (без URL):\n" + "\n\n".join(snippets)

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {"model": OPENAI_MODEL, "temperature": 0.35, "messages": build_messages(user_id, system, user_aug)}

    async def _do():
        return await client_openai.post("/chat/completions", headers=headers, json=payload)

    try:
        async with _model_sem:
            r = await _retry(lambda: _do(), attempts=3)
        r.raise_for_status()
        answer = r.json()["choices"][0]["message"]["content"].strip()
        final = strip_links_and_cleanup(_sanitize_cutoff(answer), allow_links=allow_links)

        if _looks_dynamic(user_text, final):
            final += f"\n\n_Проверено: {_tz_tashkent_date()}_"
        return final
    except Exception as e:
        logging.exception("live answer failed")
        u = USERS.get(user_id, {"lang": "ru"})
        return _friendly_error_text(e, u.get("lang", "ru"))

# --- LEGAL (только lex.uz) ---
async def legal_search_lex(query: str, max_results: int = 5) -> Optional[dict]:
    if not TAVILY_API_KEY or client_http is None:
        return None
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "max_results": max_results,
        "include_answer": False,
        "include_domains": ["lex.uz"],
    }
    async def _do():
        return await client_http.post("https://api.tavily.com/search", json=payload)
    try:
        r = await _retry(lambda: _do(), attempts=2)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning("legal search failed: %s", e)
        return None

def _format_lex_results(data: dict, limit: int = 5) -> list[dict]:
    items = []
    for it in (data.get("results") or [])[:limit]:
        url = it.get("url") or ""
        if "lex.uz" not in url:
            continue
        items.append({
            "title": (it.get("title") or "").strip(),
            "snippet": (it.get("content") or "").strip(),
            "url": url
        })
    return items

async def answer_legal(user_text: str, user_id: int) -> str:
    data = await legal_search_lex(user_text, max_results=6)
    sources = _format_lex_results(data or {}, limit=5) if data else []
    if not sources:
        return ("Не нашёл подтверждённой нормы на lex.uz по вашему вопросу. "
                "Уточните формулировку (закон/сфера) или обратитесь к юристу.")
    brief = []
    for s in sources:
        t = (s["title"] or "")[:120]
        sn = (s["snippet"] or "")[:600]
        brief.append(f"- {t}\n{sn}\n{s['url']}")
    user_aug = f"ВОПРОС:\n{user_text}\n\nНАЙДЕННЫЕ ДОКУМЕНТЫ (lex.uz):\n" + "\n\n".join(brief)

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0.1,
        "messages": build_messages(user_id, LEGAL_SYSTEM_PROMPT, user_aug),
    }
    async def _do():
        return await client_openai.post("/chat/completions", headers=headers, json=payload)
    try:
        async with _model_sem:
            r = await _retry(lambda: _do(), attempts=3)
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"].strip()
        return strip_links_and_cleanup(_sanitize_cutoff(text), allow_links=True)
    except Exception as e:
        logging.exception("answer_legal failed")
        u = USERS.get(user_id, {"lang": "ru"})
        return _friendly_error_text(e, u.get("lang","ru"))

# ================== FEEDBACK / UI =================
def feedback_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👍 Ок", callback_data="fb:ok"),
            InlineKeyboardButton(text="👎 Не ок", callback_data="fb:bad"),
        ],
        [InlineKeyboardButton(text="✍️ Оставить комментарий", callback_data="fb:comment")],
        [InlineKeyboardButton(text="↩️ Закрыть", callback_data="fb:close")],
    ])

FEEDBACK_PENDING: set[int] = set()

# ================== SAFE HELPERS =================
async def safe_answer(msg: Message, text: str, **kwargs):
    try:
        return await msg.answer(text, **kwargs)
    except TelegramBadRequest as e:
        logging.warning("answer failed: %s", e)
    except Exception:
        logging.exception("answer fatal")

async def safe_edit_text(msg, text: str, **kwargs):
    try:
        return await msg.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        logging.warning("edit_text failed: %s", e)
    except Exception:
        logging.exception("edit_text fatal")

async def safe_edit_reply_markup(msg, **kwargs):
    try:
        return await msg.edit_reply_markup(**kwargs)
    except TelegramBadRequest as e:
        logging.warning("edit_reply_markup failed: %s", e)
    except Exception:
        logging.exception("edit_reply_markup fatal")

async def safe_delete(msg):
    try:
        return await msg.delete()
    except TelegramBadRequest as e:
        logging.warning("delete failed: %s", e)
    except Exception:
        logging.exception("delete fatal")

# ================== КЛАВИАТУРЫ / ТЕКСТЫ =================
WELCOME_RU = (
    "👋 Привет! Я — SavolBot, часть команды TripleA.\n"
    "Теперь у меня ДВА режима:\n"
    "• 👤 Повседневный помощник (GPT)\n"
    "• ⚖️ Юридический консультант (только lex.uz, без фантазий)\n\n"
    "Тариф ⭐ Creative: $10/мес, 7 дней бесплатно. /tariffs\n"
    "Переключить режим: /mode  • Правила юр-раздела: /legal_rules"
)
WELCOME_UZ = (
    "👋 Salom! Men — SavolBot, TripleA jamoasi.\n"
    "Endi IKKI rejim:\n"
    "• 👤 Kundalik yordamchi (GPT)\n"
    "• ⚖️ Yuridik maslahatchi (faqat lex.uz)\n\n"
    "⭐ Creative: $10/oy, 7 kun bepul. /tariffs\n"
    "Rejimni almashtirish: /mode  • Qoidalar: /legal_rules"
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

def mode_kb(lang="ru", current=None):
    gpt = "🧰 GPT-помощник" if lang == "ru" else "🧰 GPT-yordamchi"
    legal = "⚖️ Юридический консультант" if lang == "ru" else "⚖️ Yuridik maslahatchi"
    rows = [
        [InlineKeyboardButton(text=("✅ " + gpt) if current == "gpt" else gpt, callback_data="mode:gpt")],
        [InlineKeyboardButton(text=("✅ " + legal) if current == "legal" else legal, callback_data="mode:legal")],
        [InlineKeyboardButton(text="↩️ Закрыть / Yopish", callback_data="mode:close")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ================== КОМАНДЫ =================
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

@dp.errors()
async def on_error(event, exception):
    logging.exception("Unhandled error: %s | event=%s", exception, repr(event))
    try:
        obj = getattr(event, "update", None) or getattr(event, "event", None)
        chat_id = None
        if obj and hasattr(obj, "message") and obj.message and hasattr(obj.message, "chat"):
            chat_id = obj.message.chat.id
        if chat_id and bot:
            await bot.send_message(chat_id, "⚠️ Внутренняя ошибка обработчика. Попробуйте повторить запрос.")
    except Exception:
        pass

@dp.message(Command("help"))
async def cmd_help(message: Message):
    u = get_user(message.from_user.id)
    if u["lang"] == "ru":
        txt = "ℹ️ Я умею: повседневные ответы (GPT) и юр-раздел по lex.uz.\n/tariffs — тариф, /myplan — план, /topics — темы, /mode — переключение режимов, /legal_rules — правила юр-раздела."
    else:
        txt = "ℹ️ Men kundalik rejim (GPT) va yuridik bo‘lim (faqat lex.uz) bilan ishlayman.\n/tariffs, /myplan, /topics, /mode, /legal_rules — foydali buyruqlar."
    await message.answer(txt)
    try:
        asyncio.get_running_loop().create_task(_sheets_append_history_async(message.from_user.id, "assistant", txt))
    except RuntimeError:
        pass

@dp.message(Command("about"))
async def cmd_about(message: Message):
    u = get_user(message.from_user.id)
    if u.get("lang", "ru") == "ru":
        txt = (
            "🤖 SavolBot от TripleA — два режима:\n"
            "1) 🧰 Помощник по повседневным вопросам (GPT): идеи, тексты, советы.\n"
            "2) ⚖️ Юридический консультант: только по законам РУз, с прямыми ссылками на lex.uz, без домыслов.\n\n"
            "Команды:\n"
            "/mode — выбрать режим\n"
            "/tariffs — тариф\n"
            "/myplan — мой план\n"
            "/topics — темы (для GPT)\n"
            "/new — очистить контекст"
        )
    else:
        txt = (
            "🤖 SavolBot (TripleA) — ikki rejim:\n"
            "1) 🧰 Kundalik yordamchi (GPT): g‘oyalar, matnlar, maslahatlar.\n"
            "2) ⚖️ Yuridik maslahatchi: faqat O‘zR qonунlari, lex.uz havolalari bilan, taxminsiz.\n\n"
            "Buyruqlar:\n"
            "/mode — rejim tanlash\n"
            "/tariffs — tarif\n"
            "/myplan — reja\n"
            "/topics — mavzular (GPT uchun)\n"
            "/new — kontekstni tozalash"
        )
    await safe_answer(message, txt, reply_markup=mode_kb(u.get("lang","ru"), current=get_mode(message.from_user.id)))

# ================== РЕЖИМЫ: GPT / LEGAL ==================
def get_mode(user_id: int) -> str:
    u = get_user(user_id)
    if not u.get("mode"):
        u["mode"] = "gpt"; save_users()
    return u["mode"]

def set_mode(user_id: int, mode: str):
    u = get_user(user_id); u["mode"] = mode; save_users()

@dp.message(Command("mode"))
async def cmd_mode(message: Message):
    u = get_user(message.from_user.id)
    head = "Выберите режим:" if u.get("lang","ru") == "ru" else "Rejimni tanlang:"
    await safe_answer(message, head, reply_markup=mode_kb(u.get("lang","ru"), current=get_mode(message.from_user.id)))

@dp.callback_query(F.data.startswith("mode:"))
async def cb_mode(call: CallbackQuery):
    u = get_user(call.from_user.id)
    _, m = call.data.split(":", 1)
    if m == "close":
        await safe_delete(call.message)
        return await call.answer("OK")
    if m in ("gpt", "legal"):
        set_mode(call.from_user.id, m)
        lang = u.get("lang","ru")
        label = ("GPT-помощник" if lang=="ru" else "GPT-yordamchi") if m=="gpt" else ("Юридический консультант" if lang=="ru" else "Yuridik maslahatchi")
        await safe_edit_reply_markup(call.message, reply_markup=mode_kb(lang, current=m))
        return await call.answer(("Режим: " + label) if lang == "ru" else ("Rejim: " + label))

LEGAL_RULES_RU = (
    "⚖️ Правила юридического раздела:\n"
    "• Отвечаю только в рамках законодательства Республики Узбекистан и только по проверенным нормам с lex.uz.\n"
    "• Если не нахожу точную норму — честно сообщаю, что ответа нет, без домыслов.\n"
    "• Даю короткие выдержки с указанием статьи/пункта и прямой ссылкой на lex.uz.\n"
    "• Это не индивидуальная юридическая помощь и не замена адвокату."
)
LEGAL_RULES_UZ = (
    "⚖️ Yuridik bo‘lim qoidalari:\n"
    "• Faqat O‘zbekiston Respublikasi qonunchiligiga tayangan holda va faqat lex.uz manbalari bilan javob beraman.\n"
    "• Aniq norma topilmasa — taxminsiz, halol javob: hozircha topilmadi.\n"
    "• Qisqa iqtiboslar: modda/band raqami va to‘g‘ridan-to‘g‘ri lex.uz havolasi bilan.\n"
    "• Bu shaxsiy yuridik yordam emas, advokat o‘rnini bosa olmaydi."
)

@dp.message(Command("legal"))
async def cmd_legal(message: Message):
    u = get_user(message.from_user.id)
    set_mode(message.from_user.id, "legal")
    txt_rules = LEGAL_RULES_RU if u.get("lang","ru") == "ru" else LEGAL_RULES_UZ
    head = "Режим переключён: ⚖️ Юридический консультант.\n\n" if u.get("lang","ru")=="ru" else "Rejim almashtirildi: ⚖️ Yuridik maslahatchi.\n\n"
    await safe_answer(message, head + txt_rules, reply_markup=mode_kb(u.get("lang","ru"), current="legal"))

# ================== ТЕКСТ-ХЕНДЛЕР ==================
@dp.message(F.text)
async def handle_text(message: Message):
    text = (message.text or "").strip()
    uid = message.from_user.id
    u = get_user(uid)

    # Язык
    if is_uzbek(text):
        u["lang"] = "uz"; save_users()

    # Фидбек-комментарий
    if uid in FEEDBACK_PENDING:
        FEEDBACK_PENDING.discard(uid)
        comment_text = text
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_append_feedback_async(
                uid, message.from_user.username or "", message.from_user.first_name or "",
                message.from_user.last_name or "", "comment_only", comment_text
            ))
            loop.create_task(_sheets_append_metric_async(uid, "feedback", "comment"))
        except RuntimeError:
            pass
        ok_txt = "Спасибо! Ваш отзыв записан 🙌" if u["lang"]=="ru" else "Rahmat! Fikringiz yozib olindi 🙌"
        await message.answer(ok_txt)
        append_history(uid, "user", comment_text)
        append_history(uid, "assistant", ok_txt)
        try:
            asyncio.get_running_loop().create_task(_sheets_append_history_async(uid, "user", comment_text))
            asyncio.get_running_loop().create_task(_sheets_append_history_async(uid, "assistant", ok_txt))
        except RuntimeError:
            pass
        return

    # Политика запрещённого контента
    low = text.lower()
    if any(re.search(rx, low) for rx in ILLEGAL_PATTERNS):
        deny = DENY_TEXT_UZ if u["lang"] == "uz" else DENY_TEXT_RU
        await safe_answer(message, deny)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_append_history_async(uid, "user", text))
            loop.create_task(_sheets_append_history_async(uid, "assistant", deny))
            loop.create_task(_sheets_append_metric_async(uid, "deny", "policy"))
        except RuntimeError:
            pass
        return

    # Paywall (если не в белом списке)
    if (uid not in WHITELIST_USERS) and (not has_active_sub(u)):
        txt = "💳 Бесплатный период закончился. Подключите ⭐ Creative, чтобы продолжить:"
        await safe_answer(message, txt, reply_markup=pay_kb())
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_append_history_async(uid, "user", text))
            loop.create_task(_sheets_append_history_async(uid, "assistant", txt))
            loop.create_task(_sheets_append_metric_async(uid, "paywall", "shown"))
        except RuntimeError:
            pass
        return

    # Запишем идентификацию + историю/метрики
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
            u.get("mode", "gpt"),
        ))
        loop.create_task(_sheets_append_history_async(uid, "user", text))
        loop.create_task(_sheets_append_metric_async(uid, "msg", value=str(len(text)), notes="user_len"))
    except RuntimeError:
        pass

    # Роутинг по режимам
    cur_mode = get_mode(uid)

    # Очередь + ACK
    topic_hint = TOPICS.get(u.get("topic"), {}).get("hint")
    use_live = (cur_mode == "legal") or FORCE_LIVE or is_time_sensitive(text)

    if cur_mode == "legal":
        # LEGAL ответ (без очереди)
        try:
            reply = await asyncio.wait_for(answer_legal(text, uid), timeout=REPLY_TIMEOUT_SEC)
            reply = strip_links_and_cleanup(reply, allow_links=True)
        except asyncio.TimeoutError:
            reply = _friendly_error_text(asyncio.TimeoutError(), u.get("lang","ru"))
        except Exception as e:
            logging.exception("legal reply fatal")
            reply = _friendly_error_text(e, u.get("lang","ru"))
        await safe_answer(message, reply)

        append_history(uid, "assistant", reply)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_sheets_append_history_async(uid, "assistant", reply))
            loop.create_task(_sheets_append_metric_async(uid, "msg", value=str(len(reply)), notes="assistant_len_legal"))
        except RuntimeError:
            pass
        return

    # GPT режим — ставим задачу в очередь
    task = SavolTask(
        chat_id=message.chat.id,
        uid=uid,
        text=text,
        lang=u.get("lang","ru"),
        topic_hint=topic_hint,
        use_live=use_live,
    )
    SAVOL_QUEUE.put_nowait(task)

    pos = SAVOL_QUEUE.qsize()
    if pos >= QUEUE_NOTICE_THRESHOLD:
        eta = _eta_seconds(pos)
        if u.get("lang","ru") == "uz":
            ack = f"⏳ So‘rov navbatga qo‘yildi (№{pos}). Taxminiy kutish ~ {eta} soniya. Javob shu yerga keladi."
        else:
            ack = f"⏳ Ваш запрос поставлен в очередь (№{pos}). Ожидание ~ {eta} сек. Ответ придёт сюда."
    else:
        ack = "🔎 Qabul qildim! Fikr yuritayapman — javob tez orada keladi." if u.get("lang","ru")=="uz" else "🔎 Принял! Думаю над ответом — пришлю сообщение чуть позже."

    await safe_answer(message, ack)
    append_history(uid, "assistant", ack)
    try:
        asyncio.get_running_loop().create_task(_sheets_append_history_async(uid, "assistant", ack))
    except RuntimeError:
        pass

# ================== ОЧЕРЕДЬ/ВОРКЕРЫ =================
@dataclass
class SavolTask:
    chat_id: int
    uid: int
    text: str
    lang: str = "ru"
    topic_hint: Optional[str] = None
    use_live: bool = False

REPLY_TIMEOUT_SEC = int(os.getenv("REPLY_TIMEOUT_SEC", "15"))

async def _process_task(t: SavolTask):
    u = get_user(t.uid)
    allow_links = False
    system_prompt = BASE_SYSTEM_PROMPT
    # Генерация черновика
    try:
        if t.use_live and TAVILY_API_KEY:
            draft = await asyncio.wait_for(
                answer_with_live_search(t.text, t.topic_hint, t.uid, system_prompt, allow_links=allow_links),
                timeout=REPLY_TIMEOUT_SEC
            )
        else:
            draft = await asyncio.wait_for(
                ask_gpt(t.text, t.topic_hint, t.uid, system_prompt, allow_links=allow_links),
                timeout=REPLY_TIMEOUT_SEC
            )
    except asyncio.TimeoutError:
        draft = _friendly_error_text(asyncio.TimeoutError(), u.get("lang","ru"))
    except Exception as e:
        logging.exception("process_task draft failed")
        draft = _friendly_error_text(e, u.get("lang","ru"))

    # Верификация динамики по желанию
    final = draft
    if VERIFY_DYNAMIC and _looks_dynamic(t.text, draft) and TAVILY_API_KEY:
        try:
            final = await asyncio.wait_for(
                verify_with_live_sources(t.text, draft, t.topic_hint, t.uid),
                timeout=VERIFY_TIMEOUT_SEC
            )
        except Exception:
            pass

    # Отправка ответа в чат
    try:
        if bot:
            await bot.send_message(t.chat_id, final, reply_markup=feedback_kb())
    except Exception:
        logging.exception("send_message failed")

    # История/метрики
    append_history(t.uid, "assistant", final)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_sheets_append_history_async(t.uid, "assistant", final))
        loop.create_task(_sheets_append_metric_async(t.uid, "msg", value=str(len(final)), notes="assistant_len"))
    except RuntimeError:
        pass

async def _queue_worker(name: str):
    logging.info("worker %s: started", name)
    while True:
        t: SavolTask = await SAVOL_QUEUE.get()
        try:
            await _process_task(t)
        except Exception:
            logging.exception("worker %s: task failed", name)
        finally:
            SAVOL_QUEUE.task_done()

# ================== LIFESPAN & APP =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # локальные базы
    load_users()
    load_history()

    # Sheets
    _init_sheets()

    # HTTPX
    HTTP2_ENABLED = os.getenv("HTTP2_ENABLED", "0") == "1"
    try:
        import h2  # noqa
        _h2_ok = True
    except Exception:
        _h2_ok = False
    use_http2 = HTTP2_ENABLED and _h2_ok

    global client_openai, client_http
    client_openai = httpx.AsyncClient(base_url=OPENAI_API_BASE, timeout=HTTPX_TIMEOUT, http2=use_http2)
    client_http = httpx.AsyncClient(timeout=HTTPX_TIMEOUT, http2=use_http2)

    # Вебхук Telegram
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

    # Старт воркеров
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

# === FastAPI app (ВАЖНО: на верхнем уровне) ===
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
