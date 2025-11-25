from __future__ import annotations

import asyncio
import os
import re
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from html import escape as _html_escape
from telegram import (
    Bot,
    ChatPermissions,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode, ChatType
from telegram.error import TelegramError, BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

# =========================
# Весь текст (сообщения, кнопки т.д.)
# =========================

TEXTS = {
    "progress": {
        "creating_issue": "⏳ Ожидайте создания заявки и таска в Jira...",
        "closing_issue": "⏳ Закрываем заявку и таск в Jira...",
    },
    "buttons": {
        "back": "⬅ Назад",
        "skip": "🚫 Не указывать",
        "dont_know": "Неизвестно",
        "yes": "Да",
        "no": "Нет",
        "done": "Готово",
        "vehicle_plus": "+ {label}",
        "vehicle_minus": "− {label}",
        "edit": "✍️ Внести изменения",
        "create_issue": "✅ Создать заявку",
        "call_ra": "🛟 Вызвать RA",
        "close_all": "🛑 Закрыть заявку",
        "open_chat": "💬 Перейти в чат",
    },
    "vehicles": {
        "light": "Легковой",
        "bus": "Автобус",
        "truck": "Грузовой",
        "moto": "Мото",
    },
    "common": {
        "ticket_title": "Заявка #{id}",
        "jira_create_failed": "⚠️ Не удалось создать задачу в Jira.",
        "select_next": "Выберите следующее действие",
        "help_needed": "Требуется Ваша помощь",
        "issue_closed": "✅ Заявка и таск в Jira закрыты",
        "empty": "—",
        "preview_label": "<b><i>Проверьте заявку перед созданием</i></b>\n",
    },
    "primary_block_labels": {
        "incident_type": "Тип происшествия",
        "incident_source": "Откуда узнали о происшествии",
        "brand": "Тип ТС",
        "plate_vats": "Госномер ВАТС",
        "plate_ref": "Госномер ПП",
        "location": "Место происшествия",
        "incident_time": "Дата и время ДТП",          # для ДТП
        "incident_time_break": "Время инцидента",     # для Поломки
        "dtp_type": "Тип ДТП",
        "dtp_vehicles": "ТС, попавшие в ДТП",
        "dtp_damage": "Повреждения",
        "obstacle_on_road": "ВАТС создает помехи на трассе",
        "break_symptoms": "Симптомы",
        "problem_desc": "Описание проблемы",
        "notes": "Примечания",
    },
    "headings": {
        "ra_form": "<b>Вводные данные для работы RA:</b>",
    },
    # Для сводки RA:
    "ra_summary_labels": {
        "ra_need_dtp_formalization": "Требуется оформление/урегулирование ДТП",
        "ra_need_diagnosis": "Требуется осмотр/диагностика",
        "ra_need_repair": "Требуется ремонт",
        "ra_need_evacuation": "Требуется эвакуация",
        "ra_called_112": "Был ли вызов 112",
    },
    # RA (старые поля на всякий случай)
    "ra_flags": {
        "ra_need_diagnosis": "Требуется осмотр или диагностика",
        "ra_need_repair": "Требуется ремонт",
        "ra_need_evacuation": "Требуется эвакуация",
        "ra_called_112": "Был совершен звонок в 112",
    },
    "questions": {
        "incident_type": "Выберите тип происшествия:",
        "incident_source": "Откуда узнали о происшествии?",
        "dtp_type": "Выберите тип ДТП:",
        "brand": "Выберите тип ВАТС:",
        "dtp_vehicles": "Типы и количество ТС, попавших в ДТП:",
        "dtp_damage_text": "Опишите наличие и степень повреждений ВАТС/прицепа",
        "obstacle_on_road": "Создаёт ли ВАТС помехи на трассе?",
        "break_symptoms": "Опишите симптомы неисправности:",
        "plate_vats": "Введите госномер ВАТС \n(1 буква + 3 цифры + 2 буквы + 2/3 цифры региона)",
        "plate_ref": "Введите госномер рефа/пп \n(2 буквы + 4 цифры + 2/3 цифры региона)",
        "location": "Укажите местоположение ВАТС (координаты/ориентиры)",
        "problem_desc": "Напишите подробности проблемы",
        "notes": "Особые отметки, примечание",

        # RA сценарий
        "ra_need_dtp_formalization": "Требуется оформление ДТП?",
        "ra_need_diagnosis": "Требуется осмотр/диагностика?",
        "ra_need_repair": "Требуется ремонт?",
        "ra_need_evacuation": "Требуется эвакуация?",
        "ra_called_112": "Был звонок 112?",
    },
    "choices": {
        "incident_type": {"DTP": "ДТП", "BREAK": "Поломка"},
        "incident_source": {
            "DRIVER_CALL": "Звонок от водителя",
            "TELEOPS_REQUEST": "Запрос в Телеопс",
            "EXTERNAL_CALL": "Звонок по внешнему номеру",
            "OTHER": "Другое",
        },
        "dtp_type": {"COLLISION": "Cтолкновение", "ROLLOVER": "Опрокидывание", "RUNOVER": "Наезд"},
        "brand": {"KIA_CEED": "Kia Ceed", "SITRAK": "Sitrak"},
        "yn": {"YES": "Да", "NO": "Нет"},
        "ynu": {"YES": "Да", "NO": "Нет", "UNKNOWN": "Неизвестно"},
    },
    "errors": {
        "plate_invalid": "❌ Неверный формат госномера ВАТС ❌",
        "plate_ref_invalid": "❌ Неверный формат госномера ПП ❌",
        "empty_text": "❌ Пустой ввод ❌",
    },
    "messages": {
        "need_main_before_ra": "Сначала создайте основную задачу (нет родителя для RA).",
        "dtp_vehicles_hint": "<i>Используйте кнопки +/− и затем «Готово»</i>",
        "ra_subject": "Заявка #{ticket_id} — RA",
        "missing_bot_token": "Заполните .env: BOT_TOKEN",
        "incident_source_other_prompt": "Введите источник происшествия в свободной форме:",
    },
    "jira": {
        "config_missing": "Не задана конфигурация Jira (JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN).",
        "network_error": "Сеть/подключение: {error}",
        "bad_json_created": "201 Created, но не удалось разобрать ответ: {body}",
        "bad_json_ok": "200 OK, но не удалось разобрать ответ: {body}",
        "preferred_close_statuses": ["Done", "Closed", "Resolve", "Resolved", "Закрыто", "Закрыть", "Готово", "Выполнено"],
        "no_transitions": "Нет доступных переходов для закрытия.",
        "need_main_issue": "⚠️ Основная задача ещё не создана.",
    },
    "form_edit_title": "Выберите пункт для изменения:",
    "back_to_summary": "⬅ Назад к итогу",
    "dispatch": {
        "prompt_text": "Если необходимо вмешательство диспетчера, то нажмите кнопку ниже",
        "button_text": "📞 Вызвать диспетчера",
        "help_message": "Требуется вмешательство диспетчера",
        "delay_notice": "<b>⚠️ Возможна задержка ТС в пути ⚠️</b>\n\n<i>— Описание проблемы —</i>",
        "ra_notice": "<b>Был вызван RA</b>\n",
        "sent": "Запрос отправлен диспетчеру.",
        "missing_chat": "Чат для диспетчера не настроен.",
    },
}
T = TEXTS

# =========================
# Конфиг/окружение
# =========================

load_dotenv()


def _env_int(*names: str, default: int = 0) -> int:
    for name in names:
        val = os.getenv(name)
        if val is None:
            continue
        val = val.strip()
        if not val:
            continue
        try:
            return int(val)
        except ValueError:
            continue
    return default


def _env_flag(*names: str, default: bool = False) -> bool:
    truthy = {"1", "true", "True", "yes", "on"}
    falsy = {"0", "false", "False", "no", "off"}
    for name in names:
        val = os.getenv(name)
        if val is None:
            continue
        val = val.strip()
        if not val:
            continue
        if val in truthy:
            return True
        if val in falsy:
            return False
    return default


BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Куда слать уведомления (кнопка "Перейти в чат")
RA_NOTIFY_CHAT_ID = _env_int("RA_NOTIFY_CHAT_ID", "DISPATCH_CHAT_ID", default=0)
DISPATCH_ALERT_CHAT_ID = _env_int("DISPATCH_ALERT_CHAT_ID", default=0)

# Jira
JIRA_BASE_URL = os.getenv("JIRA_BASE_URL", "").rstrip("/")
JIRA_EMAIL = os.getenv("JIRA_EMAIL", "")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN", "")
JIRA_PROJECT_KEY = os.getenv("JIRA_PROJECT_KEY", "")

JIRA_ISSUE_TYPE_MAIN_ID = os.getenv("JIRA_ISSUE_TYPE_MAIN_ID", "")
JIRA_ISSUE_TYPE_MAIN = os.getenv("JIRA_ISSUE_TYPE_MAIN", "Task")

# Внешний userbot (через адаптер)
USERBOT_ENABLED = _env_flag("USERBOT_ENABLED", "USE_USERBOT", default=False)
MANAGED_BOT_USERNAME = os.getenv("MANAGED_BOT_USERNAME", "").lstrip("@")  # ВАШ PTB-бот

_CHAT_FACTORY_ADAPTER = None  # сюда прокидываем adapter из app.py

# =========================
# Утилиты
# =========================


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def current_request_time() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M")


def short_id(n: int = 8) -> str:
    import secrets
    import string

    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


def _safe_user_html(text: Optional[str]) -> str:
    """
    Экранирует пользовательский текст для HTML-разметки Telegram.
    Используется только для полей, которые приходят напрямую от пользователя.
    """
    if text is None or text == "":
        return T["common"]["empty"]
    return _html_escape(text)


async def safe_edit_message_text(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: ParseMode = ParseMode.HTML,
) -> None:
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        return
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        logging.warning("safe_edit_message_text fallback to send_message: %s", e)

    chat_id = None
    if getattr(query, "message", None) and getattr(query.message, "chat", None):
        chat_id = query.message.chat.id
    if not chat_id and getattr(query, "from_user", None):
        chat_id = query.from_user.id
    if chat_id:
        await context.bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)


async def safe_edit_reply_markup(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    reply_markup: Optional[InlineKeyboardMarkup],
) -> None:
    try:
        await query.edit_message_reply_markup(reply_markup=reply_markup)
        return
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        logging.warning("safe_edit_reply_markup fallback to send_message: %s", e)

    chat_id = None
    if getattr(query, "message", None) and getattr(query.message, "chat", None):
        chat_id = query.message.chat.id
    if not chat_id and getattr(query, "from_user", None):
        chat_id = query.from_user.id
    if chat_id:
        text = ""
        if getattr(query, "message", None):
            text = getattr(query.message, "text_html", None) or (query.message.text or "")
        await context.bot.send_message(chat_id, text or " ", reply_markup=reply_markup, parse_mode=ParseMode.HTML)


async def remove_dispatch_button(query, context: ContextTypes.DEFAULT_TYPE, *, ticket_id: str) -> None:
    message = getattr(query, "message", None)
    markup: Optional[InlineKeyboardMarkup] = getattr(message, "reply_markup", None)
    if not markup or not getattr(markup, "inline_keyboard", None):
        return
    target = f"dispatch|{ticket_id}"
    new_rows: List[List[InlineKeyboardButton]] = []
    removed = False
    for row in markup.inline_keyboard:
        new_row: List[InlineKeyboardButton] = []
        for button in row:
            if getattr(button, "callback_data", None) == target:
                removed = True
                continue
            new_row.append(button)
        if new_row:
            new_rows.append(new_row)
    if not removed:
        return
    await safe_edit_reply_markup(
        query,
        context,
        reply_markup=InlineKeyboardMarkup(new_rows) if new_rows else None,
    )

# =========================
# Валидация госномеров
# =========================

SERIES_CYR = "АВЕКМНОРСТУХ"
LAT_TO_CYR = str.maketrans({
    "A": "А", "B": "В", "E": "Е", "K": "К", "M": "М", "H": "Н",
    "O": "О", "P": "Р", "C": "С", "T": "Т", "Y": "У", "X": "Х",
})

PLATE_RE_34 = re.compile(rf"^([{SERIES_CYR}])(\d{{3,4}})([{SERIES_CYR}]{{2}})(\d{{2,3}})$")
PLATE_RE_3 = re.compile(rf"^([{SERIES_CYR}])(\d{{3}})([{SERIES_CYR}]{{2}})(\d{{2,3}})$")

REF_COMPACT_RE = re.compile(rf"^([{SERIES_CYR}]{{2}})(\d{{4}})(\d{{2,3}})$")


def normalize_vats_plate(text: str, *, brand: Optional[str]) -> Optional[str]:
    if not text:
        return None
    s = "".join(ch for ch in (text or "").upper() if ch.isalnum()).translate(LAT_TO_CYR)
    rx = PLATE_RE_3 if brand == "KIA_CEED" else PLATE_RE_34
    m = rx.match(s)
    if not m:
        return None
    l1, d, l2, reg = m.groups()
    return f"{l1}{d}{l2}{reg}"


def normalize_ref_plate(text: str) -> Optional[str]:
    if not text:
        return None
    s = "".join(ch for ch in (text or "").upper() if ch.isalnum()).translate(LAT_TO_CYR)
    m = REF_COMPACT_RE.match(s)
    if not m:
        return None
    letters2, d4, reg = m.groups()
    return f"{letters2}{d4}{reg}"


def format_vats_display(compact: Optional[str]) -> str:
    if not compact:
        return T["common"]["empty"]
    m = PLATE_RE_34.match(compact) or PLATE_RE_3.match(compact)
    if not m:
        return compact
    l1, d, l2, reg = m.groups()
    return f"{l1}{d}{l2} {reg}"


def format_ref_display(compact: Optional[str]) -> str:
    if not compact:
        return T["common"]["empty"]
    m = REF_COMPACT_RE.match(compact)
    if not m:
        return compact
    letters2, d4, reg = m.groups()
    return f"{letters2}{d4} {reg}"

# =========================
# Анкета и шаги
# =========================

PRIMARY_MODE = "primary"
RA_MODE = "ra"


@dataclass
class Ticket:
    id: str
    user_id: int
    username: Optional[str]
    created_at: str
    incident_type: Optional[str] = None
    incident_source: Optional[str] = None
    dtp_type: Optional[str] = None
    incident_time: Optional[str] = None
    brand: Optional[str] = None
    plate_vats: Optional[str] = None
    plate_ref: Optional[str] = None
    dtp_vehicles: Dict[str, int] = field(default_factory=dict)
    location: Optional[str] = None
    dtp_damage_text: Optional[str] = None
    obstacle_on_road: Optional[str] = None
    break_symptoms: Optional[str] = None
    problem_desc: Optional[str] = None
    notes: Optional[str] = None
    jira_main: Optional[str] = None

    # RA flags
    ra_need_dtp_formalization: Optional[str] = None  # только для ДТП
    ra_need_diagnosis: Optional[str] = None
    ra_need_repair: Optional[str] = None
    ra_need_evacuation: Optional[str] = None
    ra_called_112: Optional[str] = None
    ra_chat_id: Optional[int] = None


ALL_KNOWN_KEYS = {
    "incident_type", "incident_source", "dtp_type", "incident_time", "brand",
    "plate_vats", "plate_ref", "dtp_vehicles", "location",
    "dtp_damage_text", "obstacle_on_road", "break_symptoms",
    "problem_desc", "notes",
    "ra_need_dtp_formalization",
    "ra_need_diagnosis", "ra_need_repair", "ra_need_evacuation", "ra_called_112",
}

STEP_INPUT_KIND: Dict[str, str] = {
    "incident_type": "choice",
    "incident_source": "choice",
    "dtp_type": "choice",
    "brand": "choice",
    "plate_vats": "plate",
    "plate_ref": "plate_ref",
    "dtp_vehicles": "counter",
    "location": "text",
    "dtp_damage_text": "text",
    "obstacle_on_road": "choice",
    "break_symptoms": "text",
    "problem_desc": "text",
    "notes": "text",

    # RA
    "ra_need_dtp_formalization": "choice",
    "ra_need_diagnosis": "choice",
    "ra_need_repair": "choice",
    "ra_need_evacuation": "choice",
    "ra_called_112": "choice",
}

NO_SKIP_FIELDS: set[str] = set()

INCIDENT_SOURCE_CUSTOM_VALUE = "__CUSTOM_INCIDENT_SOURCE__"
INCIDENT_SOURCE_CUSTOM_FLAG = "_incident_source_custom_input"


def _set_incident_source_custom_mode(draft: Dict[str, Any], enabled: bool) -> None:
    if enabled:
        draft[INCIDENT_SOURCE_CUSTOM_FLAG] = True
    else:
        draft.pop(INCIDENT_SOURCE_CUSTOM_FLAG, None)


def _is_incident_source_custom_mode(draft: Dict[str, Any]) -> bool:
    return bool(draft.get(INCIDENT_SOURCE_CUSTOM_FLAG))

CHOICE_OPTIONS: Dict[str, List[Tuple[str, str]]] = {
    "incident_type": [(T["choices"]["incident_type"]["DTP"], "DTP"),
                      (T["choices"]["incident_type"]["BREAK"], "BREAK")],
    "incident_source": [
        (T["choices"]["incident_source"]["DRIVER_CALL"], "DRIVER_CALL"),
        (T["choices"]["incident_source"]["TELEOPS_REQUEST"], "TELEOPS_REQUEST"),
        (T["choices"]["incident_source"]["EXTERNAL_CALL"], "EXTERNAL_CALL"),
        (T["choices"]["incident_source"]["OTHER"], INCIDENT_SOURCE_CUSTOM_VALUE),
    ],
    "dtp_type": [(T["choices"]["dtp_type"]["COLLISION"], "COLLISION"),
                 (T["choices"]["dtp_type"]["ROLLOVER"], "ROLLOVER"),
                 (T["choices"]["dtp_type"]["RUNOVER"], "RUNOVER")],
    "brand": [(T["choices"]["brand"]["KIA_CEED"], "KIA_CEED"),
              (T["choices"]["brand"]["SITRAK"], "SITRAK")],
    "obstacle_on_road": [(T["buttons"]["yes"], "YES"),
                         (T["buttons"]["no"], "NO"),
                         (T["buttons"]["dont_know"], "UNKNOWN")],
    "ra_need_dtp_formalization": [(T["buttons"]["yes"], "YES"), (T["buttons"]["no"], "NO")],
    "ra_need_diagnosis": [(T["buttons"]["yes"], "YES"), (T["buttons"]["no"], "NO")],
    "ra_need_repair": [(T["buttons"]["yes"], "YES"), (T["buttons"]["no"], "NO")],
    "ra_need_evacuation": [(T["buttons"]["yes"], "YES"), (T["buttons"]["no"], "NO")],
    "ra_called_112": [(T["buttons"]["yes"], "YES"), (T["buttons"]["no"], "NO")],
}


def _allow_skip_field(key: str) -> bool:
    return key not in NO_SKIP_FIELDS


HUMANIZE_VALUE = {
    "incident_type": {"DTP": T["choices"]["incident_type"]["DTP"], "BREAK": T["choices"]["incident_type"]["BREAK"]},
    "incident_source": {
        "DRIVER_CALL": T["choices"]["incident_source"]["DRIVER_CALL"],
        "TELEOPS_REQUEST": T["choices"]["incident_source"]["TELEOPS_REQUEST"],
        "EXTERNAL_CALL": T["choices"]["incident_source"]["EXTERNAL_CALL"],
        "OTHER": T["choices"]["incident_source"]["OTHER"],
    },
    "dtp_type": {"COLLISION": T["choices"]["dtp_type"]["COLLISION"],
                 "ROLLOVER": T["choices"]["dtp_type"]["ROLLOVER"],
                 "RUNOVER": T["choices"]["dtp_type"]["RUNOVER"]},
    "brand": {"KIA_CEED": T["choices"]["brand"]["KIA_CEED"],
              "SITRAK": T["choices"]["brand"]["SITRAK"]},
    "yn": {"YES": T["buttons"]["yes"], "NO": T["buttons"]["no"]},
    "ynu": {"YES": T["buttons"]["yes"], "NO": T["buttons"]["no"], "UNKNOWN": T["buttons"]["dont_know"]},
}


def get_draft(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    d = context.user_data.setdefault("draft", {})
    d.setdefault("mode", PRIMARY_MODE)
    return d


def _steps_for_primary(ticket: Optional[Ticket]) -> List[str]:
    def _maybe_add_plate_ref(seq: List[str]) -> List[str]:
        seq = list(seq)
        if ticket and ticket.brand == "SITRAK":
            insert_after = seq.index("plate_vats") + 1 if "plate_vats" in seq else len(seq)
            seq.insert(insert_after, "plate_ref")
        return seq

    if ticket and ticket.incident_type == "DTP":
        steps = ["incident_type", "incident_source", "dtp_type", "brand", "plate_vats", "dtp_vehicles",
                 "location", "dtp_damage_text", "problem_desc", "obstacle_on_road", "notes"]
        return _maybe_add_plate_ref(steps)
    steps = ["incident_type", "incident_source", "brand", "plate_vats", "location", "break_symptoms",
             "problem_desc", "obstacle_on_road", "notes"]
    return _maybe_add_plate_ref(steps)


def _steps_for_ra(ticket: Optional[Ticket]) -> List[str]:
    if ticket and ticket.incident_type == "DTP":
        return ["ra_need_dtp_formalization", "ra_need_diagnosis", "ra_need_repair", "ra_need_evacuation", "ra_called_112"]
    # Поломка
    return ["ra_need_diagnosis", "ra_need_repair", "ra_need_evacuation"]


def active_steps(context: ContextTypes.DEFAULT_TYPE) -> List[str]:
    draft = get_draft(context)
    mode = draft.get("mode", PRIMARY_MODE)
    ticket: Ticket = draft.get("ticket")
    if mode == PRIMARY_MODE:
        return _steps_for_primary(ticket)
    if mode == RA_MODE:
        return _steps_for_ra(ticket)
    return _steps_for_primary(ticket)


def current_step_key(context: ContextTypes.DEFAULT_TYPE) -> str:
    draft = get_draft(context)
    idx = draft.get("step_idx", 0)
    steps = active_steps(context)
    idx = max(0, len(steps) - 1 if idx >= len(steps) else idx)
    draft["step_idx"] = idx
    return steps[idx]


def set_step_idx(context: ContextTypes.DEFAULT_TYPE, idx: int) -> None:
    draft = get_draft(context)
    steps = active_steps(context)
    draft["step_idx"] = max(0, min(idx, len(steps) - 1))


def goto_next_step(context: ContextTypes.DEFAULT_TYPE) -> None:
    draft = get_draft(context)
    steps = active_steps(context)
    draft["step_idx"] = min(draft.get("step_idx", 0) + 1, len(steps) - 1)


def goto_prev_step(context: ContextTypes.DEFAULT_TYPE) -> None:
    draft = get_draft(context)
    draft["step_idx"] = max(draft.get("step_idx", 0) - 1, 0)


def is_last_step(context: ContextTypes.DEFAULT_TYPE) -> bool:
    draft = get_draft(context)
    steps = active_steps(context)
    return draft.get("step_idx", 0) >= len(steps) - 1


def set_field_local(ticket: Ticket, key: str, val: Optional[Any]) -> None:
    # None в dtp_vehicles - только пустой словарь
    if key == "dtp_vehicles":
        ticket.dtp_vehicles = {} if val is None else val  # type: ignore[assignment]
        return
    setattr(ticket, key, val)

# =========================
# Клавиатуры / рендеры
# =========================


def kb_after_main_created(ticket: Ticket) -> InlineKeyboardMarkup:
    # После создания основной задачи - сразу RA или закрыть заявку
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(T["buttons"]["call_ra"], callback_data=f"act|ra|{ticket.id}")],
         [InlineKeyboardButton(T["buttons"]["close_all"], callback_data=f"act|close|{ticket.id}")]]
    )


def kb_after_ra(ticket: Ticket, invite_link: Optional[str]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(T["buttons"]["close_all"], callback_data=f"act|close|{ticket.id}")]]
    if ticket.ra_chat_id:
        rows.append([InlineKeyboardButton(T["dispatch"]["button_text"], callback_data=f"dispatch|{ticket.id}")])
    if invite_link:
        rows.append([InlineKeyboardButton(T["buttons"]["open_chat"], url=invite_link)])
    return InlineKeyboardMarkup(rows)


def kb_choice(step_key: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for text, val in CHOICE_OPTIONS.get(step_key, []):
        rows.append([InlineKeyboardButton(text, callback_data=f"set|{step_key}|{val}")])
    if step_key != "incident_type":
        if _allow_skip_field(step_key):
            rows.append([InlineKeyboardButton(T["buttons"]["skip"], callback_data=f"nav|skip|{step_key}")])
        rows.append([InlineKeyboardButton(T["buttons"]["back"], callback_data=f"nav|back|{step_key}")])
    return InlineKeyboardMarkup(rows)


def kb_nav(cur_key: str, *, back: bool = True, skip: Optional[bool] = None) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    show_skip = _allow_skip_field(cur_key) if skip is None else skip
    if show_skip:
        rows.append([InlineKeyboardButton(T["buttons"]["skip"], callback_data=f"nav|skip|{cur_key}")])
    if back:
        rows.append([InlineKeyboardButton(T["buttons"]["back"], callback_data=f"nav|back|{cur_key}")])
    return InlineKeyboardMarkup(rows or [])


def kb_summary(ticket_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(T["buttons"]["edit"], callback_data="summary|edit")],
         [InlineKeyboardButton(T["buttons"]["create_issue"], callback_data="summary|create")]]
    )


def human(val: Optional[str], key: str) -> str:
    if val is None or val == "":
        return T["common"]["empty"]
    return HUMANIZE_VALUE.get(key, {}).get(val, val)


def _veh_summary_line(v: Optional[Dict[str, int]]) -> str:
    v = v or {}
    names = T["vehicles"]
    order = ("light", "bus", "truck", "moto")
    parts = []
    for k in order:
        cnt = int(v.get(k, 0) or 0)
        if cnt > 0:
            parts.append(f"{names[k]}×{cnt}")
    return ", ".join(parts) if parts else T["common"]["empty"]


def kb_veh_counter(v: Dict[str, int]) -> InlineKeyboardMarkup:
    names = T['vehicles']
    rows: List[List[InlineKeyboardButton]] = []

    for key in ("light", "bus", "truck", "moto"):
        rows.append([
            InlineKeyboardButton(T['buttons']['vehicle_minus'].format(label=names[key]), callback_data=f"veh|minus|{key}"),
            InlineKeyboardButton(T['buttons']['vehicle_plus'].format(label=names[key]), callback_data=f"veh|plus|{key}"),
        ])
    rows.append([InlineKeyboardButton(T['buttons']['done'], callback_data='veh|done')])
    rows.append([InlineKeyboardButton(T['buttons']['skip'], callback_data='nav|skip|dtp_vehicles')])
    rows.append([InlineKeyboardButton(T['buttons']['back'], callback_data='nav|back|dtp_vehicles')])
    return InlineKeyboardMarkup(rows)


def _veh_counts_text(v: Optional[Dict[str, int]]) -> str:
    v = v or {}
    names = T["vehicles"]
    vals = {"light": v.get("light", 0), "bus": v.get("bus", 0), "truck": v.get("truck", 0), "moto": v.get("moto", 0)}
    parts = [
        f"{names['light']}: <b>{vals['light']}</b>",
        f"{names['bus']}: <b>{vals['bus']}</b>",
        f"{names['truck']}: <b>{vals['truck']}</b>",
        f"{names['moto']}: <b>{vals['moto']}</b>",
    ]
    return "\n".join(parts)


def _veh_step_text(v: Optional[Dict[str, int]]) -> str:
    v = v or {}
    hint = T["messages"]["dtp_vehicles_hint"] or ""
    return f"{T['questions']['dtp_vehicles']}\n\n{hint}\n\n{_veh_counts_text(v)}"


def _human_ynu_for_summary(val: Optional[str]) -> str:
    if val is None:
        return T["common"]["empty"]
    return HUMANIZE_VALUE["ynu"].get(val, T["common"]["empty"])


def render_primary_block(ticket: Ticket) -> str:
    lbl = T["primary_block_labels"]
    lines: List[str] = []

    def _append(label_text: str, value: str, *, condition: bool = True) -> None:
        if not condition:
            return
        safe_value = value if value else T["common"]["empty"]
        lines.append(f"{label_text}: <b>{safe_value}</b>")

    is_dtp = (ticket.incident_type == "DTP")
    _append(lbl["incident_type"], human(ticket.incident_type, "incident_type"))
    _append(lbl["incident_source"], human(ticket.incident_source, "incident_source"))
    # Динамический заголовок для времени
    time_label = lbl["incident_time"] if is_dtp else lbl["incident_time_break"]
    _append(time_label, ticket.incident_time or T["common"]["empty"])
    _append(lbl["dtp_type"], human(ticket.dtp_type, "dtp_type"), condition=is_dtp)
    _append(lbl["brand"], human(ticket.brand, "brand"))
    _append(lbl["plate_vats"], format_vats_display(ticket.plate_vats))
    _append(lbl["plate_ref"], format_ref_display(ticket.plate_ref), condition=(ticket.brand == "SITRAK"))
    # Далее - поля, полностью зависящие от ввода пользователя: экранируем HTML
    _append(lbl["location"], _safe_user_html(ticket.location))
    _append(lbl["dtp_vehicles"], _veh_summary_line(ticket.dtp_vehicles), condition=is_dtp)
    _append(lbl["dtp_damage"], _safe_user_html(ticket.dtp_damage_text), condition=is_dtp)
    _append(lbl["obstacle_on_road"], human(ticket.obstacle_on_road, "ynu"))
    _append(lbl["break_symptoms"], _safe_user_html(ticket.break_symptoms), condition=not is_dtp)
    _append(lbl["problem_desc"], _safe_user_html(ticket.problem_desc))
    _append(lbl["notes"], _safe_user_html(ticket.notes))
    return "\n".join(lines)


def render_preview_block(ticket: Ticket) -> str:
    return f"{T['common']['preview_label']}\n{render_primary_block(ticket)}"


def compose_primary_block(ticket: Ticket) -> str:
    return render_primary_block(ticket)


def compose_ra_block(ticket: Ticket) -> str:
    """
    Сводка RA по требованию:
    - Для ДТП: оформление ДТП, диагностика, ремонт, эвакуация, 112
    - Для поломки: диагностика, ремонт, эвакуация
    Везде показываем Да/Нет/Неизвестно/—.
    """
    labels = T["ra_summary_labels"]
    is_dtp = (ticket.incident_type == "DTP")

    def line(lbl_key: str, key: str) -> str:
        return f"{labels[lbl_key]}: <b>{_human_ynu_for_summary(getattr(ticket, key))}</b>"

    lines = [T["headings"]["ra_form"]]
    if is_dtp:
        lines.append(line("ra_need_dtp_formalization", "ra_need_dtp_formalization"))
    lines.append(line("ra_need_diagnosis", "ra_need_diagnosis"))
    lines.append(line("ra_need_repair", "ra_need_repair"))
    lines.append(line("ra_need_evacuation", "ra_need_evacuation"))
    if is_dtp:
        lines.append(line("ra_called_112", "ra_called_112"))

    return "\n".join(lines)


def render_after_main(ticket: Ticket) -> str:
    return (f"<b>{T['common']['ticket_title'].format(id=ticket.id)}</b>\n"
            f"Таск в Jira: <b>{ticket.jira_main or T['common']['empty']}</b>\n\n"
            f"{compose_primary_block(ticket)}")


def render_after_ra(ticket: Ticket) -> str:
    header_lines = [
        f"<b>{T['common']['ticket_title'].format(id=ticket.id)}</b>",
        f"Таск в Jira: <b>{ticket.jira_main or T['common']['empty']}</b>",
        "RA: <b>без сабтаска в Jira</b>",
    ]
    blocks = [compose_primary_block(ticket), compose_ra_block(ticket)]
    return "\n".join(header_lines) + "\n\n" + "\n\n".join(blocks)


def format_ra_chat_title(ticket: Ticket) -> str:
    brand_name = human(ticket.brand, "brand")
    timestamp = ticket.incident_time or current_request_time()
    return f"[Заявка #{ticket.id}] | [{brand_name}] | {timestamp}"


def render_jira_summary(ticket: Ticket) -> str:
    itype = HUMANIZE_VALUE.get("incident_type", {}).get(ticket.incident_type or "", ticket.incident_type or "")
    brand = HUMANIZE_VALUE.get("brand", {}).get(ticket.brand or "", ticket.brand or "")
    plate = format_vats_display(ticket.plate_vats)
    base = f"[{itype or '-'}] {brand or '-'}"
    if plate and plate != T["common"]["empty"]:
        base += f" — {plate}"
    return base

# =========================
# Jira helpers
# =========================


def _adf_text_node(text: str) -> dict:
    return {"type": "text", "text": text}


def _adf_doc_from_plain(text: Optional[str]) -> Optional[dict]:
    if not text:
        return None
    lines = (text or "").splitlines() or [text]
    content = [{"type": "paragraph", "content": [_adf_text_node(ln)]} for ln in lines]
    return {"type": "doc", "version": 1, "content": content}


def build_fields_main(ticket: Ticket) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        "project": {"key": JIRA_PROJECT_KEY},
        "summary": render_jira_summary(ticket),
        "labels": ["ptb", "auto-ticket"],
        "description": _adf_doc_from_plain(compose_primary_block(ticket)),
    }
    if JIRA_ISSUE_TYPE_MAIN_ID:
        fields["issuetype"] = {"id": JIRA_ISSUE_TYPE_MAIN_ID}
    else:
        fields["issuetype"] = {"name": JIRA_ISSUE_TYPE_MAIN}
    return fields


async def jira_create(fields: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    if not (JIRA_BASE_URL and JIRA_EMAIL and JIRA_API_TOKEN):
        return None, T["jira"]["config_missing"]
    url = f"{JIRA_BASE_URL}/rest/api/3/issue"
    timeout = httpx.Timeout(30.0, connect=15.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            logging.info("→ JIRA POST %s fields=%s", url, json.dumps(fields, ensure_ascii=False)[:2000])
            r = await client.post(url, json={"fields": fields}, auth=(JIRA_EMAIL, JIRA_API_TOKEN))
        except httpx.RequestError as e:
            return None, T["jira"]["network_error"].format(error=e)
    if r.status_code == 201:
        try:
            data = r.json()
            return data.get("key"), None
        except Exception:
            return None, T["jira"]["bad_json_created"].format(body=r.text[:500])
    return None, f"HTTP {r.status_code}: {r.text[:800]}"


async def jira_get_transitions(issue_key: str) -> Tuple[Optional[List[dict]], Optional[str]]:
    url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/transitions"
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            r = await client.get(url, auth=(JIRA_EMAIL, JIRA_API_TOKEN))
        except httpx.RequestError as e:
            return None, f"Сеть/подключение: {e!s}"
    if r.status_code == 200:
        try:
            data = r.json()
            return data.get("transitions") or [], None
        except Exception:
            return None, T["jira"]["bad_json_ok"].format(body=r.text[:500])
    return None, f"HTTP {r.status_code}: {r.text[:800]}"


async def jira_do_transition(issue_key: str, transition_id: str) -> Optional[str]:
    url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/transitions"
    payload = {"transition": {"id": transition_id}}
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            r = await client.post(url, json=payload, auth=(JIRA_EMAIL, JIRA_API_TOKEN))
        except httpx.RequestError as e:
            return f"Сеть/подключение: {e!s}"
    if r.status_code in (204, 200):
        return None
    return f"HTTP {r.status_code}: {r.text[:800]}"


async def close_issue_by_best_transition(issue_key: str) -> Optional[str]:
    prefs = set((T["jira"]["preferred_close_statuses"] or []))
    transitions, err = await jira_get_transitions(issue_key)
    if transitions is None:
        return err or "no transitions"
    best = None
    for tr in transitions:
        name = (tr.get("name") or "").strip()
        to = ((tr.get("to") or {}).get("name") or "").strip()
        if name in prefs or to in prefs:
            best = tr.get("id")
            break
    if not best and transitions:
        best = transitions[-1].get("id")
    if not best:
        return T["jira"]["no_transitions"]
    return await jira_do_transition(issue_key, best)

# =========================
# RA: создание чата и уведомление
# =========================

async def try_create_chat_via_userbot(subject: str, *, bot: Optional[Bot] = None) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    Унифицированный вызов фабрики чатов:
    - Пытается использовать _CHAT_FACTORY_ADAPTER.create_chat(...) или create_group_with_bot(...)
    - Если сигнатура метода требует bot_username — прокидываем MANAGED_BOT_USERNAME
    - Возвращаем (invite_link, error, chat_id). PTB дальше делает ссылку/ренейм.
    """
    global _CHAT_FACTORY_ADAPTER

    def _extract_chat_id(node: Any) -> Optional[int]:
        if isinstance(node, int):
            return node
        if isinstance(node, str):
            text = node.strip()
            if text.startswith("http"):
                return None
            if text.startswith("-") and text[1:].isdigit():
                try:
                    return int(text)
                except ValueError:
                    return None
            if text.isdigit():
                try:
                    return int(text)
                except ValueError:
                    return None
            return None
        if isinstance(node, dict):
            for key in ("chat_id", "chatId", "chatid", "id"):
                if key in node:
                    nested = _extract_chat_id(node[key])
                    if nested is not None:
                        return nested
            for key in ("chat", "result", "data"):
                if key in node:
                    nested = _extract_chat_id(node[key])
                    if nested is not None:
                        return nested
        return None

    adapter_method = None
    if _CHAT_FACTORY_ADAPTER:
        if hasattr(_CHAT_FACTORY_ADAPTER, "create_chat"):
            adapter_method = getattr(_CHAT_FACTORY_ADAPTER, "create_chat")
        elif hasattr(_CHAT_FACTORY_ADAPTER, "create_group_with_bot"):
            adapter_method = getattr(_CHAT_FACTORY_ADAPTER, "create_group_with_bot")

    if adapter_method:
        import inspect
        kwargs = {"title": subject}
        try:
            sig = inspect.signature(adapter_method)
            if "bot_username" in sig.parameters and MANAGED_BOT_USERNAME:
                kwargs["bot_username"] = f"@{MANAGED_BOT_USERNAME}"
        except (ValueError, TypeError):
            pass

        try:
            res = await adapter_method(**kwargs)
        except TypeError as e:
            if "bot_username" in str(e) and MANAGED_BOT_USERNAME and "unexpected keyword" not in str(e):
                try:
                    res = await adapter_method(title=subject, bot_username=f"@{MANAGED_BOT_USERNAME}")
                except Exception as ee:
                    logging.warning("userbot create_chat retry with bot_username failed: %s", ee)
                    return None, f"userbot create_chat error: {ee!s}", None
            else:
                logging.warning("userbot create_chat error: %s", e)
                return None, f"userbot create_chat error: {e!s}", None
        except Exception as e:
            logging.warning("userbot create_chat failed: %s", e)
            return None, f"userbot create_chat error: {e!s}", None

        chat_id = _extract_chat_id(res)
        if isinstance(res, str) and res.startswith("http"):
            return res, None, chat_id
        return None, None, chat_id

    return None, "userbot disabled", None


async def notify_help_chat(context: ContextTypes.DEFAULT_TYPE, invite_link: Optional[str]) -> None:
    if not RA_NOTIFY_CHAT_ID:
        return
    if invite_link:
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(T["buttons"]["open_chat"], url=invite_link)]])
        await context.bot.send_message(chat_id=RA_NOTIFY_CHAT_ID, text=T["common"]["help_needed"], reply_markup=markup)
    else:
        await context.bot.send_message(chat_id=RA_NOTIFY_CHAT_ID, text=T["common"]["help_needed"])


async def lock_ra_chat_messages(ticket: Ticket, bot: Bot) -> None:
    chat_id = ticket.ra_chat_id
    if not chat_id:
        return
    permissions = ChatPermissions(
        can_send_messages=False,
        can_send_audios=False,
        can_send_documents=False,
        can_send_photos=False,
        can_send_videos=False,
        can_send_video_notes=False,
        can_send_voice_notes=False,
        can_send_polls=False,
        can_send_other_messages=False,
        can_add_web_page_previews=False,
        can_change_info=False,
        can_invite_users=False,
        can_pin_messages=False,
        can_manage_topics=False,
    )
    try:
        await bot.set_chat_permissions(chat_id=chat_id, permissions=permissions)
    except TelegramError as e:
        logging.warning("failed to lock RA chat %s: %s", chat_id, e)


async def finish_ra_flow_if_needed(query, context, draft, ticket) -> bool:
    if draft.get("mode") != RA_MODE or not is_last_step(context):
        return False

    # Завершение RA режима
    draft["mode"] = PRIMARY_MODE
    draft["step_idx"] = 0

    invite_link: Optional[str] = None
    created_chat_id: Optional[int] = None
    subject = T["messages"]["ra_subject"].format(ticket_id=ticket.id)
    link, err, chat_id = await try_create_chat_via_userbot(subject, bot=context.bot)
    if chat_id:
        created_chat_id = chat_id
        ticket.ra_chat_id = chat_id

    if created_chat_id:
        try:
            inv = await context.bot.create_chat_invite_link(chat_id=created_chat_id, creates_join_request=False)
            invite_link = inv.invite_link
        except TelegramError as e:
            logging.warning("failed to create invite link for chat %s: %s", created_chat_id, e)
            invite_link = None
        try:
            await context.bot.set_chat_title(chat_id=created_chat_id, title=format_ra_chat_title(ticket))
        except TelegramError as e:
            logging.warning("failed to rename RA chat %s: %s", created_chat_id, e)
        try:
            await context.bot.send_message(chat_id=created_chat_id, text=render_after_ra(ticket), disable_web_page_preview=True)
        except TelegramError as e:
            logging.warning("failed to post RA summary to chat %s: %s", created_chat_id, e)
    else:
        logging.warning("RA chat was not created by userbot: %s", err or "no details")

    await notify_help_chat(context, invite_link)
    await safe_edit_message_text(query, context, text=render_after_ra(ticket), reply_markup=kb_after_ra(ticket, invite_link))
    if DISPATCH_ALERT_CHAT_ID:
        try:
            header = [
                f"<b>{T['common']['ticket_title'].format(id=ticket.id)}</b>",
                f"Таск в Jira: <b>{ticket.jira_main or T['common']['empty']}</b>",
            ]
            body = f"{T['dispatch']['ra_notice']}\n" + "\n".join(header) + "\n\n" + compose_ra_block(ticket)
            await context.bot.send_message(
                chat_id=DISPATCH_ALERT_CHAT_ID,
                text=body,
                disable_web_page_preview=True,
            )
        except TelegramError as e:
            logging.warning("failed to send RA notice: %s", e)
    return True

# =========================
# Черновик и шаги
# =========================


async def start_new_draft(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Ticket:
    draft = get_draft(context)
    _set_incident_source_custom_mode(draft, False)
    user = update.effective_user
    user_id = user.id if user else 0
    username = user.username if user else None
    ticket = Ticket(
        id=short_id(),
        user_id=user_id,
        username=username,
        created_at=iso(utc_now()),
        incident_time=current_request_time(),
    )
    draft["ticket"] = ticket
    draft["step_idx"] = 0
    draft["editing"] = False
    draft["mode"] = PRIMARY_MODE
    return ticket


def question_text_for(step_key: str) -> str:
    return T["questions"][step_key]


async def ask_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = current_step_key(context)
    prompt = question_text_for(key)
    kind = STEP_INPUT_KIND[key]
    chat = update.effective_chat
    if chat is None:
        return

    if kind == "choice":
        await context.bot.send_message(chat.id, prompt, reply_markup=kb_choice(key))
        return
    if kind == "counter":
        ticket: Ticket = get_draft(context)["ticket"]
        await context.bot.send_message(
            chat.id,
            _veh_step_text(ticket.dtp_vehicles),
            reply_markup=kb_veh_counter(ticket.dtp_vehicles),
        )
        return
    await context.bot.send_message(chat.id, prompt, reply_markup=kb_nav(cur_key=key, back=True))

# =========================
# Хэндлеры
# =========================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type != ChatType.PRIVATE:
        return
    await start_new_draft(update, context)
    await context.bot.send_message(
        chat.id, T["questions"]["incident_type"], reply_markup=kb_choice("incident_type")
    )


async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if not chat:
        return
    text = [
        f"Chat ID: <code>{chat.id}</code>",
        f"Chat type: <b>{chat.type}</b>",
    ]
    if user:
        text.append(f"User ID: <code>{user.id}</code>")
    if chat.type == ChatType.SUPERGROUP and getattr(chat, "linked_chat_id", None):
        text.append(f"Linked chat ID: <code>{chat.linked_chat_id}</code>")
    await context.bot.send_message(chat.id, "\n".join(text), parse_mode=ParseMode.HTML)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type != ChatType.PRIVATE:
        return
    draft = get_draft(context)
    if "ticket" not in draft:
        await cmd_start(update, context)
        return
    ticket: Ticket = draft["ticket"]
    mode = draft.get("mode", PRIMARY_MODE)
    key = current_step_key(context)
    kind = STEP_INPUT_KIND[key]
    msg = update.message
    text = (msg.text or "").strip() if msg else ""
    custom_incident_source = key == "incident_source" and _is_incident_source_custom_mode(draft)

    if kind == "plate":
        norm = normalize_vats_plate(text, brand=ticket.brand)
        if not norm:
            await context.bot.send_message(chat.id, T["errors"]["plate_invalid"], reply_markup=kb_nav(cur_key=key, back=True))
            return
        set_field_local(ticket, key, norm)
        if (
            key == "plate_vats"
            and draft.get("editing")
            and mode == PRIMARY_MODE
            and ticket.brand == "SITRAK"
            and not ticket.plate_ref
        ):
            steps = active_steps(context)
            if "plate_ref" in steps:
                set_step_idx(context, steps.index("plate_ref"))
                await context.bot.send_message(
                    chat.id,
                    question_text_for("plate_ref"),
                    reply_markup=kb_nav(cur_key="plate_ref", back=True, skip=False),
                )
                return
    elif kind == "plate_ref":
        norm = normalize_ref_plate(text)
        if not norm:
            await context.bot.send_message(chat.id, T["errors"]["plate_ref_invalid"], reply_markup=kb_nav(cur_key=key, back=True))
            return
        set_field_local(ticket, key, norm)
    elif kind == "text" or custom_incident_source:
        if not text:
            await context.bot.send_message(chat.id, T["errors"]["empty_text"], reply_markup=kb_nav(cur_key=key, back=True))
            return
        set_field_local(ticket, key, text)
        if custom_incident_source:
            _set_incident_source_custom_mode(draft, False)
    else:
        return

    if draft.get("editing") and mode == PRIMARY_MODE:
        draft["editing"] = False
        await context.bot.send_message(chat.id, render_preview_block(ticket), reply_markup=kb_summary(ticket.id))
        return

    if is_last_step(context):
        if mode == PRIMARY_MODE:
            await context.bot.send_message(chat.id, render_preview_block(ticket), reply_markup=kb_summary(ticket.id))
            return
        return

    goto_next_step(context)
    next_key = current_step_key(context)
    if STEP_INPUT_KIND[next_key] == "choice":
        await context.bot.send_message(chat.id, question_text_for(next_key), reply_markup=kb_choice(next_key))
    elif STEP_INPUT_KIND[next_key] == "counter":
        await context.bot.send_message(
            chat.id,
            _veh_step_text(ticket.dtp_vehicles),
            reply_markup=kb_veh_counter(ticket.dtp_vehicles),
        )
    else:
        await context.bot.send_message(chat.id, question_text_for(next_key), reply_markup=kb_nav(cur_key=next_key, back=True))


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat or chat.type != ChatType.PRIVATE:
        return
    query = update.callback_query
    if not query:
        return
    await query.answer()
    data = query.data or ""
    draft = get_draft(context)
    if "ticket" not in draft:
        await cmd_start(update, context)
        return
    ticket: Ticket = draft["ticket"]

    if data.startswith("veh|"):
        _, action, kind = (data.split("|") + ["", ""])[:3]
        if action in ("plus", "minus"):
            cur = ticket.dtp_vehicles.get(kind, 0)
            ticket.dtp_vehicles[kind] = max(0, cur + (1 if action == "plus" else -1))
            await safe_edit_message_text(
                query, context,
                text=_veh_step_text(ticket.dtp_vehicles),
                reply_markup=kb_veh_counter(ticket.dtp_vehicles),
            )
            return
        if action == "done":
            if draft.get("editing") and draft.get("mode") == PRIMARY_MODE:
                draft["editing"] = False
                await safe_edit_message_text(
                    query, context,
                    text=render_preview_block(ticket),
                    reply_markup=kb_summary(ticket.id),
                )
                return
            if is_last_step(context) and draft.get("mode") == PRIMARY_MODE:
                await safe_edit_message_text(query, context, text=render_preview_block(ticket), reply_markup=kb_summary(ticket.id))
                return
            goto_next_step(context)
            next_key = current_step_key(context)
            await safe_edit_message_text(
                query, context,
                text=question_text_for(next_key) if STEP_INPUT_KIND[next_key] != "counter" else _veh_step_text(ticket.dtp_vehicles),
                reply_markup=kb_choice(next_key) if STEP_INPUT_KIND[next_key] == "choice" else (
                    kb_veh_counter(ticket.dtp_vehicles) if STEP_INPUT_KIND[next_key] == "counter" else kb_nav(cur_key=next_key, back=True)
                ),
            )
            return

    if data.startswith("nav|"):
        parts = data.split("|")
        action = parts[1] if len(parts) > 1 else ""
        cur_key = parts[2] if len(parts) > 2 else current_step_key(context)

        if action == "back":
            if draft.get("editing") and draft.get("mode") == PRIMARY_MODE:
                draft["editing"] = False
                await safe_edit_message_text(query, context, text=render_preview_block(ticket), reply_markup=kb_summary(ticket.id))
                return
            if cur_key == "incident_source":
                _set_incident_source_custom_mode(draft, False)
            steps = active_steps(context)
            if cur_key in steps:
                set_step_idx(context, max(0, steps.index(cur_key) - 1))
            else:
                goto_prev_step(context)
            next_key = current_step_key(context)
            await safe_edit_message_text(
                query, context,
                text=question_text_for(next_key) if STEP_INPUT_KIND[next_key] != "counter" else _veh_step_text(ticket.dtp_vehicles),
                reply_markup=kb_choice(next_key) if STEP_INPUT_KIND[next_key] == "choice" else (
                    kb_veh_counter(ticket.dtp_vehicles) if STEP_INPUT_KIND[next_key] == "counter" else kb_nav(cur_key=next_key, back=True)
                ),
            )
            return

        if action == "skip":
            # Корректно "обнуляем" поле
            if cur_key == "dtp_vehicles":
                ticket.dtp_vehicles = {}
            else:
                set_field_local(ticket, cur_key, None)
            if cur_key == "incident_source":
                _set_incident_source_custom_mode(draft, False)

            # Если это последний шаг RA, то завершаем RA флоу сразу
            if await finish_ra_flow_if_needed(query, context, draft, ticket):
                return

            if draft.get("editing") or is_last_step(context):
                await safe_edit_message_text(query, context, text=render_preview_block(ticket), reply_markup=kb_summary(ticket.id))
                return
            goto_next_step(context)
            next_key = current_step_key(context)
            await safe_edit_message_text(
                query, context,
                text=question_text_for(next_key) if STEP_INPUT_KIND[next_key] != "counter" else _veh_step_text(ticket.dtp_vehicles),
                reply_markup=kb_choice(next_key) if STEP_INPUT_KIND[next_key] == "choice" else (
                    kb_veh_counter(ticket.dtp_vehicles) if STEP_INPUT_KIND[next_key] == "counter" else kb_nav(cur_key=next_key, back=True)
                ),
            )
            return

    if data.startswith("set|"):
        _, field_key, value = data.split("|", 2)
        if field_key not in ALL_KNOWN_KEYS:
            return

        if field_key == "incident_source":
            if value == INCIDENT_SOURCE_CUSTOM_VALUE:
                _set_incident_source_custom_mode(draft, True)
                await safe_edit_message_text(
                    query,
                    context,
                    text=T["messages"]["incident_source_other_prompt"],
                    reply_markup=kb_nav(cur_key=field_key, back=True),
                )
                return
            _set_incident_source_custom_mode(draft, False)

        prev_brand = ticket.brand if field_key == "brand" else None
        set_field_local(ticket, field_key, value)
        brand_changed = field_key == "brand" and prev_brand is not None and prev_brand != value
        if brand_changed:
            ticket.plate_vats = None
            ticket.plate_ref = None

        # Если при редактировании бренд уже Sitrak и ПП ещё не указан — запросим plate_ref (без "Не указывать")
        if (
            not brand_changed
            and draft.get("editing")
            and draft.get("mode", PRIMARY_MODE) == PRIMARY_MODE
            and field_key == "brand"
            and value == "SITRAK"
            and not ticket.plate_ref
        ):
            steps = active_steps(context)
            if "plate_ref" in steps:
                set_step_idx(context, steps.index("plate_ref"))
                fk = "plate_ref"
                await safe_edit_message_text(
                    query, context,
                    text=question_text_for(fk),
                    reply_markup=kb_nav(cur_key=fk, back=True, skip=False),
                )
                return

        if brand_changed:
            steps = active_steps(context)
            if "plate_vats" in steps:
                set_step_idx(context, steps.index("plate_vats"))
                await safe_edit_message_text(
                    query,
                    context,
                    text=question_text_for("plate_vats"),
                    reply_markup=kb_nav(cur_key="plate_vats", back=True),
                )
            return

        if field_key in ("incident_type", "brand"):
            steps = active_steps(context)
            cur = current_step_key(context)
            if cur not in steps:
                set_step_idx(context, 0)

        mode = draft.get("mode", PRIMARY_MODE)
        was_current = (current_step_key(context) == field_key)
        was_last = was_current and is_last_step(context)

        if draft.get("editing") and mode == PRIMARY_MODE:
            draft["editing"] = False
            await safe_edit_message_text(query, context, text=render_preview_block(ticket), reply_markup=kb_summary(ticket.id))
            return

        if was_last:
            if mode == PRIMARY_MODE:
                await safe_edit_message_text(query, context, text=render_preview_block(ticket), reply_markup=kb_summary(ticket.id))
                return
            if mode == RA_MODE:
                if await finish_ra_flow_if_needed(query, context, draft, ticket):
                    return

        if was_current:
            goto_next_step(context)
        next_key = current_step_key(context)
        await safe_edit_message_text(
            query, context,
            text=question_text_for(next_key) if STEP_INPUT_KIND[next_key] != "counter" else _veh_step_text(ticket.dtp_vehicles),
            reply_markup=kb_choice(next_key) if STEP_INPUT_KIND[next_key] == "choice" else (
                kb_veh_counter(ticket.dtp_vehicles) if STEP_INPUT_KIND[next_key] == "counter" else kb_nav(cur_key=next_key, back=True)
            ),
        )
        return

    if data == "summary|edit":
        await safe_edit_message_text(query, context, text=T["form_edit_title"], reply_markup=kb_edit_field_list(context))
        return

    if data == "edit|cancel":
        await safe_edit_message_text(query, context, text=render_preview_block(ticket), reply_markup=kb_summary(ticket.id))
        return

    if data.startswith("edit|field|"):
        _, _, field_key = data.split("|", 2)
        if field_key not in active_steps(context):
            return
        draft["editing"] = True
        draft["mode"] = PRIMARY_MODE
        set_step_idx(context, active_steps(context).index(field_key))
        fk = field_key
        await safe_edit_message_text(
            query, context,
            text=question_text_for(fk) if STEP_INPUT_KIND[fk] != "counter" else _veh_step_text(ticket.dtp_vehicles),
            reply_markup=kb_choice(fk) if STEP_INPUT_KIND[fk] == "choice" else (
                kb_veh_counter(ticket.dtp_vehicles) if STEP_INPUT_KIND[fk] == "counter" else kb_nav(cur_key=fk, back=True)
            ),
        )
        return

    if data.startswith("dispatch|"):
        _, t_id = data.split("|", 1)
        if t_id != ticket.id:
            return
        if not DISPATCH_ALERT_CHAT_ID or not ticket.ra_chat_id:
            await safe_edit_message_text(query, context, text=T["dispatch"]["missing_chat"], reply_markup=None)
            return
        try:
            invite = await context.bot.create_chat_invite_link(chat_id=ticket.ra_chat_id, creates_join_request=False)
            markup = InlineKeyboardMarkup([[InlineKeyboardButton(T["buttons"]["open_chat"], url=invite.invite_link)]])
            await context.bot.send_message(
                chat_id=DISPATCH_ALERT_CHAT_ID,
                text=T["dispatch"]["help_message"],
                reply_markup=markup,
            )
            await context.bot.send_message(query.message.chat.id, T["dispatch"]["sent"])
        except TelegramError as e:
            logging.warning("failed to notify dispatcher: %s", e)
            await safe_edit_message_text(query, context, text=f"⚠️ {_html_escape(str(e))}", reply_markup=None)
            await remove_dispatch_button(query, context, ticket_id=ticket.id)
            return
        await remove_dispatch_button(query, context, ticket_id=ticket.id)
        return

    if data == "summary|create":
        await safe_edit_message_text(query, context, text=T["progress"]["creating_issue"], reply_markup=None)
        fields_main = build_fields_main(ticket)
        jira_key, jira_err = await jira_create(fields_main)
        if jira_key:
            ticket.jira_main = jira_key
            # После создания основной задачи - сразу показываем сводку и две кнопки: RA / Закрыть
            await safe_edit_message_text(query, context, text=render_after_main(ticket), reply_markup=kb_after_main_created(ticket))
            if DISPATCH_ALERT_CHAT_ID:
                try:
                    header = [
                        f"<b>{T['common']['ticket_title'].format(id=ticket.id)}</b>",
                        f"Таск в Jira: <b>{ticket.jira_main or T['common']['empty']}</b>",
                    ]
                    body = f"{T['dispatch']['delay_notice']}\n" + "\n".join(header) + "\n\n" + render_primary_block(ticket)
                    await context.bot.send_message(
                        chat_id=DISPATCH_ALERT_CHAT_ID,
                        text=body,
                        disable_web_page_preview=True,
                    )
                except TelegramError as e:
                    logging.warning("failed to send delay notice: %s", e)
        else:
            safe_err = jira_err or T["common"]["jira_create_failed"]
            await safe_edit_message_text(query, context, text=f"⚠️ {_html_escape(safe_err)}", reply_markup=kb_summary(ticket.id))
        return

    if data.startswith("act|"):
        _, action, t_id = data.split("|", 2)
        if t_id != ticket.id:
            return

        if action == "ra":
            if not ticket.jira_main:
                await safe_edit_message_text(query, context, text=T["messages"]["need_main_before_ra"], reply_markup=kb_after_main_created(ticket))
                return
            # Входим в RA опрос
            draft["mode"] = RA_MODE
            draft["step_idx"] = 0
            first_key = current_step_key(context)
            await safe_edit_message_text(query, context, text=question_text_for(first_key), reply_markup=kb_choice(first_key))
            return

        if action == "close":
            await safe_edit_message_text(query, context, text=T["progress"]["closing_issue"], reply_markup=None)
            if not ticket.jira_main:
                await safe_edit_message_text(query, context, text=T["jira"]["need_main_issue"], reply_markup=None)
                return
            err = await close_issue_by_best_transition(ticket.jira_main)
            if err:
                # Экранируем текст ошибки, чтобы Jira не могла «сломать» HTML-разметку
                await safe_edit_message_text(query, context, text=_html_escape(err or "close error"), reply_markup=None)
            else:
                await lock_ra_chat_messages(ticket, context.bot)
                await safe_edit_message_text(query, context, text=T["common"]["issue_closed"], reply_markup=None)
            return

# =========================
# Error handler и вспомогательное
# =========================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception while processing update: %s", update)


def kb_edit_field_list(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for key in active_steps(context):
        rows.append([InlineKeyboardButton(T["questions"][key], callback_data=f"edit|field|{key}")])
    rows.append([InlineKeyboardButton(T["back_to_summary"], callback_data="edit|cancel")])
    return InlineKeyboardMarkup(rows)

# =========================
# Сборка и запуск PTB
# =========================


def build_app() -> Application:
    request = HTTPXRequest(connect_timeout=30.0, read_timeout=70.0, write_timeout=30.0, pool_timeout=70.0)
    defaults = Defaults(parse_mode=ParseMode.HTML)

    async def _post_init(app: Application) -> None:
        # Зарезервировано под инициализацию (health-check, кэш, и т.д.)
        pass

    app = (ApplicationBuilder().token(BOT_TOKEN).request(request).defaults(defaults).post_init(_post_init).build())
    app.add_handler(CommandHandler("start", cmd_start, filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)
    return app


def build_application(*, chat_factory=None) -> Application:
    global _CHAT_FACTORY_ADAPTER
    _CHAT_FACTORY_ADAPTER = chat_factory
    if not BOT_TOKEN:
        raise SystemExit(T["messages"]["missing_bot_token"])
    return build_app()


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit(T["messages"]["missing_bot_token"])
    application = build_app()
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
