from __future__ import annotations
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, List

import httpx
from dotenv import load_dotenv
from regular_bot import TEXTS as T

load_dotenv()

# =========================
# Конфиг Jira
# =========================

JIRA_BASE_URL = os.getenv("JIRA_BASE_URL", "").rstrip("/")
JIRA_EMAIL = os.getenv("JIRA_EMAIL", "")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN", "")
JIRA_PROJECT_KEY = os.getenv("JIRA_PROJECT_KEY", "")

JIRA_ISSUE_TYPE_MAIN_ID = os.getenv("JIRA_ISSUE_TYPE_MAIN_ID", "")
JIRA_ISSUE_TYPE_MAIN = os.getenv("JIRA_ISSUE_TYPE_MAIN", "Task")

# =========================
# Утилиты
# =========================

def format_jira_error(status: int, body_text: str) -> str:
    lines = [f"HTTP {status}"]
    text = (body_text or "").strip()
    try:
        data = json.loads(text) if text else {}
    except json.JSONDecodeError:
        data = None
    if isinstance(data, dict):
        messages = data.get("errorMessages")
        if isinstance(messages, list):
            lines += [f"  - {m}" for m in messages]
        field_errors = data.get("errors")
        if isinstance(field_errors, dict):
            lines += [f"  - {f}: {msg}" for f, msg in field_errors.items()]
    elif text:
        lines.append(text[:1500])
    return "\n".join(lines)

def _jira_net_error(exc: Exception) -> str:
    return T["jira"]["network_error"].format(error=exc)

# =========================
# REST: основные вызовы
# =========================

async def jira_create(fields: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Создать задачу в Jira."""
    if not (JIRA_BASE_URL and JIRA_EMAIL and JIRA_API_TOKEN):
        return None, T["jira"]["config_missing"]

    url = f"{JIRA_BASE_URL}/rest/api/3/issue"
    timeout = httpx.Timeout(30.0, connect=15.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.post(url, json={"fields": fields}, auth=(JIRA_EMAIL, JIRA_API_TOKEN))
        except httpx.RequestError as e:
            return None, _jira_net_error(e)
    if resp.status_code == 201:
        try:
            return resp.json().get("key"), None
        except Exception:
            return None, T["jira"]["bad_json_created"].format(body=resp.text[:300])
    return None, format_jira_error(resp.status_code, resp.text)

async def jira_update_fields(issue_key: str, patch_fields: Dict[str, Any]) -> Optional[str]:
    """Обновить поля задачи."""
    url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}"
    timeout = httpx.Timeout(30.0, connect=15.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.put(url, json={"fields": patch_fields}, auth=(JIRA_EMAIL, JIRA_API_TOKEN))
        except httpx.RequestError as e:
            return _jira_net_error(e)
    if resp.status_code in (200, 204):
        return None
    return format_jira_error(resp.status_code, resp.text)

async def jira_get_issue_basic(issue_key: str) -> Tuple[Optional[dict], Optional[str]]:
    """Прочитать базовую информацию о задаче (например, проект)."""
    url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}"
    params = {"fields": "project"}
    timeout = httpx.Timeout(30.0, connect=15.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.get(url, params=params, auth=(JIRA_EMAIL, JIRA_API_TOKEN))
        except httpx.RequestError as e:
            return None, _jira_net_error(e)
    if resp.status_code == 200:
        try:
            return resp.json(), None
        except Exception:
            return None, T["jira"]["bad_json_ok"].format(body=resp.text[:300])
    return None, format_jira_error(resp.status_code, resp.text)

async def jira_get_transitions(issue_key: str) -> Tuple[List[dict], Optional[str]]:
    """Получить доступные переходы."""
    url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/transitions"
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.get(url, auth=(JIRA_EMAIL, JIRA_API_TOKEN))
        except httpx.RequestError as e:
            return [], _jira_net_error(e)
    if resp.status_code == 200:
        try:
            data = resp.json()
            return data.get("transitions", []), None
        except Exception:
            return [], T["jira"]["bad_json_ok"].format(body=resp.text[:300])
    return [], format_jira_error(resp.status_code, resp.text)

async def jira_do_transition(issue_key: str, transition_id: str) -> Optional[str]:
    """Выполнить переход (закрытие задачи)."""
    url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/transitions"
    payload = {"transition": {"id": transition_id}}
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.post(url, json=payload, auth=(JIRA_EMAIL, JIRA_API_TOKEN))
        except httpx.RequestError as e:
            return _jira_net_error(e)
    if resp.status_code in (200, 204):
        return None
    return format_jira_error(resp.status_code, resp.text)

async def jira_close_issue(issue_key: str) -> Optional[str]:
    """Закрывает задачу (ищет лучший переход)."""
    transitions, err = await jira_get_transitions(issue_key)
    if err:
        return err
    prefs = set(T["jira"]["preferred_close_statuses"])
    best = None
    for tr in transitions:
        name = (tr.get("name") or "").strip()
        to_name = ((tr.get("to") or {}).get("name") or "").strip()
        if name in prefs or to_name in prefs:
            best = tr.get("id")
            break
    if not best and transitions:
        best = transitions[-1].get("id")
    if not best:
        return T["jira"]["no_transitions"]
    return await jira_do_transition(issue_key, best)
