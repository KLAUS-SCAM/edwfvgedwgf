import json
from typing import Any, List
from core.db import fetchrow, execute, fetch

SETTINGS_TABLE = "settings_kv"

async def kv_set(key: str, value: Any):
    """Сохранить настройку (key=value)"""
    payload = json.dumps(value)
    await execute(
        f"""insert into {SETTINGS_TABLE}(key, value)
            values ($1, $2::jsonb)
            on conflict (key)
            do update set value=$2::jsonb, updated_at=now()""",
        key, payload
    )

async def kv_get(key: str, default=None):
    """Получить настройку по ключу"""
    row = await fetchrow(f"select value from {SETTINGS_TABLE} where key=$1", key)
    if not row:
        return default
    return row["value"]

# === Клиенты для рассылки ===
async def iter_client_ids() -> List[int]:
    """
    Возвращает список Telegram ID клиентов для рассылки.
    По умолчанию берёт из таблицы settings_kv['client_ids'].
    Если позже будет таблица users — подхватим оттуда.
    """
    ids = await kv_get("client_ids")
    if isinstance(ids, list) and ids:
        return [int(x) for x in ids]

    # пример если позже подключим таблицу клиентов:
    # rows = await fetch("select tg_id from users where role='client' and is_active=true")
    # return [r["tg_id"] for r in rows]

    return []
