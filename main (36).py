import os, asyncio, logging, secrets
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Tuple

import asyncpg, orjson
from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, F
from aiogram import Router
from aiogram.enums import ParseMode, ChatType
from aiogram import filters
from aiogram.types import (
    Message, Update, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, BotCommand
)
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramForbiddenError
from collections import defaultdict
from core.db import get_pool, ensure_schema

# -------------------- ENV --------------------
BOT_TOKEN         = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID     = int(os.getenv("ADMIN_CHAT_ID"))
OPERATORS_CHAT_ID = int(os.getenv("OPERATORS_CHAT_ID"))
FALLBACK_CHAT     = int(os.getenv("FALLBACK_CHAT_ID", str(ADMIN_CHAT_ID)))
WEBHOOK_BASE      = os.getenv("WEBHOOK_BASE", "").rstrip("/")
WEBHOOK_PATH      = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET", secrets.token_hex(16))
POSTGRES_DSN      = os.getenv("POSTGRES_DSN")
OWNER_TG_ID       = int(os.getenv("OWNER_TG_ID", "0"))
POWER_ADMINS      = {
    int(x) for x in os.getenv("POWER_ADMINS", str(OWNER_TG_ID)).split() if x.strip()
}

SLA_WAIT             = int(os.getenv("SLA_WAIT_SECONDS", "300"))
SLA_REPORT_EVERY_MIN = int(os.getenv("SLA_REPORT_EVERY_MIN", "30"))
BROADCAST_RATE       = int(os.getenv("BROADCAST_RATE", "20"))
SUPPORT_LINK         = os.getenv("SUPPORT_LINK", "")
HOWTO_LINK           = os.getenv("HOWTO_LINK", "")

# -------------------- TOPIC NAMES --------------------
NOT_ANSWERED_TOPIC_NAME = "❗Неотвеченные"
BROADCAST_TOPIC_NAME    = "📣 Рассылки"
REFERRALS_TOPIC_NAME    = "💰 Рефералы"
PANEL_TOPIC_NAME        = "⚙️ Панель управления"
LOGS_TOPIC_NAME        = "🧷 Логи"
NUMBERS_TOPIC_NAME     = "📞 Номера клиентов"
DIAG_TOPIC_NAME        = "🧪 Диагностика"

# -------------------- APP/AIROGRAM --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("fastbot")

app = FastAPI()
from loader import bot, dp

pool: Optional[asyncpg.Pool] = None

# Глобальная клиентская клавиатура (переиспользуемая)
client_kb = ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[])

# -------------------- CLIENT KEYBOARD --------------------
async def refresh_client_kb_from_settings():
    """Кнопки у клиента — из настроек (+ новая «Сдать номер»)."""
    ref = await get_setting("btn_ref_text", "💰 Заработать")
    how = await get_setting("btn_howto_text", "📜 Как получить 200 ₽")
    sup = await get_setting("btn_support_text", "🆘 Поддержка")
    sub = await get_setting("btn_submit_text", "📞 Сдать номер")  # NEW

    client_kb.keyboard = [
        [KeyboardButton(text=ref)],
        [KeyboardButton(text=how)],
        [KeyboardButton(text=sup)],
        [KeyboardButton(text=sub)],  # NEW
    ]


# -------------------- STATE --------------------
broadcast_states: Dict[int, dict] = {}  # для ПРО-рассылки (предпросмотр/подтверждение)
panel_states: Dict[int, dict] = {}      # зарезервировано под панель
last_action_prompt: Dict[int, Tuple[int, datetime]] = {}
active_broadcasts: Dict[int, dict] = {}  # {admin_id: {"paused": bool, "stopped": bool}}
await_number_until: Dict[int, datetime] = {}   # {user_id: deadline_utc}
user_last_number_message: Dict[int, int] = {}  # {user_id: msg_id в операторской теме}

# -------------------- HELPERS --------------------
def normalize_phone(raw: str) -> Optional[str]:
    """
    Нормализуем телефон под формат +7XXXXXXXXXX
    Принимаем: '8 999 123-45-67', '+7 (999) 1234567', '9991234567', '7 999 ...'
    Возвращаем 11 цифр с префиксом +7 или None, если не похоже на номер.
    """
    if not raw:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 11 and digits[0] in ("7", "8"):
        return "+7" + digits[1:]
    if len(digits) == 10 and digits[0] in "9":
        return "+7" + digits
    return None


async def send_log(text: str):
    """Пишем в тему логов, если есть. Иначе в FALLBACK_CHAT."""
    try:
        tid = await get_topic_id("logs_topic")
        if tid:
            await bot.send_message(ADMIN_CHAT_ID, f"🧷 {text}", message_thread_id=tid)
        else:
            await bot.send_message(FALLBACK_CHAT, f"🧷 {text}")
    except Exception:
        pass

async def send_to_numbers_topic_text(text: str):
    """Отправка строки в тему '📞 Номера клиентов' (если она есть)."""
    try:
        tid = await get_topic_id("numbers_topic")
        if tid:
            await bot.send_message(ADMIN_CHAT_ID, text, message_thread_id=tid)
    except Exception as e:
        log.warning(f"numbers_topic send failed: {e}")

async def copy_to_numbers_topic(message: Message):
    """Копия исходного сообщения клиента в тему '📞 Номера клиентов'."""
    try:
        tid = await get_topic_id("numbers_topic")
        if tid:
            await bot.copy_message(
                chat_id=ADMIN_CHAT_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                message_thread_id=tid
            )
    except Exception as e:
        log.warning(f"numbers_topic copy failed: {e}")


async def insert_user_phone(
    tg_id: int, username: Optional[str], phone_norm: str, raw_text: str,
    ref_owner_tg_id: Optional[int] = None
):
    """
    Пишем запись о номере. Таблицу создадим на шаге БД (но тут оборачиваем в try).
    status: new/in_progress/paid/rejected/invalid/no_response
    """
    try:
        async with pool.acquire() as con:
            await con.execute("""
            INSERT INTO user_phones(tg_id, username, phone_norm, raw_text, status, ref_owner_tg_id)
            VALUES ($1,$2,$3,$4,'new',$5)
            """, tg_id, username, phone_norm, raw_text[:4000], ref_owner_tg_id)
    except Exception as e:
        log.warning(f"user_phones insert failed: {e}")
        await send_log(f"DB warn: user_phones insert failed: {e}")

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

async def get_setting(key: str, default: Optional[str] = None) -> str:
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row and row["value"] is not None else (default or "")

async def set_setting(key: str, value: str):
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO settings(key,value) VALUES($1,$2)
            ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
        """, key, value)

async def can_broadcast(uid: int) -> bool:
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT can_broadcast FROM admins WHERE tg_id=$1", uid)
        return bool(row and row["can_broadcast"])

async def can_manage(uid: int) -> bool:
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT can_manage FROM admins WHERE tg_id=$1", uid)
        return bool(row and row["can_manage"])

async def ensure_topic(name: str, chat_id: int) -> int:
    """Создаёт тему, если чат — форум. Иначе 0."""
    try:
        chat = await bot.get_chat(chat_id)
        if not getattr(chat, "is_forum", False):
            return 0
        topic = await bot.create_forum_topic(chat_id, name[:128])
        return topic.message_thread_id
    except Exception as e:
        log.warning(f"ensure_topic({name}) failed: {e}")
        return 0

async def get_topic_id(key: str) -> int:
    """Берёт id темы из settings."""
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        if not row:
            return 0
        try:
            return int(row["value"])
        except Exception:
            return 0

# ===== USER TOPICS =====
async def get_or_create_topic_for_user(tg_id: int, username: Optional[str]) -> int:
    """Создаёт/возвращает тред клиента в OPERATORS_CHAT_ID."""
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT topic_id FROM user_topics WHERE tg_id=$1", tg_id)
        if row and int(row["topic_id"]) != 0:
            return int(row["topic_id"])

    chat = await bot.get_chat(OPERATORS_CHAT_ID)
    if not getattr(chat, "is_forum", False):
        await bot.send_message(
            FALLBACK_CHAT,
            f"⚠️ Форум отключён. Тема для <code>{tg_id}</code> не создана."
        )
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO user_topics(tg_id,topic_id)
                VALUES($1,$2) ON CONFLICT (tg_id) DO NOTHING
            """, tg_id, 0)
            await con.execute("""
                INSERT INTO tickets(tg_id,topic_id,status)
                VALUES($1,$2,'open') ON CONFLICT (tg_id) DO NOTHING
            """, tg_id, 0)
        return 0

    base_title = f"@{username or '-'} | {tg_id}"
    topic = await bot.create_forum_topic(OPERATORS_CHAT_ID, name=base_title[:128])
    topic_id = topic.message_thread_id
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO user_topics(tg_id,topic_id)
            VALUES($1,$2)
            ON CONFLICT (tg_id) DO UPDATE SET topic_id=EXCLUDED.topic_id
        """, tg_id, topic_id)
        await con.execute("""
            INSERT INTO tickets(tg_id,topic_id,status,last_user_ts,last_admin_ts)
            VALUES($1,$2,'open',now(),NULL)
            ON CONFLICT (tg_id) DO UPDATE SET topic_id=EXCLUDED.topic_id
        """, tg_id, topic_id)
    return topic_id

# ===== LABELS / MACROS UI для операторов (как было) =====
def kb_labels(rows) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=f"{r['emoji'] or ''} {r['title']}", callback_data=f"label:{r['code']}")
    kb.adjust(2)
    kb.button(text="🔄 Сбросить", callback_data="label:reset")
    kb.button(text="🧹 Завершено", callback_data="label:done")
    return kb.as_markup()

def kb_macros(rows) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=f"{r['title'] or r['code']}", callback_data=f"macro:{r['id']}")
    kb.adjust(2)
    return kb.as_markup()

def kb_actions_for_thread(tg_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🧩 Макросы", callback_data=f"open:macros:{tg_id}")
    kb.button(text="🏷 Метки",   callback_data=f"open:labels:{tg_id}")
    kb.adjust(2)
    return kb.as_markup()

async def maybe_send_actions_under_thread(topic_id: int, tg_id: int):
    """Показываем блок действий под тредом по счётчику/таймеру."""
    cnt, last_ts = last_action_prompt.get(
        tg_id, (0, datetime.min.replace(tzinfo=timezone.utc))
    )
    cnt += 1
    now = datetime.now(timezone.utc)
    cooldown_ok = (now - last_ts) >= timedelta(
        hours=int(os.getenv("ACTION_COOLDOWN_HOURS", "24"))
    )
    should = (cnt % max(1, int(os.getenv("ACTION_EVERY", "5"))) == 0) or cooldown_ok
    last_action_prompt[tg_id] = (cnt, last_ts if not should else now)
    if not should:
        return
    try:
        await bot.send_message(
            OPERATORS_CHAT_ID,
            "⚙️ <b>Действия:</b> выберите нужное ↓",
            message_thread_id=topic_id,
            reply_markup=kb_actions_for_thread(tg_id)
        )
    except Exception as e:
        log.warning(f"actions prompt error: {e}")

# ===== REFERRALS =====
from handlers import referrals as ref_mod

# ===================== CLIENT /start и кнопки (НЕ ТРОГАЛ — только логгинг) =====================
@dp.message(CommandStart())
async def cmd_start(m: Message):
    try:
        payload = ""
        if " " in (m.text or ""):
            payload = (m.text or "").split(" ", 1)[1].strip()
    except Exception:
        payload = ""

    # регистрация
    async with pool.acquire() as con:
        existed = await con.fetchrow("SELECT 1 FROM users WHERE tg_id=$1", m.from_user.id)
        if not existed:
            await con.execute("INSERT INTO users(tg_id,username,role) VALUES($1,$2,'client')",
                              m.from_user.id, m.from_user.username)
        else:
            await con.execute("UPDATE users SET username=$2 WHERE tg_id=$1",
                              m.from_user.id, m.from_user.username)

    if payload and payload.startswith("r"):
        try:
            await ref_mod.record_referral_use(payload, m.from_user.id)
        except Exception as e:
            log.warning(f"record_referral_use error: {e}")

    await refresh_client_kb_from_settings()
    await m.answer(
        "<b>📲 Чтобы получить 200₽</b>\n"
        "Пришли сюда свой номер телефона и ожидай ответа в порядке очереди. "
        "После регистрации номера ты сразу получишь выплату на банковскую карту.\n\n"
        "Также можно заработать по рефералке 👇",
        reply_markup=client_kb
    )

@dp.message(F.chat.type == ChatType.PRIVATE)
async def client_router(m: Message):
    await refresh_client_kb_from_settings()
    ref = await get_setting("btn_ref_text", "💰 Заработать")
    how = await get_setting("btn_howto_text", "📜 Как получить 200 ₽")
    sup = await get_setting("btn_support_text", "🆘 Поддержка")
    sub = await get_setting("btn_submit_text", "📞 Сдать номер")

    txt = (m.text or "").strip()

    # === Кнопки клиента
    if txt == ref:
        return await ref_mod.myref_cmd(m)

    if txt == how:
        text = "📝 Краткий план: пришли номер и жди ответа оператора."
        if HOWTO_LINK:
            text += f"\n\nПодробная инструкция: {HOWTO_LINK}"
        return await m.answer(text, reply_markup=client_kb)

    if txt == sup:
        if SUPPORT_LINK:
            return await m.answer(f"Поддержка: {SUPPORT_LINK}", reply_markup=client_kb)
        return await m.answer("Напиши сюда же — оператор ответит.", reply_markup=client_kb)

    # === NEW: режим сдачи номера (как триггер)
    if txt == sub:
        # ставим «окно» на 5 минут
        dl = now_utc() + timedelta(minutes=5)
        await_number_until[m.from_user.id] = dl
        hint = await get_setting(
            "submit_hint_text",
            "✍️ Пришлите номер одним сообщением. Пример: +7 999 123-45-67"
        )
        return await m.answer(
            f"📞 <b>Режим сдачи номера активен на 5 минут</b>.\n{hint}",
            reply_markup=client_kb
        )

       # === Если человек прислал ТЕКСТ — проверяем, это номер и активно ли «окно»
    if m.text:
        # активно ли окно
        deadline = await_number_until.get(m.from_user.id)
        phone = normalize_phone(m.text)

        if phone and deadline and now_utc() <= deadline:
            # принимаем номер, сбрасываем окно
            await_number_until.pop(m.from_user.id, None)

            # кто реф-овод (если есть)
            ref_owner = None
            try:
                async with pool.acquire() as con:
                    r = await con.fetchrow("SELECT ref_owner FROM users WHERE tg_id=$1", m.from_user.id)
                    if r and r["ref_owner"]:
                        ref_owner = int(r["ref_owner"])
            except Exception:
                pass

            await insert_user_phone(
                m.from_user.id,
                m.from_user.username,
                phone,
                m.text,
                ref_owner_tg_id=ref_owner
            )

            # создаём/берём тред клиента ЗАРАНЕЕ — чтобы собрать ссылку
            topic_id = await get_or_create_topic_for_user(m.from_user.id, m.from_user.username)

            # ссылка на тред клиента
            chat_part = (
                str(OPERATORS_CHAT_ID)[4:]
                if str(OPERATORS_CHAT_ID).startswith("-100")
                else str(abs(OPERATORS_CHAT_ID))
            )
            thread_link = f"https://t.me/c/{chat_part}/{topic_id}"

            # пост в «📞 Номера клиентов» с кликабельной ссылкой на тред
            who = f"@{m.from_user.username}" if m.from_user.username else f"id {m.from_user.id}"
            await send_to_numbers_topic_text(
                f"📞 <b>{phone}</b> от {who} • <a href='{thread_link}'>Тема клиента</a>"
            )

            # копия исходного сообщения клиента в «📞 Номера клиентов»
            await copy_to_numbers_topic(m)

            # теперь форвард в операторскую тему (будет использован тот же topic_id)
            await user_to_thread(m)
            return

        # номер есть, но окно не активно → предупреждение
        if phone and not (deadline and now_utc() <= deadline):
            warn = await get_setting(
                "submit_warn_text",
                "⚠️ Номер принят не будет. Сначала нажмите «📞 Сдать номер», затем пришлите номер одним сообщением."
            )
            return await m.answer(warn, reply_markup=client_kb)

    # остальное (фото/голос/и т.п.) — идёт как обычный поток в операторов
    return await user_to_thread(m)

# ===================== ПРОКСИ КЛИЕНТ ↔ ОПЕРАТОРЫ (НЕ ТРОГАЛ) =====================
async def user_to_thread(m: Message):
    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO users(tg_id,username) VALUES($1,$2)
            ON CONFLICT (tg_id) DO UPDATE SET username=EXCLUDED.username
        """, m.from_user.id, m.from_user.username)

    topic_id = await get_or_create_topic_for_user(m.from_user.id, m.from_user.username)
    try:
        await bot.forward_message(OPERATORS_CHAT_ID, m.chat.id, m.message_id, message_thread_id=topic_id)
        await maybe_send_actions_under_thread(topic_id, m.from_user.id)
    except Exception as e:
        await bot.send_message(FALLBACK_CHAT, f"[COPY ERR user->{m.from_user.id}] {e}")

    async with pool.acquire() as con:
        await con.execute("UPDATE tickets SET last_user_ts=now() WHERE tg_id=$1", m.from_user.id)

@dp.message(F.chat.id == OPERATORS_CHAT_ID, F.message_thread_id.as_("thread_id"))
async def operator_in_thread(m: Message, thread_id: int):
    if m.from_user.is_bot:
        return
    if m.text and m.text.startswith("/"):
        return
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT tg_id FROM user_topics WHERE topic_id=$1", thread_id)
    if not row:
        return
    tg_id = int(row["tg_id"])
    try:
        await bot.copy_message(tg_id, m.chat.id, m.message_id)
        async with pool.acquire() as con:
            await con.execute("UPDATE tickets SET last_admin_ts=now() WHERE tg_id=$1", tg_id)
        await ref_mod.maybe_mark_valid_on_operator_reply(tg_id)
    except TelegramForbiddenError:
        async with pool.acquire() as con:
            await con.execute("UPDATE users SET banned=TRUE WHERE tg_id=$1", tg_id)
        try:
            await bot.edit_forum_topic(OPERATORS_CHAT_ID, message_thread_id=thread_id,
                                       name=f"🚫 [BAN] @{m.from_user.username or 'user'} -> {tg_id}")
        except Exception:
            pass
        await bot.send_message(OPERATORS_CHAT_ID, "🚫 Пользователь заблокировал бота. Сообщения не доставляются.",
                               message_thread_id=thread_id)
        await send_log(f"User {tg_id} blocked bot")
    except Exception as e:
        log.warning(f"Send message error to {tg_id}: {e}")

# ===================== LABELS & MACROS (НЕ ТРОГАЛ) =====================
@dp.callback_query(F.data.startswith("open:"))
async def cb_open_menu(c: CallbackQuery):
    if c.message.chat.id != OPERATORS_CHAT_ID or c.message.message_thread_id is None:
        return await c.answer()
    parts = c.data.split(":")
    if len(parts) < 3:
        return await c.answer()
    kind = parts[1]
    async with pool.acquire() as con:
        if kind == "labels":
            rows = await con.fetch("SELECT code,title,emoji FROM labels WHERE active=TRUE ORDER BY id")
            await bot.send_message(OPERATORS_CHAT_ID, "🏷️ Выберите метку:",
                                   message_thread_id=c.message.message_thread_id, reply_markup=kb_labels(rows))
            return await c.answer()
        if kind == "macros":
            rows = await con.fetch("SELECT id,code,COALESCE(title,code) AS title FROM macros WHERE is_active=TRUE ORDER BY id")
            if not rows:
                await bot.send_message(OPERATORS_CHAT_ID, "Макросов пока нет.", message_thread_id=c.message.message_thread_id)
                return await c.answer()
            await bot.send_message(OPERATORS_CHAT_ID, "📋 Макросы:",
                                   message_thread_id=c.message.message_thread_id, reply_markup=kb_macros(rows))
            return await c.answer()
    await c.answer()

@dp.message(F.chat.id == OPERATORS_CHAT_ID, Command("labels"))
async def cmd_labels(m: Message):
    if not m.message_thread_id:
        return
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT code,title,emoji FROM labels WHERE active=TRUE ORDER BY id")
    await m.reply("🏷️ Выберите метку:", reply_markup=kb_labels(rows))

@dp.callback_query(F.data.startswith("label:"))
async def cb_label(c: CallbackQuery):
    if c.message.chat.id != OPERATORS_CHAT_ID or c.message.message_thread_id is None:
        return await c.answer()
    code = c.data.split(":", 1)[1]
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT tg_id FROM user_topics WHERE topic_id=$1", c.message.message_thread_id)
    if not row:
        return await c.answer("Нет привязки темы.", show_alert=True)
    tg_id = int(row["tg_id"])
    if code == "reset":
        async with pool.acquire() as con:
            u = await con.fetchrow("SELECT username FROM users WHERE tg_id=$1", tg_id)
        base = f"@{(u['username'] if u and u['username'] else '-') } | {tg_id}"
        try:
            await bot.edit_forum_topic(OPERATORS_CHAT_ID, message_thread_id=c.message.message_thread_id, name=base[:128])
        except Exception:
            pass
        async with pool.acquire() as con:
            await con.execute("DELETE FROM thread_label WHERE tg_id=$1", tg_id)
        return await c.answer("Сброшено.")

    async with pool.acquire() as con:
        lab = await con.fetchrow("SELECT id, emoji, title FROM labels WHERE code=$1 AND active=TRUE", code)
        if not lab:
            return await c.answer("Нет такой метки.", show_alert=True)
        await con.execute("""
            INSERT INTO thread_label(tg_id,label_id) VALUES($1,$2)
            ON CONFLICT (tg_id) DO UPDATE SET label_id=EXCLUDED.label_id
        """, tg_id, int(lab["id"]))
        ut = await con.fetchrow("SELECT topic_id FROM user_topics WHERE tg_id=$1", tg_id)
        un = await con.fetchrow("SELECT username FROM users WHERE tg_id=$1", tg_id)
    if ut and ut["topic_id"]:
        prefix = f"{lab['emoji'] or ''} {lab['title']}".strip()
        newname = f"{prefix} | @{un['username'] or '-'} | {tg_id}"
        try:
            await bot.edit_forum_topic(OPERATORS_CHAT_ID, message_thread_id=int(ut["topic_id"]), name=newname[:128])
        except Exception as e:
            log.warning(f"edit topic name failed: {e}")
    await c.answer("Ок")

@dp.message(F.chat.id == OPERATORS_CHAT_ID, Command("macros"))
async def cmd_macros(m: Message):
    if not m.message_thread_id:
        return
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT id,code,COALESCE(title,code) AS title FROM macros WHERE is_active=TRUE ORDER BY id")
    if not rows:
        return await m.reply("Макросов пока нет.")
    await m.reply("📋 Макросы:", reply_markup=kb_macros(rows))

@dp.callback_query(F.data.startswith("macro:"))
async def cb_macro(c: CallbackQuery):
    if c.message.chat.id != OPERATORS_CHAT_ID or c.message.message_thread_id is None:
        return await c.answer()
    mid = int(c.data.split(":", 1)[1])
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT text FROM macros WHERE id=$1", mid)
        th = await con.fetchrow("SELECT tg_id FROM user_topics WHERE topic_id=$1", c.message.message_thread_id)
    if not row or not th:
        return await c.answer("Нет макроса/темы.", show_alert=True)
    tg_id = int(th["tg_id"])
    try:
        await bot.send_message(tg_id, row["text"])
        async with pool.acquire() as con:
            await con.execute("INSERT INTO macro_usage(macro_id,admin_id,tg_id) VALUES($1,$2,$3)",
                              mid, c.from_user.id, tg_id)
        await bot.send_message(OPERATORS_CHAT_ID, "📤 Отправлено клиенту.",
                               message_thread_id=c.message.message_thread_id)
        await c.answer("Отправлено.")
    except Exception as e:
        log.warning(f"macro send error: {e}")
        await c.answer("Ошибка отправки.", show_alert=True)


# ===================== SIMPLE BROADCAST (как было) =====================
async def get_broadcast_topic_id() -> int:
    return await get_topic_id("broadcast_topic")

@dp.message(F.chat.id == ADMIN_CHAT_ID, Command("broadcast"))
async def cmd_broadcast_start(m: Message):
    btid = await get_broadcast_topic_id()
    if btid and m.message_thread_id != btid:
        return await m.reply(f"📣 Запускай команду в теме «{BROADCAST_TOPIC_NAME}».")
    if not await can_broadcast(m.from_user.id):
        return await m.reply("🚫 У тебя нет доступа к рассылке.")
    broadcast_states[m.from_user.id] = {"step": "wait", "msg": None}
    await m.reply("🟡 Отправь одно сообщение (текст/фото/видео/файл). Я его разошлю всем.")

@dp.message(F.chat.id == ADMIN_CHAT_ID, ~F.text.startswith("/"))
async def admin_broadcast_flow(m: Message):
    st = broadcast_states.get(m.from_user.id)
    if not st:
        return
    btid = await get_broadcast_topic_id()
    if btid and m.message_thread_id != btid:
        return
    if st["step"] == "wait":
        st["msg"] = m
        st["step"] = "confirm"
        return await m.reply("✅ Готово. Напиши 'OK' чтобы отправить, или 'CANCEL' чтобы отменить.")
    if st["step"] == "confirm":
        txt = (m.text or "").strip().lower()
        if txt == "cancel":
            del broadcast_states[m.from_user.id]
            return await m.reply("❎ Отменено.")
        if txt == "ok":
            msg_obj = st["msg"]
            del broadcast_states[m.from_user.id]
            async with pool.acquire() as con:
                rows = await con.fetch("SELECT tg_id FROM users WHERE banned=FALSE")
            tg_ids = [r["tg_id"] for r in rows]
            delay = 0 if BROADCAST_RATE <= 0 else 60.0 / BROADCAST_RATE
            sent = 0; failed = 0
            for uid in tg_ids:
                try:
                    if msg_obj.content_type == "text":
                        await bot.send_message(uid, msg_obj.text)
                    else:
                        try:
                            await bot.copy_message(uid, msg_obj.chat.id, msg_obj.message_id)
                        except Exception:
                            if msg_obj.text:
                                await bot.send_message(uid, msg_obj.text)
                    sent += 1
                except Exception as e:
                    failed += 1
                    log.warning(f"Broadcast send error to {uid}: {e}")
                if delay:
                    await asyncio.sleep(delay)
            return await bot.send_message(ADMIN_CHAT_ID,
                                          f"📣 Готово. Отправлено: {sent}. Ошибок: {failed}.",
                                          message_thread_id=btid)
        st["msg"] = m
        return await m.reply("Текст обновлён. Напиши 'OK' чтобы разослать, либо 'CANCEL'.")

# ===================== DIAG: где я? =====================
@dp.message(Command("whereami"))
async def cmd_whereami(m: Message):
    await m.reply(
        "🔎 <b>DEBUG</b>\n"
        f"chat.id = <code>{m.chat.id}</code>\n"
        f"thread = <code>{getattr(m, 'message_thread_id', None)}</code>\n"
        f"from  = <code>{m.from_user.id}</code>\n"
        f"text  = <code>{m.text or ''}</code>"
    )


# ===================== DIAG =====================
@dp.message(Command("diag"))
async def cmd_diag(m: Message):
    if m.chat.id != ADMIN_CHAT_ID:
        return
    if not await can_manage(m.from_user.id):
        return
    chat = await bot.get_chat(ADMIN_CHAT_ID)
    ptid = await get_topic_id("panel_topic")
    btid = await get_topic_id("broadcast_topic")
    async with pool.acquire() as con:
        users = await con.fetchrow("SELECT COUNT(*) c FROM users")
        open_t = await con.fetchrow("""
            SELECT COUNT(*) c FROM tickets
            WHERE last_user_ts IS NOT NULL
              AND (last_admin_ts IS NULL OR last_admin_ts < last_user_ts)
        """)
    await m.reply(
        f"🔧 Форум={getattr(chat,'is_forum',False)}\n"
        f"PANEL={ptid} | BROADCAST={btid}\n"
        f"👥 users={int(users['c'])} | ❗open={int(open_t['c'])}"
    )

# ===================== ADMIN PANEL — ENHANCED =====================
from aiogram.exceptions import TelegramBadRequest
from io import StringIO, BytesIO
from aiogram.types import BufferedInputFile

# ---- Кнопки панели
def panel_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📣 PRO-Рассылка", callback_data="panel:broadcast_pro")],
        [InlineKeyboardButton(text="📊 История рассылок", callback_data="panel:history")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="panel:stats")],
        [InlineKeyboardButton(text="🧾 Экспорт CSV", callback_data="panel:export")],
        [InlineKeyboardButton(text="🧹 Очистить историю", callback_data="panel:cleanup_history")],
        [InlineKeyboardButton(text="🧪 Диагностика", callback_data="panel:diag")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="panel:settings")]
    ])

# ---- /panel
@dp.message(Command("panel"))
async def cmd_panel(m: Message):
    if m.chat.id != ADMIN_CHAT_ID:
        return await m.reply("🚫 Панель доступна только в админском чате.")
    ptid = await get_topic_id("panel_topic")
    if not ptid:
        return await m.reply("⚙️ Панель не найдена. Пришли /fixpanel.")

    try:
        if m.message_thread_id != ptid:
            await m.reply("➡️ Панель открывается в теме «⚙️ Панель управления». Перейди туда.")
        await bot.send_message(
            ADMIN_CHAT_ID,
            "⚙️ <b>Панель управления</b>\nВыберите действие:",
            message_thread_id=ptid,
            reply_markup=panel_menu_kb()
        )
    except Exception as e:
        log.warning(f"panel open error: {e}")
        await m.reply("⚠️ Не удалось открыть панель (проверь форумы и права бота).")

# ---- callbacks панели
@dp.callback_query(F.data.startswith("panel:"))
async def panel_callbacks(c: CallbackQuery):
    action = c.data.split(":", 1)[1]
    try:
        if action == "broadcast_pro":
            # переносим в тему «📣 Рассылки» и вызываем PRO-UI из handlers/broadcast_pro.py
            from handlers import broadcast_pro as bp
            await bp.start_pro_broadcast_entry(bot, pool, c.from_user.id, c.from_user.username, ADMIN_CHAT_ID)
        elif action == "history":
            await show_broadcast_history(c.message)
        elif action == "stats":
            await show_broadcast_stats(c.message)
        elif action == "export":
            await export_menu(c.message)
        elif action == "cleanup_history":
            await cleanup_broadcast_history(c.message)
        elif action == "diag":
            await show_diagnostics(c.message)
        elif action == "settings":
            await show_settings(c.message)
    except Exception as e:
        log.warning(f"panel_callbacks error: {e}")
        try:
            await c.message.reply(f"⚠️ Ошибка: {e}")
        except TelegramBadRequest:
            pass
            
@dp.callback_query(F.data == "panel:back")
async def cb_panel_back(c: CallbackQuery):
    """Возврат в главное меню панели"""
    ptid = await get_topic_id("panel_topic")
    if ptid:
        await bot.send_message(
            ADMIN_CHAT_ID,
            "⚙️ <b>Панель управления</b>\nВыберите действие:",
            message_thread_id=ptid,
            reply_markup=panel_menu_kb()
        )
    await c.answer()


# ===================== ИСТОРИЯ РАССЫЛОК =====================
def _hist_row_kb(row_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔍 Детали", callback_data=f"h:details:{row_id}"),
            InlineKeyboardButton(text="🔁 Повторить", callback_data=f"h:repeat:{row_id}"),
        ],
        [
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"h:delete:{row_id}"),
        ]
    ])

async def show_broadcast_history(m: Message):
    """Показывает последние 15 записей истории с кнопками действий"""
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT id, created_at, admin_id, type, media_type, sent_count, total_users,
                   LEFT(COALESCE(message_text,''), 120) AS preview
            FROM broadcast_history
            ORDER BY created_at DESC
            LIMIT 15
        """)
    if not rows:
        return await m.reply("🕒 История рассылок пуста.")

    lines = ["<b>📊 Последние рассылки:</b>"]
    for r in rows:
        rate = 0 if int(r["total_users"] or 0) == 0 else round(100*int(r["sent_count"])/int(r["total_users"]), 1)
        lines.append(
            f"• <code>#{r['id']}</code> • {r['created_at']:%d.%m %H:%M} • {r['type'].upper()} • {r['media_type']}\n"
            f"  {r['sent_count']}/{r['total_users']} ({rate}%) — {r['preview']}"
        )
    txt = "\n".join(lines)
    msg = await m.reply(txt)
    # под каждым id отдельное сообщение с кнопками (чтобы не упирались в лимиты редактирования)
    for r in rows:
        try:
            await m.reply(f"#{r['id']} • действия:", reply_markup=_hist_row_kb(int(r["id"])))
        except Exception as e:
            log.warning(f"history row kb err: {e}")

@dp.callback_query(F.data.startswith("h:"))
async def history_cb(c: CallbackQuery):
    _, cmd, sid = c.data.split(":")
    bid = int(sid)
    if cmd == "details":
        await history_details(c.message, bid)
    elif cmd == "repeat":
        await history_repeat(c.message, bid, c.from_user.id, c.from_user.username)
    elif cmd == "delete":
        await history_delete(c.message, bid)
    await c.answer()

async def history_details(m: Message, bid: int):
    async with pool.acquire() as con:
        row = await con.fetchrow("""
            SELECT id, created_at, admin_id, type, media_type, sent_count, total_users, message_text
            FROM broadcast_history WHERE id=$1
        """, bid)
    if not row:
        return await m.reply("❓ Запись не найдена.")
    rate = 0 if int(row["total_users"] or 0) == 0 else round(100*int(row["sent_count"])/int(row["total_users"]), 1)
    text = (
        f"<b>🔍 Детали рассылки #{row['id']}</b>\n"
        f"🗓 {row['created_at']:%d.%m.%Y %H:%M}\n"
        f"👤 admin_id: <code>{row['admin_id']}</code>\n"
        f"📦 тип: <code>{row['type']}</code> • медиа: <code>{row['media_type']}</code>\n"
        f"📨 {row['sent_count']}/{row['total_users']} • {rate}%\n"
        f"📝 Текст:\n<code>{(row['message_text'] or '')[:4000]}</code>"
    )
    await m.reply(text)

async def history_repeat(m: Message, bid: int, uid: int, uname: str | None):
    """Повтор запуска рассылки с тем же текстом/параметрами (без медиа возьмем текст/капшен)"""
    async with pool.acquire() as con:
        row = await con.fetchrow("""
            SELECT message_text, media_type FROM broadcast_history WHERE id=$1
        """, bid)
    if not row:
        return await m.reply("❓ Запись не найдена.")
    text = row["message_text"] or ""
    if not text:
        return await m.reply("⚠️ В записи нет текста. Повтор для медиа делай через новую PRO-рассылку.")

    # создаём «виртуальное» сообщение из текста и переиспользуем PRO-отправку
    from aiogram.types import Chat, User
    fake = Message(
        message_id=m.message_id,
        date=datetime.now(),
        chat=Chat(id=m.chat.id, type="supergroup"),
        message_thread_id=m.message_thread_id,
        from_user=User(id=uid, is_bot=False, first_name=str(uname or "admin")),
        text=text
    )
    # запустим через handlers/broadcast_pro.process_broadcast с пустыми фильтрами
    from handlers import broadcast_pro as bp
    await bp.process_broadcast(m, fake, filters={})

async def history_delete(m: Message, bid: int):
    async with pool.acquire() as con:
        n = await con.execute("DELETE FROM broadcast_history WHERE id=$1", bid)
    await m.reply(f"🗑 Удалено: {n}.")

# ===================== СТАТИСТИКА =====================
async def show_broadcast_stats(m: Message):
    async with pool.acquire() as con:
        rows = await con.fetch("""
            SELECT type,
                   COUNT(*) AS total_count,
                   SUM(sent_count) AS total_sent,
                   SUM(total_users) AS total_targets,
                   ROUND(100.0 * NULLIF(SUM(sent_count),0) / NULLIF(SUM(total_users),0), 2) AS success_rate
            FROM broadcast_history
            GROUP BY type
            ORDER BY type
        """)
        recent = await con.fetchrow("""
            SELECT COUNT(*) AS c, COALESCE(SUM(sent_count),0) AS s
            FROM broadcast_history WHERE created_at > now() - INTERVAL '7 days'
        """)
    if not rows:
        return await m.reply("📊 Нет данных по рассылкам.")
    text = ["<b>📈 Статистика рассылок:</b>\n"]
    for r in rows:
        text.append(
            f"• {r['type'].upper()}: "
            f"count={r['total_count']} | sent={r['total_sent']} | targets={r['total_targets']} | "
            f"success={r['success_rate'] or 0}%"
        )
    text.append(f"\n🗓 За 7 дней: {recent['c']} рассылок, отправлено {recent['s']} сообщений.")
    await m.reply("\n".join(text))

# ===================== ЭКСПОРТ CSV =====================
def _export_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Последние 7 дней", callback_data="exp:last7")],
        [InlineKeyboardButton(text="🗓 Последние 30 дней", callback_data="exp:last30")],
        [InlineKeyboardButton(text="📁 Всё время", callback_data="exp:all")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="exp:back")],
    ])

async def export_menu(m: Message):
    await m.reply("🧾 Что выгружаем?", reply_markup=_export_menu_kb())

@dp.callback_query(F.data.startswith("exp:"))
async def export_cb(c: CallbackQuery):
    kind = c.data.split(":")[1]
    if kind == "back":
        # вернуть главное меню панели в теме панели
        ptid = await get_topic_id("panel_topic")
        if ptid:
            await bot.send_message(
                ADMIN_CHAT_ID,
                "⚙️ <b>Панель управления</b>\nВыберите действие:",
                message_thread_id=ptid,
                reply_markup=panel_menu_kb()
            )
        return await c.answer()

    # временной фильтр
    where = ""
    title = "all"
    if kind == "last7":
        where = "WHERE created_at > now() - INTERVAL '7 days'"
        title = "last7"
    elif kind == "last30":
        where = "WHERE created_at > now() - INTERVAL '30 days'"
        title = "last30"

    async with pool.acquire() as con:
        rows = await con.fetch(f"""
            SELECT id, created_at, admin_id, type, media_type, sent_count, total_users, message_text
            FROM broadcast_history
            {where}
            ORDER BY created_at DESC
        """)

    if not rows:
        await c.message.reply("🔍 Ничего не найдено под выбранный фильтр.")
        return await c.answer()

    # собираем CSV
    sio = StringIO()
    sio.write("id,created_at,admin_id,type,media_type,sent_count,total_users,message_text\n")
    for r in rows:
        line = (
            f'{r["id"]},'
            f'"{r["created_at"].strftime("%Y-%m-%d %H:%M:%S")}",'
            f'{r["admin_id"]},'
            f'{(r["type"] or "").replace(",", " ")},'
            f'{(r["media_type"] or "").replace(",", " ")},'
            f'{r["sent_count"]},{r["total_users"]},'
            f'"{(r["message_text"] or "").replace(chr(34), chr(39)).replace(chr(10), " ").replace(chr(13), " ")}"'
        )
        
        sio.write(line + "\n")
    data = sio.getvalue().encode("utf-8")
    doc = BufferedInputFile(data, filename=f"broadcast-history-{title}.csv")
    await bot.send_document(c.message.chat.id, doc, caption="🧾 Экспорт истории", message_thread_id=c.message.message_thread_id)
    await c.answer("Готово.")

# ===================== ОЧИСТКА ИСТОРИИ =====================
async def cleanup_broadcast_history(m: Message):
    try:
        async with pool.acquire() as con:
            # если есть процедура — используем; если нет — fallback
            try:
                await con.execute("CALL auto_cleanup_broadcasts()")
                n = "старые записи удалены процедурой"
            except Exception:
                res = await con.execute("DELETE FROM broadcast_history WHERE created_at < now() - INTERVAL '360 days'")
                n = res or "ok"
        await m.reply(f"🧹 Очистка выполнена: {n}.")
    except Exception as e:
        log.warning(f"cleanup error: {e}")
        await m.reply(f"⚠️ Ошибка при очистке: {e}")

# ===================== ДИАГНОСТИКА =====================
async def show_diagnostics(m: Message):
    try:
        chat = await bot.get_chat(ADMIN_CHAT_ID)
    except Exception as e:
        return await m.reply(f"⚠️ Не смог получить админ-чат: {e}")

    ptid = await get_topic_id("panel_topic")
    btid = await get_topic_id("broadcast_topic")
    refid = await get_topic_id("referrals_topic")
    notid = await get_topic_id("not_answered_topic")

    async with pool.acquire() as con:
        users = await con.fetchval("SELECT COUNT(*) FROM users")
        banned = await con.fetchval("SELECT COUNT(*) FROM users WHERE banned=TRUE")
        hist = await con.fetchval("SELECT COUNT(*) FROM broadcast_history")
        last = await con.fetchrow("SELECT MAX(created_at) AS ts FROM broadcast_history")

    text = (
        "<b>🧪 Диагностика</b>\n"
        f"• Форум в админ-чате: <code>{getattr(chat, 'is_forum', False)}</code>\n"
        f"• panel_topic: <code>{ptid}</code>\n"
        f"• broadcast_topic: <code>{btid}</code>\n"
        f"• referrals_topic: <code>{refid}</code>\n"
        f"• not_answered_topic: <code>{notid}</code>\n\n"
        f"👥 users: <code>{users}</code> | 🚫 banned: <code>{banned}</code>\n"
        f"🗂 history rows: <code>{hist}</code>\n"
        f"🕒 last broadcast: <code>{(last['ts'].strftime('%d.%m %H:%M') if last and last['ts'] else '—')}</code>"
    )
    await m.reply(text)

# ===================== ADMIN POWER SETTINGS (ЧАСТЬ 4 — УЛУЧШЕНИЯ) =====================
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ---------- Меню настроек в панели ----------
def settings_root_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🧩 Кнопки клиента", callback_data="set:buttons")
    kb.button(text="📋 Макросы", callback_data="set:macros")
    kb.button(text="🏷 Метки", callback_data="set:labels")
    kb.button(text="👤 Админы", callback_data="set:admins")
    kb.button(text="🧠 Система", callback_data="set:sys")
    kb.button(text="⬅️ Назад", callback_data="panel:back")
    kb.adjust(2)
    return kb.as_markup()

async def show_settings(m: Message):
    await m.reply(
        "⚙️ <b>Настройки панели управления</b>\nВыберите категорию:",
        reply_markup=settings_root_kb()
    )

# ---------- CALLBACKS ----------
@dp.callback_query(F.data.startswith("set:"))
async def cb_settings_root(c: CallbackQuery):
    act = c.data.split(":", 1)[1]
    if act == "buttons":
        await settings_buttons(c.message)
    elif act == "macros":
        await settings_macros(c.message)
    elif act == "labels":
        await settings_labels(c.message)
    elif act == "admins":
        await settings_admins(c.message)
    elif act == "sys":
        await settings_sys(c.message)
    await c.answer()

# ===============================================================
#        КНОПКИ КЛИЕНТА
# ===============================================================
def _buttons_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить текст", callback_data="btns:edit")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data="btns:add")],
        [InlineKeyboardButton(text="🔁 Сбросить по умолчанию", callback_data="btns:reset")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="set:back")]
    ])

async def settings_buttons(m: Message):
    async with pool.acquire() as con:
        ref = await get_setting("btn_ref_text", "💰 Заработать")
        how = await get_setting("btn_howto_text", "📜 Как получить 200 ₽")
        sup = await get_setting("btn_support_text", "🆘 Поддержка")
    text = (
        f"🎛 <b>Настройки кнопок клиента:</b>\n\n"
        f"1️⃣ {ref}\n"
        f"2️⃣ {how}\n"
        f"3️⃣ {sup}\n\n"
        "Ты можешь изменить текст, добавить кнопку или сбросить."
    )
    await m.reply(text, reply_markup=_buttons_kb())

@dp.callback_query(F.data.startswith("btns:"))
async def cb_buttons(c: CallbackQuery):
    act = c.data.split(":")[1]
    if act == "edit":
        await c.message.reply("✏️ Пришли текст трёх кнопок через `|` (пример: 💰 Заработать | 📜 Инструкция | 🆘 Поддержка)")
        broadcast_states[c.from_user.id] = {"mode": "edit_buttons"}
    elif act == "add":
        await c.message.reply("➕ Напиши текст новой кнопки (она появится четвёртой)")
        broadcast_states[c.from_user.id] = {"mode": "add_button"}
    elif act == "reset":
        await set_setting("btn_ref_text", "💰 Заработать")
        await set_setting("btn_howto_text", "📜 Как получить 200 ₽")
        await set_setting("btn_support_text", "🆘 Поддержка")
        await refresh_client_kb_from_settings()
        await c.message.reply("♻️ Кнопки сброшены по умолчанию.")
    await c.answer()

@dp.message(F.chat.id == ADMIN_CHAT_ID, ~F.text.startswith("/"))
async def edit_buttons_reply(m: Message):
    st = broadcast_states.get(m.from_user.id)
    if not st or "mode" not in st:
        return
    if st["mode"] == "edit_buttons":
        parts = [p.strip() for p in m.text.split("|")]
        if len(parts) != 3:
            return await m.reply("⚠️ Нужно 3 кнопки через `|`")
        await set_setting("btn_ref_text", parts[0])
        await set_setting("btn_howto_text", parts[1])
        await set_setting("btn_support_text", parts[2])
        await refresh_client_kb_from_settings()
        broadcast_states.pop(m.from_user.id, None)
        await m.reply("✅ Кнопки обновлены у всех клиентов.")
    elif st["mode"] == "add_button":
        new_btn = m.text.strip()
        if not new_btn:
            return await m.reply("⚠️ Текст пуст.")
        async with pool.acquire() as con:
            await con.execute("INSERT INTO custom_buttons(title) VALUES($1)", new_btn)
        broadcast_states.pop(m.from_user.id, None)
        await m.reply(f"➕ Добавлена новая кнопка: {new_btn}")

# ===============================================================
#        МАКРОСЫ
# ===============================================================
def _macros_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Новый макрос", callback_data="macros:add")],
        [InlineKeyboardButton(text="📋 Все макросы", callback_data="macros:list")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="set:back")]
    ])

async def settings_macros(m: Message):
    await m.reply("📋 <b>Настройки макросов</b> — выбери действие:", reply_markup=_macros_admin_kb())

@dp.callback_query(F.data.startswith("macros:"))
async def cb_macros_admin(c: CallbackQuery):
    act = c.data.split(":")[1]
    if act == "add":
        await c.message.reply("🧩 Пришли код макроса и текст через `|` (пример: привет | Здравствуйте, чем могу помочь?)")
        broadcast_states[c.from_user.id] = {"mode": "add_macro"}
    elif act == "list":
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT id, code, title, is_active FROM macros ORDER BY id")
        if not rows:
            return await c.message.reply("Пока нет макросов.")
        text = "<b>📄 Список макросов:</b>\n\n" + "\n".join(
            [f"{r['id']}. {r['code']} — {'✅' if r['is_active'] else '❌'}" for r in rows]
        )
        await c.message.reply(text)
    await c.answer()

@dp.message(F.chat.id == ADMIN_CHAT_ID, ~F.text.startswith("/"))
async def add_macro_reply(m: Message):
    st = broadcast_states.get(m.from_user.id)
    if not st or st.get("mode") != "add_macro":
        return
    try:
        code, text = [x.strip() for x in m.text.split("|", 1)]
        async with pool.acquire() as con:
            await con.execute("INSERT INTO macros(code, text, is_active) VALUES($1,$2,TRUE)", code, text)
        await m.reply(f"✅ Макрос «{code}» добавлен.")
        broadcast_states.pop(m.from_user.id, None)
    except Exception as e:
        await m.reply(f"⚠️ Ошибка добавления: {e}")

# ===============================================================
#        МЕТКИ
# ===============================================================
def _labels_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Новая метка", callback_data="labels:add")],
        [InlineKeyboardButton(text="📋 Все метки", callback_data="labels:list")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="set:back")]
    ])

async def settings_labels(m: Message):
    await m.reply("🏷 <b>Настройки меток</b> — выбери действие:", reply_markup=_labels_kb())

@dp.callback_query(F.data.startswith("labels:"))
async def cb_labels(c: CallbackQuery):
    act = c.data.split(":")[1]
    if act == "add":
        await c.message.reply("🏷 Пришли код, текст и эмодзи через `|` (пример: vip | VIP клиент | 💎)")
        broadcast_states[c.from_user.id] = {"mode": "add_label"}
    elif act == "list":
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT id, code, title, emoji, active FROM labels ORDER BY id")
        if not rows:
            return await c.message.reply("Нет меток.")
        text = "<b>🏷 Список меток:</b>\n\n" + "\n".join(
            [f"{r['id']}. {r['emoji'] or ''} {r['title']} — {r['code']} ({'✅' if r['active'] else '❌'})" for r in rows]
        )
        await c.message.reply(text)
    await c.answer()

@dp.message(F.chat.id == ADMIN_CHAT_ID, ~F.text.startswith("/"))
async def add_label_reply(m: Message):
    st = broadcast_states.get(m.from_user.id)
    if not st or st.get("mode") != "add_label":
        return
    try:
        code, title, emoji = [x.strip() for x in m.text.split("|", 2)]
        async with pool.acquire() as con:
            await con.execute("INSERT INTO labels(code, title, emoji, active) VALUES($1,$2,$3,TRUE)", code, title, emoji)
        broadcast_states.pop(m.from_user.id, None)
        await m.reply(f"✅ Метка «{title}» добавлена.")
    except Exception as e:
        await m.reply(f"⚠️ Ошибка: {e}")

# ===============================================================
#        АДМИНЫ / ДОСТУП
# ===============================================================
async def settings_admins(m: Message):
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT tg_id, can_broadcast, can_manage FROM admins ORDER BY tg_id")
    if not rows:
        return await m.reply("❌ Нет админов.")
    text = "<b>👤 Администраторы:</b>\n\n"
    for r in rows:
        text += f"• <code>{r['tg_id']}</code> | рассылка={r['can_broadcast']} | панель={r['can_manage']}\n"
    await m.reply(text + "\nЧтобы выдать права: /grant 123456789 broadcast/manage")

@dp.message(F.chat.id == ADMIN_CHAT_ID, Command("grant"))
async def cmd_grant(m: Message):
    parts = (m.text or "").split()
    if len(parts) < 3:
        return await m.reply("Используй: /grant user_id broadcast/manage")
    uid = int(parts[1])
    right = parts[2]
    async with pool.acquire() as con:
        if right == "broadcast":
            await con.execute("UPDATE admins SET can_broadcast=TRUE WHERE tg_id=$1", uid)
        elif right == "manage":
            await con.execute("UPDATE admins SET can_manage=TRUE WHERE tg_id=$1", uid)
    await m.reply(f"✅ Права {right} выданы {uid}")

# ===============================================================
#        СИСТЕМНЫЕ ФУНКЦИИ
# ===============================================================
async def settings_sys(m: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Перезагрузить клавиатуру", callback_data="sys:refresh_kb")],
        [InlineKeyboardButton(text="🧩 Проверка базы кнопок", callback_data="sys:check_btns")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="set:back")]
    ])
    await m.reply("🧠 <b>Системные инструменты</b>:", reply_markup=kb)

@dp.callback_query(F.data.startswith("sys:"))
async def cb_sys(c: CallbackQuery):
    act = c.data.split(":")[1]
    if act == "refresh_kb":
        await refresh_client_kb_from_settings()
        await c.message.reply("♻️ Клавиатура у клиентов обновлена.")
    elif act == "check_btns":
        async with pool.acquire() as con:
            custom = await con.fetch("SELECT title FROM custom_buttons")
        base = [await get_setting("btn_ref_text"), await get_setting("btn_howto_text"), await get_setting("btn_support_text")]
        text = "🧩 <b>Проверка кнопок:</b>\n\n" + "\n".join(["- " + x for x in base]) + "\n"
        if custom:
            text += "\nДополнительные:\n" + "\n".join(["- " + r["title"] for r in custom])
        await c.message.reply(text)
    await c.answer()

# ===================== SLA monitor =====================
async def sla_monitor_loop():
    await asyncio.sleep(3)
    not_ans_topic = await get_topic_id("not_answered_topic")
    already_notified = set()
    while True:
        try:
            async with pool.acquire() as con:
                rows = await con.fetch("""
                SELECT t.tg_id, t.topic_id, u.username,
                       EXTRACT(EPOCH FROM (now() - t.last_user_ts))::int AS age
                FROM tickets t
                JOIN users u ON u.tg_id = t.tg_id
                WHERE t.last_user_ts IS NOT NULL
                  AND (t.last_admin_ts IS NULL OR t.last_admin_ts < t.last_user_ts)
                  AND now() - t.last_user_ts > make_interval(secs => $1)
                ORDER BY age DESC
                """, SLA_WAIT)
            if not rows:
                already_notified.clear()
            else:
                lines = ["❗ <b>Неотвеченные заявки:</b>"]
                i = 0
                for r in rows:
                    if r["tg_id"] in already_notified:
                        continue
                    already_notified.add(r["tg_id"])
                    i += 1
                    minutes = r["age"] // 60
                    chat_part = (
                        str(OPERATORS_CHAT_ID)[4:]
                        if str(OPERATORS_CHAT_ID).startswith("-100")
                        else str(abs(OPERATORS_CHAT_ID))
                    )
                    link = f"https://t.me/c/{chat_part}/{r['topic_id']}"
                    lines.append(
                        f"{i}. @{r['username'] or '-'} (id {r['tg_id']}) • {minutes} мин • <a href='{link}'>Тема</a>"
                    )
                    if i >= 20:
                        break
                text = "\n".join(lines)
                if not_ans_topic:
                    await bot.send_message(
                        OPERATORS_CHAT_ID,
                        text,
                        message_thread_id=not_ans_topic,
                        disable_web_page_preview=True,
                    )
                else:
                    await bot.send_message(
                        FALLBACK_CHAT, text, disable_web_page_preview=True
                    )
        except Exception as e:
            log.warning(f"SLA monitor error: {e}")
        await asyncio.sleep(SLA_REPORT_EVERY_MIN * 60)


# ===================== AUTO DIAG =====================
async def auto_diag_loop():
    await asyncio.sleep(5)
    period_sec = 6 * 3600
    while True:
        try:
            diag_tid = await get_topic_id("diag_topic")

            # собираем минимум показателей
            users_c = 0
            try:
                async with pool.acquire() as con:
                    r = await con.fetchrow("SELECT COUNT(*) c FROM users")
                    users_c = int(r["c"]) if r else 0
            except Exception:
                pass

            txt = (
                f"🧪 <b>Автодиагностика</b>\n"
                f"⏱️ {datetime.now().strftime('%d.%m %H:%M')}\n"
                f"👥 users: {users_c}\n"
                f"🌐 webhook: {'ON' if WEBHOOK_BASE else 'OFF'}\n"
            )

            if diag_tid:
                await bot.send_message(ADMIN_CHAT_ID, txt, message_thread_id=diag_tid)
            else:
                await bot.send_message(FALLBACK_CHAT, txt)

        except Exception as e:
            log.warning(f"auto_diag_loop error: {e}")

        await asyncio.sleep(period_sec)

# ===================== FASTAPI WEBHOOK =====================
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if WEBHOOK_SECRET:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if secret != WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="Forbidden")
    raw = await request.body()
    try:
        update = Update.model_validate(orjson.loads(raw), context={"bot": bot})
    except Exception as e:
        log.error(f"Update parse error: {e}")
        raise HTTPException(status_code=400, detail="Bad update")
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def health():
    try:
        me = await bot.get_me()
        op_chat = await bot.get_chat(OPERATORS_CHAT_ID)
        ad_chat = await bot.get_chat(ADMIN_CHAT_ID)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {
        "ok": True,
        "bot": me.username,
        "operators_forum": getattr(op_chat, "is_forum", False),
        "admin_forum": getattr(ad_chat, "is_forum", False),
        "SLA": SLA_WAIT,
        "SLA_REPORT_MIN": SLA_REPORT_EVERY_MIN
    }

    
# ===================== АВТОФИКС ПАНЕЛИ/РАССЫЛКИ =====================
async def ensure_panel_and_broadcast() -> tuple[int, int]:
    """
    Проверяет, что темы '⚙️ Панель управления' и '📣 Рассылки' существуют.
    Если нет — создаёт заново и обновляет settings.
    Возвращает кортеж (panel_id, broadcast_id)
    """
    async def topic_exists(chat_id: int, topic_id: int) -> bool:
        """Проверка, что тема реально существует (бот может в ней писать)."""
        if not topic_id:
            return False
        try:
            msg = await bot.send_message(chat_id, "ping", message_thread_id=topic_id)
            await bot.delete_message(chat_id, msg.message_id)
            return True
        except Exception:
            return False

    panel_id = await get_topic_id("panel_topic")
    broadcast_id = await get_topic_id("broadcast_topic")

    # Проверка панели
    if not panel_id or not await topic_exists(ADMIN_CHAT_ID, panel_id):
        try:
            topic = await bot.create_forum_topic(ADMIN_CHAT_ID, PANEL_TOPIC_NAME)
            panel_id = topic.message_thread_id
            async with pool.acquire() as con:
                await con.execute("""
                    INSERT INTO settings(key, value) VALUES('panel_topic', $1)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, str(panel_id))
            log.info(f"✅ Admin panel fixed: new id={panel_id}")
        except Exception as e:
            log.warning(f"⚠️ Panel auto-fix failed: {e}")

    # Проверка рассылки
    if not broadcast_id or not await topic_exists(ADMIN_CHAT_ID, broadcast_id):
        try:
            topic = await bot.create_forum_topic(ADMIN_CHAT_ID, BROADCAST_TOPIC_NAME)
            broadcast_id = topic.message_thread_id
            async with pool.acquire() as con:
                await con.execute("""
                    INSERT INTO settings(key, value) VALUES('broadcast_topic', $1)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, str(broadcast_id))
            log.info(f"✅ Broadcast fixed: new id={broadcast_id}")
        except Exception as e:
            log.warning(f"⚠️ Broadcast auto-fix failed: {e}")

    return panel_id, broadcast_id

# ===================== STARTUP / SHUTDOWN =====================
@app.on_event("startup")
async def on_startup():
    global pool
    pool = await get_pool(POSTGRES_DSN)
    await ensure_schema(pool, bot, OPERATORS_CHAT_ID, ADMIN_CHAT_ID)
    await ensure_panel_and_broadcast()

    # ensure topics created
    for key, name, chat_id in [
        ("not_answered_topic", NOT_ANSWERED_TOPIC_NAME, OPERATORS_CHAT_ID),
        ("broadcast_topic",    BROADCAST_TOPIC_NAME,    ADMIN_CHAT_ID),
        ("referrals_topic",    REFERRALS_TOPIC_NAME,    ADMIN_CHAT_ID),
        ("panel_topic",        PANEL_TOPIC_NAME,        ADMIN_CHAT_ID),
        ("logs_topic",         LOGS_TOPIC_NAME,         ADMIN_CHAT_ID),
        ("numbers_topic",      NUMBERS_TOPIC_NAME,      ADMIN_CHAT_ID),
        ("diag_topic",         DIAG_TOPIC_NAME,         ADMIN_CHAT_ID),
    ]:
        async with pool.acquire() as con:
            row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        if not row or not str(row["value"]).isdigit() or int(row["value"]) == 0:
            tid = await ensure_topic(name, chat_id)
            async with pool.acquire() as con:
                await con.execute("""
                    INSERT INTO settings(key,value) VALUES($1,$2)
                    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """, key, str(tid))

    # owner + power
    async with pool.acquire() as con:
        await con.execute("""
        INSERT INTO admins(tg_id,can_broadcast,can_manage)
        VALUES ($1, TRUE, TRUE)
        ON CONFLICT (tg_id) DO UPDATE SET can_broadcast=TRUE, can_manage=TRUE
        """, OWNER_TG_ID)
        for x in POWER_ADMINS:
            await con.execute("""
            INSERT INTO admins(tg_id,can_broadcast,can_manage)
            VALUES($1, TRUE, FALSE)
            ON CONFLICT (tg_id) DO NOTHING
            """, x)

    # команды
    try:
        await bot.set_my_commands([
            BotCommand(command="myref",         description="Реферальная ссылка 💸"),
            BotCommand(command="refstat",       description="Статистика рефералов 📊"),
            BotCommand(command="broadcast",     description="Создать рассылку 📣"),
            BotCommand(command="broadcast_pro", description="PRO-рассылка 📣"),
            BotCommand(command="macros",        description="Макросы 📋"),
            BotCommand(command="labels",        description="Метки 🏷️"),
            BotCommand(command="finish",        description="Завершить тему ✅"),
            BotCommand(command="panel",         description="Панель управления ⚙️"),
            BotCommand(command="diag",          description="Диагностика 🧪"),
        ])
    except Exception as e:
        log.warning(f"set_my_commands error: {e}")

        # === HANDLERS INIT ===
    try:
        from handlers import referrals, broadcast_pro

        # referral init (подключаем один раз)
        if not hasattr(dp, "_ref_router_added"):
            referrals.init(bot, pool, ADMIN_CHAT_ID)
            if hasattr(referrals, "router"):
                dp.include_router(referrals.router)
                dp._ref_router_added = True
                log.info("✅ referrals.router подключён")

        # broadcast_pro init (подключаем один раз)
        if not hasattr(dp, "_bcast_router_added"):
            broadcast_pro.init(bot, pool, ADMIN_CHAT_ID)
            if hasattr(broadcast_pro, "router"):
                dp.include_router(broadcast_pro.router)
                dp._bcast_router_added = True
                log.info("✅ broadcast_pro.router подключён")

    except Exception as e:
        log.warning(f"Handlers init error: {e}")

    # Для прозрачности логов
    logging.info("Routers loaded: referrals,broadcast_pro (ENABLED)")

    # webhook
    await bot.delete_webhook(drop_pending_updates=True)
    if WEBHOOK_BASE:
        await bot.set_webhook(f"{WEBHOOK_BASE}{WEBHOOK_PATH}", secret_token=WEBHOOK_SECRET)
        log.info(f"Webhook set: {WEBHOOK_BASE}{WEBHOOK_PATH}")

    # фоновые таски
    asyncio.create_task(sla_monitor_loop())
    asyncio.create_task(cleanup_history_loop())
    asyncio.create_task(auto_diag_loop())  # NEW: автодиагностика каждые 6 часов



# ежедневный клинап истории
async def cleanup_history_loop():
    await asyncio.sleep(10)
    while True:
        try:
            async with pool.acquire() as con:
                await con.execute("CALL auto_cleanup_broadcasts()")
            log.info("🧹 Старые рассылки (старше 360 дней) успешно удалены.")
        except Exception as e:
            log.warning(f"Ошибка при очистке истории рассылок: {e}")
        await asyncio.sleep(86400)

async def _start_broadcast_pro_safe(m: Message):
    """Безопасный запуск PRO-модуля из команды."""
    try:
        from handlers import broadcast_pro as bp
    except Exception as e:
        await m.reply(f"🚫 PRO-модуль недоступен: {e}")
        return

    try:
        await bp.start_pro_broadcast_entry(
            bot=bot,
            pool=pool,
            admin_id=m.from_user.id,
            admin_username=m.from_user.username,
            admin_chat_id=ADMIN_CHAT_ID
        )
    except Exception as e:
        log.exception("start_pro_broadcast_entry failed")
        await m.reply(f"⚠️ Ошибка запуска PRO-рассылки: {e}")



# ===================== UNIVERSAL COMMAND FIX =====================

@dp.message(F.text.regexp(r"^/panel"))
async def force_panel_in_forum(m: Message):
    """Открывает панель в теме ⚙️ Панель управления"""
    log.info(f"[FORCE PANEL] from={m.from_user.id} chat={m.chat.id} thread={m.message_thread_id}")
    try:
        ptid = await get_topic_id("panel_topic")
        if not ptid:
            return await m.reply("⚙️ Панель не найдена. Пришли /fixpanel.")
        await bot.send_message(
            ADMIN_CHAT_ID,
            "⚙️ <b>Панель управления</b>\nВыберите действие:",
            message_thread_id=ptid,
            reply_markup=panel_menu_kb()
        )
    except Exception as e:
        log.error(f"force_panel_in_forum error: {e}")
        await m.reply(f"⚠️ Ошибка открытия панели: {e}")


@dp.message(F.text.regexp(r"^/broadcast_pro"))
async def force_broadcast_pro_in_forum(m: Message):
    """Запуск PRO-рассылки"""
    log.info(f"[FORCE BROADCAST_PRO] from={m.from_user.id}")
    try:
        from handlers import broadcast_pro as bp
        await bp.start_pro_broadcast_entry(bot, pool, m.from_user.id, m.from_user.username, ADMIN_CHAT_ID)
    except Exception as e:
        log.error(f"force_broadcast_pro_in_forum error: {e}")
        await m.reply(f"⚠️ Ошибка запуска PRO-рассылки: {e}")


@dp.message(F.text.regexp(r"^/broadcast"))
async def force_broadcast_in_forum(m: Message):
    """Запуск простой рассылки"""
    log.info(f"[FORCE BROADCAST] from={m.from_user.id}")
    try:
        await cmd_broadcast_start(m)
    except Exception as e:
        log.error(f"force_broadcast_in_forum error: {e}")
        await m.reply(f"⚠️ Ошибка рассылки: {e}")


@dp.message(F.text.regexp(r"^/diag"))
async def force_diag_in_forum(m: Message):
    """Запуск диагностики"""
    if m.chat.id != ADMIN_CHAT_ID:
        return
    try:
        await cmd_diag(m)
    except Exception as e:
        log.error(f"force_diag_in_forum error: {e}")
        await m.reply(f"⚠️ Ошибка диагностики: {e}")


# ===================== CALLBACK FIX =====================
@dp.callback_query(F.data.startswith("pro:"))
async def universal_pro_callbacks(c: CallbackQuery):
    """Передаёт обработку кнопок PRO-модуля"""
    from handlers import broadcast_pro as bp
    await bp.pro_callback(c, bot, pool, ADMIN_CHAT_ID)

# ===================== FINAL ROUTER FIX =====================
async def bind_all_routers():
    """Безопасное подключение всех routers один раз"""
    try:
        from handlers import referrals, broadcast_pro

        # referrals
        if hasattr(referrals, "router") and referrals.router not in dp.sub_routers:
            dp.include_router(referrals.router)
            log.info("🔁 referrals.router привязан к Dispatcher")

        # broadcast_pro
        if hasattr(broadcast_pro, "router") and broadcast_pro.router not in dp.sub_routers:
            dp.include_router(broadcast_pro.router)
            log.info("🔁 broadcast_pro.router привязан к Dispatcher")

    except Exception as e:
        log.error(f"[FINAL ROUTER FIX] Ошибка подключения router'ов: {e}")


@app.on_event("startup")
async def on_startup():
    global pool
    pool = await get_pool(POSTGRES_DSN)
    await ensure_schema(pool, bot, OPERATORS_CHAT_ID, ADMIN_CHAT_ID)
    await ensure_panel_and_broadcast()

    # твой остальной код стартапа остаётся без изменений...
    asyncio.create_task(sla_monitor_loop())
    asyncio.create_task(cleanup_history_loop())
    asyncio.create_task(auto_diag_loop())

    # 🔥 В самом конце — жёсткая привязка всех routers
    await bind_all_routers()

# ===================== ENTRYPOINT (Render → ASGI) =====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

