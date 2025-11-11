import asyncio
import logging
from datetime import datetime
from io import StringIO

from aiogram import Bot, F, Router
from aiogram import Router
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

router = Router()
log = logging.getLogger("admin_panel")


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
async def get_topic_id(pool, key: str) -> int:
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        if not row:
            return 0
        try:
            return int(row["value"])
        except Exception:
            return 0


async def get_setting(pool, key: str, default: str = "") -> str:
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row and row["value"] else default


async def set_setting(pool, key: str, value: str):
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO settings(key,value) VALUES($1,$2)
            ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
            """,
            key,
            value,
        )


# ========== ГЛАВНОЕ МЕНЮ ==========
def main_panel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📞 Номера клиентов", callback_data="panel:numbers")
    kb.button(text="💰 Рефералка", callback_data="panel:referrals")
    kb.button(text="📊 Статистика", callback_data="panel:stats")
    kb.button(text="⚙️ Настройки", callback_data="panel:settings")
    kb.button(text="🧩 Макросы и Метки", callback_data="panel:macros_labels")
    kb.button(text="🧪 Диагностика", callback_data="panel:diag")
    kb.button(text="🧷 Логи", callback_data="panel:logs")
    kb.adjust(2)
    return kb.as_markup()


@router.message(F.text == "/panel")
async def cmd_panel(m: Message, bot: Bot, pool, ADMIN_CHAT_ID: int):
    if m.chat.id != ADMIN_CHAT_ID:
        return await m.reply("🚫 Панель доступна только в админском чате.")
    tid = await get_topic_id(pool, "panel_topic")
    if not tid:
        return await m.reply("⚙️ Панель не найдена. Пришли /fixpanel.")
    await bot.send_message(
        ADMIN_CHAT_ID,
        "⚙️ <b>Панель управления</b>\nВыберите категорию:",
        message_thread_id=tid,
        reply_markup=main_panel_kb(),
    )


# ========== CALLBACK ОБРАБОТЧИК ==========
@router.callback_query(F.data.startswith("panel:"))
async def cb_panel(c: CallbackQuery, bot: Bot, pool, ADMIN_CHAT_ID: int):
    action = c.data.split(":", 1)[1]

    if action == "numbers":
        await show_numbers_panel(c.message, pool)
    elif action == "referrals":
        await show_referrals_panel(c.message, pool)
    elif action == "stats":
        await show_stats_panel(c.message, pool)
    elif action == "settings":
        await show_settings_panel(c.message, pool)
    elif action == "macros_labels":
        await show_macros_labels_panel(c.message, pool)
    elif action == "diag":
        await show_diag_panel(c.message, bot, pool, ADMIN_CHAT_ID)
    elif action == "logs":
        await show_logs_panel(c.message, bot, pool, ADMIN_CHAT_ID)

    await c.answer()


# ========== РАЗДЕЛ: НОМЕРА КЛИЕНТОВ ==========
async def show_numbers_panel(m: Message, pool):
    text = "<b>📞 Хранилище номеров</b>\n\nФильтры по статусам:"
    kb = InlineKeyboardBuilder()
    kb.button(text="🟡 Новые", callback_data="nums:new")
    kb.button(text="🟢 Оплаченные", callback_data="nums:paid")
    kb.button(text="🔴 Отклонённые", callback_data="nums:rejected")
    kb.button(text="⚪ Все номера", callback_data="nums:all")
    kb.button(text="🧾 Экспорт CSV", callback_data="nums:export")
    kb.adjust(2)
    await m.reply(text, reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("nums:"))
async def cb_numbers(c: CallbackQuery, pool):
    act = c.data.split(":")[1]
    where = ""
    title = "Все номера"

    if act == "new":
        where = "WHERE status='new'"
        title = "🟡 Новые"
    elif act == "paid":
        where = "WHERE status='paid'"
        title = "🟢 Оплаченные"
    elif act == "rejected":
        where = "WHERE status='rejected'"
        title = "🔴 Отклонённые"

    async with pool.acquire() as con:
        rows = await con.fetch(
            f"SELECT tg_id, username, phone_norm, status, created_at FROM user_phones {where} ORDER BY created_at DESC LIMIT 20"
        )

    if not rows:
        return await c.message.reply("📭 Нет записей по выбранному фильтру.")

    lines = [f"<b>{title}</b>\n"]
    for r in rows:
        lines.append(
            f"@{r['username'] or '-'} | <code>{r['phone_norm']}</code> • {r['status']} • {r['created_at']:%d.%m %H:%M}"
        )

    await c.message.reply("\n".join(lines))
    await c.answer()


# ========== РАЗДЕЛ: РЕФЕРАЛКА ==========
async def show_referrals_panel(m: Message, pool):
    async with pool.acquire() as con:
        count = await con.fetchval("SELECT COUNT(*) FROM referrals")
    text = (
        f"<b>💰 Реферальная система</b>\n"
        f"Всего записей: {count}\n\n"
        f"Можно выгрузить статистику, посмотреть топ рефералов или изменить бонус."
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Топ рефералов", callback_data="refs:top")
    kb.button(text="💸 Изменить бонус", callback_data="refs:bonus")
    kb.button(text="🧾 Экспорт CSV", callback_data="refs:export")
    kb.adjust(2)
    await m.reply(text, reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("refs:"))
async def cb_referrals(c: CallbackQuery, pool):
    act = c.data.split(":")[1]

    if act == "top":
        async with pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT ref_owner_tg_id, COUNT(*) AS cnt
                FROM user_phones
                WHERE ref_owner_tg_id IS NOT NULL
                GROUP BY ref_owner_tg_id
                ORDER BY cnt DESC
                LIMIT 10
                """
            )
        if not rows:
            return await c.message.reply("📭 Пока нет рефералов.")
        text = "<b>🏆 Топ рефералов:</b>\n\n"
        for i, r in enumerate(rows, 1):
            text += f"{i}. <code>{r['ref_owner_tg_id']}</code> — {r['cnt']} реф.\n"
        await c.message.reply(text)

    elif act == "bonus":
        await c.message.reply("💸 Пришли новый бонус в рублях (пример: 1000)")
        # обработка изменения бонуса — в основном коде (через broadcast_states)

    elif act == "export":
        async with pool.acquire() as con:
            rows = await con.fetch(
                "SELECT ref_owner_tg_id, COUNT(*) AS cnt FROM user_phones WHERE ref_owner_tg_id IS NOT NULL GROUP BY ref_owner_tg_id"
            )
        if not rows:
            return await c.message.reply("Нет данных для выгрузки.")
        sio = StringIO()
        sio.write("ref_owner_tg_id,count\n")
        for r in rows:
            sio.write(f"{r['ref_owner_tg_id']},{r['cnt']}\n")
        data = sio.getvalue().encode("utf-8")
        doc = BufferedInputFile(data, filename="referrals.csv")
        await c.message.reply_document(doc, caption="💰 Экспорт рефералов")

    await c.answer()


# ========== РАЗДЕЛ: СТАТИСТИКА ==========
async def show_stats_panel(m: Message, pool):
    async with pool.acquire() as con:
        users = await con.fetchval("SELECT COUNT(*) FROM users")
        phones = await con.fetchval("SELECT COUNT(*) FROM user_phones")
        paid = await con.fetchval("SELECT COUNT(*) FROM user_phones WHERE status='paid'")
    await m.reply(
        f"<b>📊 Общая статистика:</b>\n"
        f"👥 Пользователей: {users}\n"
        f"📞 Номеров: {phones}\n"
        f"💸 Оплачено: {paid}\n"
    )


# ========== РАЗДЕЛ: НАСТРОЙКИ ==========
async def show_settings_panel(m: Message, pool):
    text = "⚙️ <b>Настройки</b>\nРедактируйте тексты клиентских кнопок, лимиты и бонусы."
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Кнопки клиента", callback_data="set:buttons")
    kb.button(text="♻️ Сбросить по умолчанию", callback_data="set:reset")
    kb.adjust(1)
    await m.reply(text, reply_markup=kb.as_markup())


# ========== РАЗДЕЛ: МАКРОСЫ И МЕТКИ ==========
async def show_macros_labels_panel(m: Message, pool):
    text = (
        "<b>🧩 Макросы и Метки</b>\n"
        "Здесь можно добавлять или редактировать макросы, а также метки операторов."
    )
    await m.reply(text)


# ========== РАЗДЕЛ: ДИАГНОСТИКА ==========
async def show_diag_panel(m: Message, bot: Bot, pool, ADMIN_CHAT_ID: int):
    users = await pool.fetchval("SELECT COUNT(*) FROM users")
    phones = await pool.fetchval("SELECT COUNT(*) FROM user_phones")
    text = (
        f"🧪 <b>Диагностика</b>\n"
        f"⏱ {datetime.now().strftime('%d.%m %H:%M')}\n"
        f"👥 users={users} | 📞 phones={phones}\n"
        f"🌐 webhook={'ON' if bot else 'OFF'}"
    )
    await m.reply(text)


# ========== РАЗДЕЛ: ЛОГИ ==========
async def show_logs_panel(m: Message, bot: Bot, pool, ADMIN_CHAT_ID: int):
    tid = await get_topic_id(pool, "logs_topic")
    if not tid:
        return await m.reply("⚠️ Тема логов не найдена.")
    await bot.send_message(
        ADMIN_CHAT_ID,
        "🧷 <b>Логи бота</b>\nВсе критические ошибки автоматически публикуются здесь.",
        message_thread_id=tid,
    )

