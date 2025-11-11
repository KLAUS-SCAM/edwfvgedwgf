import asyncio
import logging
from aiogram import F, Router
from datetime import datetime
from io import StringIO
from aiogram import Bot, Dispatcher
from aiogram import Router
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

router = Router()
def init(bot, pool, admin_chat_id):
    # если хочешь — сохрани где-то ссылки, но можно и пустым оставить
    return router

log = logging.getLogger("broadcast_pro")

# --- внутренние состояния (id администратора → данные рассылки)
pro_states = {}  # {admin_id: {"msg": Message, "filters": {...}, "paused": False, "stopped": False}}


# ===============================================================
#  ПРОФЕССИОНАЛЬНАЯ РАССЫЛКА
# ===============================================================
async def start_pro_broadcast_entry(bot: Bot, pool, uid: int, uname: str, ADMIN_CHAT_ID: int):
    """Запуск интерфейса PRO-рассылки"""
    tid = await _get_topic_id(pool, "broadcast_topic")
    if not tid:
        return await bot.send_message(ADMIN_CHAT_ID, "⚠️ Тема «📣 Рассылки» не найдена!")

    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Новая рассылка", callback_data="pro:new")
    kb.button(text="📁 Шаблоны", callback_data="pro:templates")
    kb.button(text="📊 История", callback_data="pro:history")
    kb.adjust(1)
    await bot.send_message(
        ADMIN_CHAT_ID,
        "📣 <b>PRO-рассылка</b>\nВыберите действие:",
        message_thread_id=tid,
        reply_markup=kb.as_markup(),
    )


async def _get_topic_id(pool, key: str) -> int:
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return int(row["value"]) if row and str(row["value"]).isdigit() else 0


# ===============================================================
#  CALLBACK-HANDLER
# ===============================================================
@router.callback_query(F.data.startswith("pro:"))
async def pro_callback(c: CallbackQuery, bot: Bot, pool, ADMIN_CHAT_ID: int):
    act = c.data.split(":", 1)[1]

    if act == "new":
        await c.message.reply(
            "🧾 Пришли одно сообщение (текст/медиа), которое нужно разослать.\n"
            "После этого появится предпросмотр и кнопки подтверждения."
        )
        pro_states[c.from_user.id] = {"step": "wait_msg"}
        return await c.answer("Жду сообщение для рассылки.")

    if act == "templates":
        await show_templates(bot, c.message, pool)
        return await c.answer()

    if act == "history":
        await show_history(bot, c.message, pool)
        return await c.answer()

    if act in {"pause", "resume", "stop"}:
        await manage_broadcast(act, c, bot)
        return await c.answer()


# ===============================================================
#  ПОЛУЧЕНИЕ СООБЩЕНИЯ ДЛЯ РАССЫЛКИ
# ===============================================================
@router.message(F.chat.type.in_({"group", "supergroup"}))
async def process_broadcast(m: Message, bot: Bot, pool):
    st = pro_states.get(m.from_user.id)
    if not st or st.get("step") != "wait_msg":
        return

    pro_states[m.from_user.id] = {
        "step": "confirm",
        "msg": m,
        "paused": False,
        "stopped": False,
    }

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Отправить", callback_data="pro:confirm_send"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="pro:cancel"),
        ]
    ])
    await m.reply("🟡 Предпросмотр готов. Нажми ✅ для начала рассылки.", reply_markup=kb)


# ===============================================================
#  КОНФИРМ / ОТМЕНА
# ===============================================================
@router.callback_query(F.data.in_({"pro:confirm_send", "pro:cancel"}))
async def pro_confirm_cb(c: CallbackQuery, bot: Bot, pool):
    if c.data == "pro:cancel":
        pro_states.pop(c.from_user.id, None)
        return await c.message.reply("❎ Рассылка отменена.")

    st = pro_states.get(c.from_user.id)
    if not st:
        return await c.answer("Нет активной рассылки.")

    msg_obj = st["msg"]
    asyncio.create_task(run_broadcast(bot, pool, c.from_user.id, msg_obj))
    await c.message.reply("🚀 Рассылка запущена.")
    await c.answer()


# ===============================================================
#  ВЫПОЛНЕНИЕ РАССЫЛКИ
# ===============================================================
async def run_broadcast(bot: Bot, pool, admin_id: int, msg: Message):
    state = pro_states.get(admin_id, {"paused": False, "stopped": False})
    sent = 0
    failed = 0

    async with pool.acquire() as con:
        rows = await con.fetch("SELECT tg_id FROM users WHERE banned=FALSE")
    tg_ids = [r["tg_id"] for r in rows]
    total = len(tg_ids)
    delay = 0.05  # ~20 сообщений/сек

    tid = await _get_topic_id(pool, "broadcast_topic")

    msg_ctrl = await bot.send_message(
        msg.chat.id,
        f"📤 Рассылка начата ({total} польз.)",
        message_thread_id=tid,
        reply_markup=_broadcast_ctrl_kb(),
    )

    for uid in tg_ids:
        st = pro_states.get(admin_id)
        if not st or st.get("stopped"):
            break
        while st.get("paused"):
            await asyncio.sleep(1)

        try:
            if msg.content_type == "text":
                await bot.send_message(uid, msg.text)
            else:
                await bot.copy_message(uid, msg.chat.id, msg.message_id)
            sent += 1
        except Exception as e:
            failed += 1
            log.warning(f"broadcast to {uid} failed: {e}")
        await asyncio.sleep(delay)

        if sent % 100 == 0 or uid == tg_ids[-1]:
            await bot.edit_message_text(
                f"📊 Отправлено: {sent}/{total}\n❌ Ошибок: {failed}",
                msg.chat.id,
                msg_ctrl.message_id,
                reply_markup=_broadcast_ctrl_kb(),
            )

    await save_broadcast_history(pool, admin_id, msg, sent, total)
    await bot.send_message(msg.chat.id, f"✅ Рассылка завершена.\n📨 Отправлено {sent}/{total}, ошибок {failed}.")
    pro_states.pop(admin_id, None)


def _broadcast_ctrl_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⏸ Пауза", callback_data="pro:pause"),
            InlineKeyboardButton(text="▶️ Продолжить", callback_data="pro:resume"),
            InlineKeyboardButton(text="⛔ Стоп", callback_data="pro:stop"),
        ]
    ])


async def manage_broadcast(act, c: CallbackQuery, bot: Bot):
    st = pro_states.get(c.from_user.id)
    if not st:
        return await c.answer("Нет активной рассылки.")
    if act == "pause":
        st["paused"] = True
        await c.message.reply("⏸ Рассылка приостановлена.")
    elif act == "resume":
        st["paused"] = False
        await c.message.reply("▶️ Продолжено.")
    elif act == "stop":
        st["stopped"] = True
        await c.message.reply("⛔ Рассылка остановлена.")


# ===============================================================
#  ШАБЛОНЫ РАССЫЛОК
# ===============================================================
async def show_templates(bot: Bot, m: Message, pool):
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT id,title FROM broadcast_templates ORDER BY id")
    if not rows:
        return await m.reply("📭 Шаблонов нет.")
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.button(text=r["title"], callback_data=f"pro:tpl:{r['id']}")
    kb.adjust(1)
    await m.reply("📁 Выберите шаблон:", reply_markup=kb.as_markup())


# ===============================================================
#  ИСТОРИЯ РАССЫЛОК
# ===============================================================
async def show_history(bot: Bot, m: Message, pool):
    async with pool.acquire() as con:
        rows = await con.fetch(
            "SELECT id, created_at, admin_id, sent_count, total_users FROM broadcast_history ORDER BY created_at DESC LIMIT 10"
        )
    if not rows:
        return await m.reply("📭 История рассылок пуста.")
    text = "<b>📜 Последние рассылки:</b>\n\n"
    for r in rows:
        rate = 0 if not r["total_users"] else round(100 * r["sent_count"] / r["total_users"], 1)
        text += f"#{r['id']} • {r['created_at']:%d.%m %H:%M} • {rate}% успеха\n"
    await m.reply(text)


async def save_broadcast_history(pool, admin_id: int, msg: Message, sent: int, total: int):
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO broadcast_history(created_at, admin_id, type, media_type, sent_count, total_users, message_text)
            VALUES($1,$2,'pro',$3,$4,$5,$6)
            """,
            datetime.utcnow(),
            admin_id,
            msg.content_type,
            sent,
            total,
            (msg.text or msg.caption or "")[:4000],
        )
        
# ===============================================================
# INIT HOOK — подключение router к Dispatcher
# ===============================================================
def init(bot, pool, admin_chat_id):
    """
    Подключает router PRO-рассылки к основному Dispatcher.
    """
    from main import dp
    try:
        dp.include_router(router)
        import logging
        logging.info("✅ broadcast_pro.router подключён успешно")
    except Exception as e:
        import logging
        logging.error(f"❌ Ошибка при подключении broadcast_pro.router: {e}")
