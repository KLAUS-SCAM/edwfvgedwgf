import logging
from aiogram import F, Router
from datetime import datetime
from aiogram import Bot, F, Router
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

router = Router()
log = logging.getLogger("referrals")

# ===============================================================
#  ИНИЦИАЛИЗАЦИЯ
# ===============================================================
def init(bot: Bot, pool, ADMIN_CHAT_ID: int):
    """Инициализация рефералки"""
    global g_bot, g_pool, g_admin_chat
    g_bot, g_pool, g_admin_chat = bot, pool, ADMIN_CHAT_ID
    log.info("✅ Referral system initialized")


# ===============================================================
#  ГЕНЕРАЦИЯ ССЫЛКИ
# ===============================================================
@router.message(F.text == "💰 Заработать")
@router.message(F.text.lower().contains("реферал"))
async def myref_cmd(m: Message):
    """Создание персональной реферальной ссылки"""
    tg_id = m.from_user.id
    uname = m.from_user.username or "user"
    payload = f"r{tg_id}"
    link = f"https://t.me/{(await g_bot.get_me()).username}?start={payload}"

    async with g_pool.acquire() as con:
        await con.execute("""
            INSERT INTO referrals(ref_owner, ref_link, created_at)
            VALUES($1, $2, NOW())
            ON CONFLICT (ref_owner) DO UPDATE SET ref_link=EXCLUDED.ref_link
        """, tg_id, link)

    await m.answer(
        f"💰 <b>Твоя реферальная ссылка:</b>\n\n<code>{link}</code>\n\n"
        "🔗 Отправь её друзьям — за каждого активного реферала ты получаешь бонус!",
        reply_markup=ref_menu_kb(),
    )


def ref_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="ref:stat")],
        [InlineKeyboardButton(text="🏆 Топ рефералов", callback_data="ref:top")]
    ])


# ===============================================================
#  ЗАПИСЬ ПРИХОДА РЕФЕРАЛА
# ===============================================================
async def record_referral_use(payload: str, new_user_id: int):
    """Когда человек зашёл по реферальной ссылке"""
    if not payload.startswith("r"):
        return
    ref_owner = int(payload[1:])
    if ref_owner == new_user_id:
        return  # не считаем себя

    async with g_pool.acquire() as con:
        # Проверим, не зарегистрирован ли уже
        exist = await con.fetchrow("SELECT 1 FROM users WHERE tg_id=$1", new_user_id)
        if not exist:
            await con.execute(
                "INSERT INTO users(tg_id, username, role, ref_owner) VALUES($1,$2,'client',$3)",
                new_user_id, None, ref_owner
            )
        else:
            await con.execute(
                "UPDATE users SET ref_owner=$2 WHERE tg_id=$1", new_user_id, ref_owner
            )

    # Лог в админку
    ref_tid = await get_topic_id("referrals_topic")
    text = (
        f"👥 <b>Новый реферал!</b>\n"
        f"Рефовод: <code>{ref_owner}</code>\n"
        f"Реферал: <code>{new_user_id}</code>\n"
        f"⏰ {datetime.now():%d.%m %H:%M}"
    )
    try:
        if ref_tid:
            await g_bot.send_message(g_admin_chat, text, message_thread_id=ref_tid)
        else:
            await g_bot.send_message(g_admin_chat, text)
    except Exception as e:
        log.warning(f"notify admin ref error: {e}")


# ===============================================================
#  КОЛБЭКИ МЕНЮ РЕФЕРАЛКИ
# ===============================================================
@router.callback_query(F.data.startswith("ref:"))
async def cb_ref(c, bot: Bot):
    act = c.data.split(":")[1]
    if act == "stat":
        await show_my_stats(c)
    elif act == "top":
        await show_top_refs(c)
    await c.answer()


async def show_my_stats(c):
    uid = c.from_user.id
    async with g_pool.acquire() as con:
        stats = await con.fetchrow("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE validated=TRUE) AS valid
            FROM referrals_usage WHERE ref_owner=$1
        """, uid)
    total = stats["total"] if stats else 0
    valid = stats["valid"] if stats else 0
    await c.message.reply(
        f"📊 <b>Твоя статистика:</b>\n"
        f"Всего приглашено: <b>{total}</b>\n"
        f"Подтверждено: <b>{valid}</b>\n"
        f"💰 Бонусы начисляются за подтверждённых рефералов."
    )


async def show_top_refs(c):
    async with g_pool.acquire() as con:
        rows = await con.fetch("""
            SELECT ref_owner, COUNT(*) AS cnt
            FROM users
            WHERE ref_owner IS NOT NULL
            GROUP BY ref_owner
            ORDER BY cnt DESC
            LIMIT 10
        """)
    if not rows:
        return await c.message.reply("📭 Пока нет активных рефералов.")
    text = "<b>🏆 Топ 10 рефоводов:</b>\n\n"
    for i, r in enumerate(rows, 1):
        text += f"{i}. <code>{r['ref_owner']}</code> — {r['cnt']} реф.\n"
    await c.message.reply(text)


# ===============================================================
#  УВЕДОМЛЕНИЕ О ДОСТИЖЕНИИ БОНУСА
# ===============================================================
async def maybe_mark_valid_on_operator_reply(tg_id: int):
    """Если оператор ответил, и это реферал — проверим цель"""
    async with g_pool.acquire() as con:
        r = await con.fetchrow("SELECT ref_owner FROM users WHERE tg_id=$1", tg_id)
        if not r or not r["ref_owner"]:
            return
        owner = int(r["ref_owner"])
        await con.execute("""
            INSERT INTO referrals_usage(ref_owner, referral, validated, created_at)
            VALUES($1,$2,TRUE,NOW())
            ON CONFLICT (referral) DO UPDATE SET validated=TRUE
        """ , owner, tg_id)

        # Проверим кол-во подтверждённых
        count = await con.fetchval("SELECT COUNT(*) FROM referrals_usage WHERE ref_owner=$1 AND validated=TRUE", owner)
        bonus_goal = int(await get_setting("ref_bonus_goal", "10"))
        bonus_value = int(await get_setting("ref_bonus_value", "1000"))

    # уведомляем, если достигнут лимит
    if count == bonus_goal:
        txt_owner = (
            f"🎉 <b>Поздравляем!</b>\n"
            f"Ты достиг цели — {bonus_goal} подтверждённых рефералов.\n"
            f"💸 Тебе начислен бонус {bonus_value}₽.\n"
            f"Если не получили выплату — напиши в поддержку."
        )
        try:
            await g_bot.send_message(owner, txt_owner)
        except Exception:
            pass

        ref_tid = await get_topic_id("referrals_topic")
        note = (
            f"💰 <b>Рефовод {owner} достиг цели!</b>\n"
            f"✅ {count}/{bonus_goal} рефералов\n"
            f"🎁 Бонус {bonus_value}₽ начислить вручную."
        )
        try:
            if ref_tid:
                await g_bot.send_message(g_admin_chat, note, message_thread_id=ref_tid)
            else:
                await g_bot.send_message(g_admin_chat, note)
        except Exception as e:
            log.warning(f"notify admin bonus error: {e}")


# ===============================================================
#  ВСПОМОГАТЕЛЬНЫЕ
# ===============================================================
async def get_topic_id(key: str) -> int:
    async with g_pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return int(row["value"]) if row and str(row["value"]).isdigit() else 0


async def get_setting(key: str, default: str = "") -> str:
    async with g_pool.acquire() as con:
        row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row and row["value"] else default
