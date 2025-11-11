import asyncpg
import logging
from aiogram import Bot

log = logging.getLogger("core.db")


# ===============================================================
#  ИНИЦИАЛИЗАЦИЯ ПУЛА
# ===============================================================
async def get_pool(dsn: str) -> asyncpg.Pool:
    log.info("🔌 Подключение к PostgreSQL...")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
    log.info("✅ Подключено к PostgreSQL.")
    return pool


# ===============================================================
#  ГАРАНТИРУЕМ ВСЕ СХЕМЫ И ТАБЛИЦЫ
# ===============================================================
async def ensure_schema(pool: asyncpg.Pool, bot: Bot, operators_chat_id: int, admin_chat_id: int):
    """Создаёт все таблицы, если их нет"""
    log.info("🧱 Проверка и создание таблиц...")

    async with pool.acquire() as con:
        # === USERS ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id BIGINT PRIMARY KEY,
            username TEXT,
            role TEXT DEFAULT 'client',
            banned BOOLEAN DEFAULT FALSE,
            ref_owner BIGINT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # === ADMINS ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            tg_id BIGINT PRIMARY KEY,
            can_broadcast BOOLEAN DEFAULT FALSE,
            can_manage BOOLEAN DEFAULT FALSE
        );
        """)

        # === SETTINGS ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)

        # === LABELS ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS labels (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE,
            title TEXT,
            emoji TEXT,
            active BOOLEAN DEFAULT TRUE
        );
        """)

        # === MACROS ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS macros (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE,
            title TEXT,
            text TEXT,
            is_active BOOLEAN DEFAULT TRUE
        );
        """)

        # === THREAD LABELS (привязка темы к метке) ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS thread_label (
            tg_id BIGINT PRIMARY KEY,
            label_id INT REFERENCES labels(id)
        );
        """)

        # === USER TOPICS (для связи клиента с темой форума) ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS user_topics (
            tg_id BIGINT PRIMARY KEY,
            topic_id BIGINT
        );
        """)

        # === TICKETS (для SLA и поддержки) ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            tg_id BIGINT PRIMARY KEY,
            topic_id BIGINT,
            status TEXT DEFAULT 'open',
            last_user_ts TIMESTAMP,
            last_admin_ts TIMESTAMP
        );
        """)

        # === BROADCAST HISTORY ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_history (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP DEFAULT NOW(),
            admin_id BIGINT,
            type TEXT,
            media_type TEXT,
            sent_count INT DEFAULT 0,
            total_users INT DEFAULT 0,
            message_text TEXT
        );
        """)

        # === BROADCAST TEMPLATES ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_templates (
            id SERIAL PRIMARY KEY,
            title TEXT,
            content TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # === USER PHONES (ХРАНИЛИЩЕ НОМЕРОВ) ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS user_phones (
            id SERIAL PRIMARY KEY,
            tg_id BIGINT NOT NULL,
            username TEXT,
            phone_norm TEXT,
            raw_text TEXT,
            status TEXT DEFAULT 'new',
            ref_owner_tg_id BIGINT,
            created_at TIMESTAMP DEFAULT NOW(),
            last_action TIMESTAMP DEFAULT NOW(),
            macro_used TEXT DEFAULT NULL
        );
        """)

        # === REFERRALS ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            ref_owner BIGINT PRIMARY KEY,
            ref_link TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # === REFERRALS USAGE ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS referrals_usage (
            id SERIAL PRIMARY KEY,
            ref_owner BIGINT,
            referral BIGINT UNIQUE,
            validated BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # === CUSTOM BUTTONS (доп. клиентские кнопки) ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS custom_buttons (
            id SERIAL PRIMARY KEY,
            title TEXT
        );
        """)

        # === MACRO USAGE (аналитика макросов) ===
        await con.execute("""
        CREATE TABLE IF NOT EXISTS macro_usage (
            id SERIAL PRIMARY KEY,
            macro_id INT REFERENCES macros(id),
            admin_id BIGINT,
            tg_id BIGINT,
            used_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # === AUTO-CLEAN FUNCTION ===
        await con.execute("""
        CREATE OR REPLACE PROCEDURE auto_cleanup_broadcasts()
        LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM broadcast_history WHERE created_at < NOW() - INTERVAL '360 days';
        END;
        $$;
        """)

    log.info("✅ Все таблицы и процедуры проверены / созданы.")


# ===============================================================
#  ДОПОЛНИТЕЛЬНЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ===============================================================
async def ensure_topic_setting(pool: asyncpg.Pool, bot: Bot, chat_id: int, key: str, name: str):
    """Создаёт тему, если отсутствует, и записывает в settings"""
    try:
        async with pool.acquire() as con:
            row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        if row and str(row["value"]).isdigit() and int(row["value"]) > 0:
            return int(row["value"])
        topic = await bot.create_forum_topic(chat_id, name)
        tid = topic.message_thread_id
        async with pool.acquire() as con:
            await con.execute("""
                INSERT INTO settings(key,value) VALUES($1,$2)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
            """, key, str(tid))
        log.info(f"✅ Created topic '{name}' with id={tid}")
        return tid
    except Exception as e:
        log.warning(f"ensure_topic_setting error: {e}")
        return 0
