import os

class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
    OPERATORS_CHAT_ID = int(os.getenv("OPERATORS_CHAT_ID", "0"))
    FALLBACK_CHAT_ID = int(os.getenv("FALLBACK_CHAT_ID", "0"))

    POSTGRES_DSN = os.getenv("POSTGRES_DSN")

    BROADCAST_RATE = int(os.getenv("BROADCAST_RATE", "20"))
    SLA_WAIT_SECONDS = int(os.getenv("SLA_WAIT_SECONDS", "300"))
    SLA_REPORT_EVERY_MIN = int(os.getenv("SLA_REPORT_EVERY_MIN", "30"))

CFG = Config()
