from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# === Главное меню админ-панели ===
def admin_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🧷 Привязать тему «Рассылки»",
                callback_data="bind_broadcast"
            )
        ],
        [
            InlineKeyboardButton(
                text="🚀 Проф. рассылка",
                callback_data="broadcast_pro"
            )
        ],
        [
            InlineKeyboardButton(
                text="📊 Статистика (скоро)",
                callback_data="stats_stub"
            )
        ],
    ])


# === Клавиатура подтверждения рассылки ===
def broadcast_confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Поехали", callback_data="broadcast_go"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel"),
        ]
    ])
