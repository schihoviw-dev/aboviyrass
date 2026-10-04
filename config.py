# -*- coding: utf-8 -*-
"""
Конфиг менеджера рассылок Telegram.
"""

# =========================================================
# БОТ-МЕНЕДЖЕР
# =========================================================
BOT = {
    "api_id": 25275139,
    "api_hash": "17da51cd5121cebf33c0ef4b7d6cf03d",
    "bot_token": "8217470990:AAGJEDyNoOYZl49HwYmJX9RT9onsXG6-dWk",
    "owner_id": 6742354289,
}

# =========================================================
# СТРОКОВЫЕ СЕССИИ (Pyrogram string session)
# =========================================================
SESSION_STRINGS = {
    "my_session": (
        "AgGBqwMANJfdRCzQThfulU4SzdmwmjUfB0Gcb6nzghFr8wyfL5c0IQXCW2BOYeIv9n-"
        "Off4wwiv6SdYUKd3IFq7qzevThznk5VD0YIQKDmBFAuiUHyGE3wHLmJnPC0ZHFUvHvU"
        "HhdDgrxQ1rCvu9a5hAlvzwvfeTxMNK3iO_B2nO5u9h5Zjv7GFgRH8_xWpTnMhfzB1KO3"
        "ikfRJoR9lr0ip0kD1SossWF38GSlEmb4yqFKLVEV_tW2j00uuU-denCpoZMcHwKE_zL7-"
        "px_1Fklbr_JGfVzExepzG5B2tDf4anLWdFnR_KxWAIwJQCbl-DbkTBMy_LK25HJ2swXa"
        "Z7OSo53AneQAAAAGR4ClxAA"
    ),
}

# =========================================================
# НАСТРОЙКИ ПО УМОЛЧАНИЮ
# copy_mode: False = обычная пересылка (с «Переслано от»)
# copy_mode: True  = копия без подписи
# =========================================================
DEFAULTS = {
    "interval_sec": 20,
    "target_delay_sec": 1.5,
    "copy_mode": False,
    "loop_forever": True,
    "loop_pause_sec": 60,
    "history_limit": 0,
    "live": True,
}

# =========================================================
# СИСТЕМНЫЕ НАСТРОЙКИ
# =========================================================
SETTINGS = {
    "log_level": "INFO",
    "log_file": "logs/bot.log",
    "db_path": "manager.db",
    "sessions_dir": ".",
}