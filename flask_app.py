# -*- coding: utf-8 -*-
"""
پل بین Flask (WSGI - همون چیزی که PythonAnywhere اجرا می‌کند) و
python-telegram-bot (که async است). این فایل رو اجرا نکن، PythonAnywhere
خودش صداش می‌زنه.
"""
import asyncio
import threading
import os

from flask import Flask, request
from telegram import Update

import bot  # همون bot.py خودت، کنار همین فایل باشد

# ---------- ساخت اپلیکیشن بات ----------
application = bot.build_app()

# ---------- یک event loop جدا که همیشه روشن می‌ماند ----------
loop = asyncio.new_event_loop()


def _start_loop():
    asyncio.set_event_loop(loop)
    loop.run_until_complete(application.initialize())
    loop.run_until_complete(application.start())   # job_queue (تبچی/بیلینگ) هم از همینجا استارت می‌شود
    loop.run_forever()


threading.Thread(target=_start_loop, daemon=True).start()

# ---------- اپ Flask ----------
flask_app = Flask(__name__)

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "change-this-secret")


@flask_app.route(f"/webhook/{WEBHOOK_SECRET}", methods=["POST"])
def telegram_webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, application.bot)
    asyncio.run_coroutine_threadsafe(application.process_update(update), loop)
    return "OK"


@flask_app.route("/")
def index():
    return "Bot is alive."
