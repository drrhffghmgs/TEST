# -*- coding: utf-8 -*-
"""
ربات فروش پنل سنایی + کیف پول + زیرمجموعه‌گیری + تبچی + پنل مدیریت
تک‌فایل | python-telegram-bot v21 | SQLite
"""

import logging, os, random, sqlite3, time, html
from telegram import Update, InlineKeyboardButton as B, InlineKeyboardMarkup as M
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                          MessageHandler, ContextTypes, filters)

# ------------------------- تنظیمات -------------------------
TOKEN        = os.environ.get("BOT_TOKEN", "")
ADMIN_ID     = int(os.environ.get("ADMIN_ID", "8248647747"))
LOG_CHANNEL  = os.environ.get("LOG_CHANNEL", "@starsdarkconfig")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "kanfingfribot01").lstrip("@")
MAIN_CHANNEL = os.environ.get("MAIN_CHANNEL", "@kanfingfree")
DB_PATH      = os.environ.get("DB_PATH", "bot.db")

REF_REWARD   = 70_000      # پاداش هر زیرمجموعه
ADS_PRICE    = 100_000     # هزینه فعال‌سازی تبچی
ADS_HOURLY   = 4_000       # کسر ساعتی تبچی
MIN_INTERVAL = 300         # حداقل فاصله ارسال بنر (ثانیه)

PLANS = {
    "p500": ("پنل سنایی ۵۰۰ گیگ",   "500 GB", 1_300_000),
    "p800": ("پنل سنایی ۸۰۰ گیگ",   "800 GB", 1_700_000),
    "p1tb": ("پنل سنایی ۱ ترابایت", "1 TB",   3_000_000),
}

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger("bot")

# ------------------------- دیتابیس -------------------------
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")

db.executescript("""
CREATE TABLE IF NOT EXISTS users(
  user_id INTEGER PRIMARY KEY, username TEXT, name TEXT,
  balance INTEGER DEFAULT 0, referrer INTEGER, verified INTEGER DEFAULT 0,
  refs INTEGER DEFAULT 0, banned INTEGER DEFAULT 0, joined_at INTEGER);
CREATE TABLE IF NOT EXISTS channels(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ref TEXT, title TEXT,
  link TEXT, verify INTEGER DEFAULT 1, main INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS orders(
  id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, plan TEXT,
  title TEXT, price INTEGER, status TEXT DEFAULT 'pending', created_at INTEGER);
CREATE TABLE IF NOT EXISTS ads(
  user_id INTEGER PRIMARY KEY, active INTEGER DEFAULT 0, paused INTEGER DEFAULT 0,
  src_chat INTEGER, src_msg INTEGER, interval INTEGER DEFAULT 600,
  next_charge INTEGER DEFAULT 0, last_send INTEGER DEFAULT 0, bought INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS ad_groups(
  user_id INTEGER, chat_id INTEGER, title TEXT, fails INTEGER DEFAULT 0,
  PRIMARY KEY(user_id, chat_id));
""")
db.commit()

def q(sql, args=(), one=False, write=False):
    cur = db.execute(sql, args)
    if write:
        db.commit(); return cur.lastrowid
    r = cur.fetchone() if one else cur.fetchall()
    return r

def ensure_main_channel():
    if MAIN_CHANNEL and not q("SELECT 1 FROM channels WHERE main=1", one=True):
        q("INSERT INTO channels(ref,title,link,verify,main) VALUES(?,?,?,1,1)",
          (MAIN_CHANNEL, "کانال اصلی", f"https://t.me/{MAIN_CHANNEL.lstrip('@')}"), write=True)

def get_user(uid):
    return q("SELECT * FROM users WHERE user_id=?", (uid,), one=True)

def add_balance(uid, amount):
    q("UPDATE users SET balance=balance+? WHERE user_id=?", (amount, uid), write=True)

def fmt(n):
    return f"{int(n):,} تومان"

def ref_link(uid):
    return f"https://t.me/{BOT_USERNAME}?start=ref{uid}"

# ------------------------- کیبوردها -------------------------
def main_menu(uid):
    kb = [
        [B("🔵 🛒 ساخت پنل سنایی 🔵", callback_data="panel")],
        [B("🟢🟢  ت ب چ ی  (ارسال خودکار بنر)  🟢🟢", callback_data="ads")],
        [B("🎁 زیرمجموعه‌گیری", callback_data="ref"),
         B("💰 کیف پول", callback_data="wallet")],
        [B("👤 حساب کاربری", callback_data="acc"),
         B("❓ راهنما (همه‌چیز رایگان!)", callback_data="help")],
    ]
    if uid == ADMIN_ID:
        kb.append([B("🛠 پنل مدیریت", callback_data="admin")])
    return M(kb)

BACK = M([[B("⬅️ بازگشت به منو", callback_data="home")]])

# ------------------------- جوین اجباری -------------------------
async def missing_channels(bot, uid):
    miss = []
    for ch in q("SELECT * FROM channels"):
        if not ch["verify"]:
            continue
        try:
            m = await bot.get_chat_member(ch["ref"], uid)
            if m.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
                miss.append(ch)
        except Exception as e:
            log.warning("join-check %s: %s", ch["ref"], e)
    return miss

def join_kb(miss):
    kb = [[B(f"📢 {c['title']}", url=c["link"] or f"https://t.me/{str(c['ref']).lstrip('@')}")]
          for c in miss]
    manual = q("SELECT * FROM channels WHERE verify=0")
    kb += [[B(f"🔗 {c['title']}", url=c["link"])] for c in manual]
    kb.append([B("✅ عضو شدم", callback_data="checkjoin")])
    return M(kb)

async def gate(update, context):
    """True یعنی کاربر مجاز است. در غیر این صورت پیام لازم را می‌فرستد."""
    uid = update.effective_user.id
    if uid == ADMIN_ID:
        return True
    u = get_user(uid)
    if u and u["banned"]:
        return False
    miss = await missing_channels(context.bot, uid)
    if miss:
        await send(update, "🔒 برای استفاده از ربات ابتدا در کانال‌های زیر عضو شوید:", join_kb(miss))
        return False
    if u and not u["verified"]:
        await ask_captcha(update, context)
        return False
    return True

# ------------------------- کپچا -------------------------
async def ask_captcha(update, context):
    a, b_ = random.randint(3, 19), random.randint(1, 9)
    if random.choice([True, False]):
        context.user_data["captcha"] = a + b_; text = f"{a} + {b_}"
    else:
        if b_ > a: a, b_ = b_, a
        context.user_data["captcha"] = a - b_; text = f"{a} - {b_}"
    context.user_data["state"] = "captcha"
    await send(update, f"🤖 برای تایید اینکه ربات نیستید، پاسخ را بفرستید:\n\n<b>{text} = ?</b>")

async def captcha_answer(update, context):
    txt = (update.message.text or "").strip()
    try:
        val = int(txt.replace("٠","0").replace("۰","0"))
    except ValueError:
        return await update.message.reply_text("❌ فقط عدد بفرستید.")
    if val != context.user_data.get("captcha"):
        return await ask_captcha(update, context)
    uid = update.effective_user.id
    context.user_data.pop("state", None); context.user_data.pop("captcha", None)
    q("UPDATE users SET verified=1 WHERE user_id=?", (uid,), write=True)
    u = get_user(uid)
    if u and u["referrer"]:
        add_balance(u["referrer"], REF_REWARD)
        q("UPDATE users SET refs=refs+1 WHERE user_id=?", (u["referrer"],), write=True)
        try:
            await context.bot.send_message(
                u["referrer"],
                f"🎉 یک زیرمجموعه جدید تایید شد!\n💰 {fmt(REF_REWARD)} به کیف پول شما اضافه شد.")
        except Exception:
            pass
    await update.message.reply_text("✅ تایید شد! خوش آمدید.", reply_markup=main_menu(uid))

# ------------------------- ابزار ارسال -------------------------
async def send(update, text, kb=None):
    if update.callback_query:
        try:
            return await update.callback_query.edit_message_text(
                text, reply_markup=kb, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True)
        except Exception:
            return await update.callback_query.message.reply_text(
                text, reply_markup=kb, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True)
    return await update.effective_message.reply_text(
        text, reply_markup=kb, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# ------------------------- /start -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = user.id
    u = get_user(uid)
    referrer = None
    if context.args and context.args[0].startswith("ref"):
        try:
            r = int(context.args[0][3:])
            if r != uid and get_user(r):
                referrer = r
        except ValueError:
            pass
    if not u:
        q("INSERT INTO users(user_id,username,name,referrer,verified,joined_at) VALUES(?,?,?,?,?,?)",
          (uid, user.username or "", user.full_name, referrer, 0 if referrer else 1, int(time.time())),
          write=True)
        u = get_user(uid)
    else:
        q("UPDATE users SET username=?,name=? WHERE user_id=?",
          (user.username or "", user.full_name, uid), write=True)

    miss = await missing_channels(context.bot, uid) if uid != ADMIN_ID else []
    if miss:
        return await send(update, "🔒 برای استفاده از ربات ابتدا در کانال‌های زیر عضو شوید:", join_kb(miss))
    if not u["verified"]:
        return await ask_captcha(update, context)
    await send(update,
        "👋 <b>خوش آمدید!</b>\n\n"
        "🔵 <b>ساخت پنل سنایی</b> — خرید پنل با کیف پول\n"
        "🟢 <b>تبچی</b> — ارسال خودکار بنر در گروه‌های شما\n"
        "🎁 <b>زیرمجموعه‌گیری</b> — تنها راه شارژ کیف پول\n\n"
        f"✨ همه‌چیز در این ربات <b>رایگان</b> است: با دعوت دوستان، هر زیرمجموعه {fmt(REF_REWARD)} "
        "به کیف پول شما اضافه می‌شود و با همان موجودی پنل و تبچی می‌گیرید. هیچ پرداخت نقدی لازم نیست.",
        main_menu(uid))

async def checkjoin(update, context):
    uid = update.effective_user.id
    miss = await missing_channels(context.bot, uid)
    if miss:
        await update.callback_query.answer("❌ هنوز در همه کانال‌ها عضو نیستید!", show_alert=True)
        return
    u = get_user(uid)
    if u and not u["verified"]:
        return await ask_captcha(update, context)
    await send(update, "✅ عضویت تایید شد.", main_menu(uid))

# ------------------------- ساخت پنل -------------------------
async def panel_menu(update, context):
    kb = [[B(f"🔵 {t} — {fmt(p)}", callback_data=f"buy:{k}")] for k, (t, _v, p) in PLANS.items()]
    kb.append([B("⬅️ بازگشت", callback_data="home")])
    u = get_user(update.effective_user.id)
    await send(update,
        f"🛒 <b>ساخت پنل سنایی</b>\n\n💰 موجودی شما: <b>{fmt(u['balance'] if u else 0)}</b>\n"
        "پرداخت فقط از طریق کیف پول انجام می‌شود.\n\nپلن مورد نظر را انتخاب کنید:", M(kb))

async def buy_confirm(update, context):
    key = update.callback_query.data.split(":")[1]
    t, v, p = PLANS[key]
    await send(update, f"📦 <b>{t}</b>\n💾 حجم: {v}\n💵 مبلغ: <b>{fmt(p)}</b>\n\nتایید می‌کنید؟",
               M([[B("✅ تایید و پرداخت", callback_data=f"pay:{key}")],
                  [B("⬅️ بازگشت", callback_data="panel")]]))

async def do_pay(update, context):
    key = update.callback_query.data.split(":")[1]
    t, v, p = PLANS[key]
    uid = update.effective_user.id
    u = get_user(uid)
    if not u or u["balance"] < p:
        return await send(update,
            f"❌ موجودی کافی نیست.\n💰 موجودی: {fmt(u['balance'] if u else 0)}\n"
            f"💵 نیاز: {fmt(p)}\n\nبا زیرمجموعه‌گیری کیف پول را رایگان شارژ کنید.",
            M([[B("🎁 زیرمجموعه‌گیری", callback_data="ref")], [B("⬅️ بازگشت", callback_data="panel")]]))
    add_balance(uid, -p)
    oid = q("INSERT INTO orders(user_id,plan,title,price,created_at) VALUES(?,?,?,?,?)",
            (uid, key, t, p, int(time.time())), write=True)
    code = f"ORD-{oid:05d}"
    await send(update,
        f"✅ سفارش شما ثبت شد.\n🧾 کد سفارش: <code>{code}</code>\n📦 {t}\n💵 {fmt(p)}\n\n"
        "به‌زودی توسط مدیریت تحویل داده می‌شود.", BACK)
    # اطلاع به ادمین
    uname = f"@{u['username']}" if u["username"] else "—"
    try:
        await context.bot.send_message(ADMIN_ID,
            f"🧾 <b>سفارش جدید</b>\n🆔 کد: <code>{code}</code>\n📦 {t}\n💾 {v}\n💵 {fmt(p)}\n"
            f"👤 کاربر: <code>{uid}</code> ({uname})",
            parse_mode=ParseMode.HTML,
            reply_markup=M([[B("✅ تحویل سفارش", callback_data=f"adm_deliver:{oid}")],
                            [B("❌ رد و بازگشت وجه", callback_data=f"adm_reject:{oid}")]]))
    except Exception as e:
        log.warning("admin notify: %s", e)
    # لاگ کانال (بدون نام کاربر)
    try:
        await context.bot.send_message(LOG_CHANNEL,
            f"🧾 <b>سفارش جدید</b>\n\n📦 نوع: پنل سنایی\n💾 حجم: <b>{v}</b>\n"
            f"💵 مبلغ: <b>{fmt(p)}</b>\n🆔 کد سفارش: <code>{code}</code>\n"
            f"🕐 {time.strftime('%Y/%m/%d - %H:%M')}",
            parse_mode=ParseMode.HTML,
            reply_markup=M([[B("🤖 ورود به ربات", url=f"https://t.me/{BOT_USERNAME}")]]))
    except Exception as e:
        log.warning("log channel: %s", e)

# ------------------------- کیف پول / حساب / زیرمجموعه -------------------------
async def wallet(update, context):
    u = get_user(update.effective_user.id)
    await send(update,
        f"💰 <b>کیف پول</b>\n\nموجودی: <b>{fmt(u['balance'])}</b>\n"
        f"👥 زیرمجموعه‌های تایید‌شده: <b>{u['refs']}</b>\n\n"
        "ℹ️ شارژ کیف پول فقط از طریق زیرمجموعه‌گیری انجام می‌شود.",
        M([[B("🎁 لینک زیرمجموعه‌گیری", callback_data="ref")], [B("⬅️ بازگشت", callback_data="home")]]))

async def referral(update, context):
    uid = update.effective_user.id
    u = get_user(uid)
    await send(update,
        f"🎁 <b>زیرمجموعه‌گیری</b>\n\nلینک اختصاصی شما:\n<code>{ref_link(uid)}</code>\n\n"
        f"👥 زیرمجموعه‌ها: <b>{u['refs']}</b>\n💵 پاداش هر نفر: <b>{fmt(REF_REWARD)}</b>\n\n"
        "⚠️ زیرمجموعه زمانی تایید می‌شود که کاربر جدید:\n"
        "۱) با لینک شما وارد ربات شود\n۲) در تمام کانال‌های جوین اجباری عضو شود\n"
        "۳) کپچای ریاضی را درست حل کند",
        M([[B("📤 اشتراک‌گذاری لینک",
              url=f"https://t.me/share/url?url={ref_link(uid)}&text=" + "ربات پنل سنایی رایگان")],
           [B("⬅️ بازگشت", callback_data="home")]]))

async def account(update, context):
    user = update.effective_user
    u = get_user(user.id)
    a = q("SELECT * FROM ads WHERE user_id=?", (user.id,), one=True)
    orders = q("SELECT COUNT(*) c FROM orders WHERE user_id=?", (user.id,), one=True)["c"]
    ads_state = "غیرفعال"
    if a and a["bought"]:
        ads_state = "فعال ✅" if a["active"] and not a["paused"] else "متوقف (موجودی) ⏸"
    await send(update,
        "👤 <b>حساب کاربری</b>\n\n"
        f"🆔 آیدی عددی: <code>{user.id}</code>\n"
        f"🔗 یوزرنیم: {('@'+user.username) if user.username else 'ندارد'}\n"
        f"📝 نام: {html.escape(user.full_name)}\n"
        f"🌐 زبان: {user.language_code or '—'}\n"
        f"💰 موجودی: <b>{fmt(u['balance'])}</b>\n"
        f"👥 زیرمجموعه‌ها: <b>{u['refs']}</b>\n"
        f"🛒 تعداد سفارش‌ها: <b>{orders}</b>\n"
        f"🟢 وضعیت تبچی: <b>{ads_state}</b>\n"
        f"📅 تاریخ عضویت: {time.strftime('%Y/%m/%d', time.localtime(u['joined_at'] or 0))}",
        BACK)

async def help_menu(update, context):
    await send(update,
        "❓ <b>راهنما</b>\n\n"
        f"✨ <b>همه‌چیز در این ربات رایگان است.</b> کافی است لینک اختصاصی خود را برای دوستانتان "
        f"بفرستید؛ به ازای هر عضو تایید‌شده <b>{fmt(REF_REWARD)}</b> به کیف پولتان اضافه می‌شود.\n\n"
        "🔵 <b>ساخت پنل</b>: خرید پنل سنایی فقط با کیف پول.\n"
        f"🟢 <b>تبچی</b>: فعال‌سازی {fmt(ADS_PRICE)} و سپس {fmt(ADS_HOURLY)} به‌ازای هر ساعت روشن بودن.\n"
        f"⏱ حداقل فاصله ارسال بنر: {MIN_INTERVAL} ثانیه.\n\n"
        "💡 هیچ پرداخت ریالی در ربات وجود ندارد؛ فقط زیرمجموعه‌گیری.", BACK)

# ------------------------- تبچی -------------------------
def ads_row(uid):
    a = q("SELECT * FROM ads WHERE user_id=?", (uid,), one=True)
    if not a:
        q("INSERT INTO ads(user_id) VALUES(?)", (uid,), write=True)
        a = q("SELECT * FROM ads WHERE user_id=?", (uid,), one=True)
    return a

async def ads_menu(update, context):
    uid = update.effective_user.id
    a = ads_row(uid); u = get_user(uid)
    if not a["bought"]:
        return await send(update,
            "🟢 <b>تبچی — ارسال خودکار بنر</b>\n\n"
            "بنر شما به‌صورت خودکار و در فاصله‌های زمانی دلخواه، در گروه‌هایی که ثبت کرده‌اید ارسال می‌شود.\n\n"
            f"💵 هزینه فعال‌سازی: <b>{fmt(ADS_PRICE)}</b>\n"
            f"⏳ هزینه نگه‌داری: <b>{fmt(ADS_HOURLY)}</b> برای هر ساعت روشن بودن\n"
            f"💰 موجودی شما: <b>{fmt(u['balance'])}</b>\n\n"
            "⚠️ پرداخت فقط از کیف پول (شارژ با زیرمجموعه‌گیری).",
            M([[B("🟢 خرید و فعال‌سازی تبچی", callback_data="ads_buy")],
               [B("⬅️ بازگشت", callback_data="home")]]))
    groups = q("SELECT * FROM ad_groups WHERE user_id=?", (uid,))
    state = "🟢 روشن" if a["active"] and not a["paused"] else ("⏸ متوقف (کمبود موجودی)" if a["paused"] else "🔴 خاموش")
    banner = "✅ ثبت شده" if a["src_msg"] else "❌ ثبت نشده"
    await send(update,
        f"🟢 <b>پنل تبچی</b>\n\nوضعیت: <b>{state}</b>\n🖼 بنر: {banner}\n"
        f"⏱ فاصله ارسال: <b>{a['interval']}</b> ثانیه\n👥 گروه‌های ثبت‌شده: <b>{len(groups)}</b>\n"
        f"💰 موجودی: <b>{fmt(u['balance'])}</b>\n\n"
        "📌 برای ثبت گروه: ربات را در گروه عضو کنید و به‌عنوان <b>ادمین گروه</b> عبارت "
        "<code>تنظیم بنر</code> را بفرستید.",
        M([[B("🖼 ثبت / تغییر بنر", callback_data="ads_banner")],
           [B("⏱ تنظیم زمان ارسال", callback_data="ads_interval")],
           [B("👥 گروه‌های من", callback_data="ads_groups")],
           [B("🔴 خاموش کردن" if a["active"] else "🟢 روشن کردن", callback_data="ads_toggle")],
           [B("⬅️ بازگشت", callback_data="home")]]))

async def ads_buy(update, context):
    uid = update.effective_user.id
    u = get_user(uid); a = ads_row(uid)
    if a["bought"]:
        return await ads_menu(update, context)
    if u["balance"] < ADS_PRICE:
        return await send(update,
            f"❌ موجودی کافی نیست.\n💰 موجودی: {fmt(u['balance'])}\n💵 نیاز: {fmt(ADS_PRICE)}",
            M([[B("🎁 زیرمجموعه‌گیری", callback_data="ref")], [B("⬅️ بازگشت", callback_data="ads")]]))
    add_balance(uid, -ADS_PRICE)
    q("UPDATE ads SET bought=1, active=1, paused=0, next_charge=? WHERE user_id=?",
      (int(time.time()) + 3600, uid), write=True)
    await send(update, "✅ تبچی فعال شد! حالا بنر و زمان ارسال را تنظیم کنید.",
               M([[B("🟢 ورود به پنل تبچی", callback_data="ads")]]))

async def ads_toggle(update, context):
    uid = update.effective_user.id
    a = ads_row(uid)
    if a["active"]:
        q("UPDATE ads SET active=0 WHERE user_id=?", (uid,), write=True)
    else:
        u = get_user(uid)
        if u["balance"] < ADS_HOURLY:
            return await update.callback_query.answer(
                f"موجودی کمتر از {fmt(ADS_HOURLY)} است.", show_alert=True)
        q("UPDATE ads SET active=1, paused=0, next_charge=? WHERE user_id=?",
          (int(time.time()) + 3600, uid), write=True)
    await ads_menu(update, context)

async def ads_groups(update, context):
    uid = update.effective_user.id
    gs = q("SELECT * FROM ad_groups WHERE user_id=?", (uid,))
    if not gs:
        return await send(update,
            "👥 هنوز گروهی ثبت نشده.\n\nربات را در گروه اضافه کنید و به‌عنوان ادمین گروه "
            "<code>تنظیم بنر</code> را بفرستید.", M([[B("⬅️ بازگشت", callback_data="ads")]]))
    kb = [[B(f"🗑 {g['title'][:30]}", callback_data=f"ads_delg:{g['chat_id']}")] for g in gs]
    kb.append([B("⬅️ بازگشت", callback_data="ads")])
    await send(update, "👥 <b>گروه‌های ثبت‌شده</b>\nبرای حذف، روی گروه بزنید:", M(kb))

async def ads_delg(update, context):
    cid = int(update.callback_query.data.split(":")[1])
    q("DELETE FROM ad_groups WHERE user_id=? AND chat_id=?", (update.effective_user.id, cid), write=True)
    await ads_groups(update, context)

async def group_register(update, context):
    """دستور «تنظیم بنر» داخل گروه"""
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    a = q("SELECT * FROM ads WHERE user_id=?", (user.id,), one=True)
    if not a or not a["bought"]:
        return await msg.reply_text("❌ ابتدا باید تبچی را در ربات فعال کنید.")
    try:
        cm = await context.bot.get_chat_member(chat.id, user.id)
        if cm.status not in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return await msg.reply_text("⛔️ فقط ادمین‌های گروه می‌توانند بنر را در این گروه ثبت کنند.")
    except Exception:
        return await msg.reply_text("⛔️ وضعیت شما در گروه قابل بررسی نیست.")
    q("INSERT OR REPLACE INTO ad_groups(user_id,chat_id,title,fails) VALUES(?,?,?,0)",
      (user.id, chat.id, chat.title or str(chat.id)), write=True)
    await msg.reply_text("✅ بنر در این گروه ثبت شد.")
    try:
        await context.bot.send_message(user.id,
            f"✅ <b>بنر در این گروه ثبت شد</b>\n\n👥 گروه: {html.escape(chat.title or '')}\n"
            f"⏱ فاصله ارسال فعلی: {a['interval']} ثانیه", parse_mode=ParseMode.HTML)
    except Exception:
        pass

# ------------------------- پنل مدیریت -------------------------
def is_admin(update):
    return update.effective_user.id == ADMIN_ID

async def admin_menu(update, context):
    if not is_admin(update):
        return
    await send(update, "🛠 <b>پنل مدیریت</b>",
        M([[B("📊 آمار ربات", callback_data="adm_stats")],
           [B("📢 پیام همگانی", callback_data="adm_bc")],
           [B("🧾 سفارش‌های در انتظار", callback_data="adm_orders")],
           [B("➕ افزودن کانال جوین اجباری", callback_data="adm_addch"),
            B("📋 کانال‌ها", callback_data="adm_chs")],
           [B("💳 تغییر موجودی کاربر", callback_data="adm_bal"),
            B("🔎 اطلاعات کاربر", callback_data="adm_info")],
           [B("🚫 بلاک / آنبلاک", callback_data="adm_ban")],
           [B("⬅️ بازگشت", callback_data="home")]]))

async def adm_stats(update, context):
    if not is_admin(update): return
    s = lambda sql: q(sql, one=True)["c"]
    users = s("SELECT COUNT(*) c FROM users")
    ver = s("SELECT COUNT(*) c FROM users WHERE verified=1")
    today = s(f"SELECT COUNT(*) c FROM users WHERE joined_at>{int(time.time())-86400}")
    orders = s("SELECT COUNT(*) c FROM orders")
    pend = s("SELECT COUNT(*) c FROM orders WHERE status='pending'")
    rev = q("SELECT COALESCE(SUM(price),0) c FROM orders WHERE status='done'", one=True)["c"]
    bal = q("SELECT COALESCE(SUM(balance),0) c FROM users", one=True)["c"]
    ads_a = s("SELECT COUNT(*) c FROM ads WHERE active=1 AND paused=0")
    ads_b = s("SELECT COUNT(*) c FROM ads WHERE bought=1")
    grp = s("SELECT COUNT(*) c FROM ad_groups")
    chs = s("SELECT COUNT(*) c FROM channels")
    await send(update,
        f"📊 <b>آمار ربات</b>\n\n👥 کاربران: <b>{users}</b> (تایید‌شده: {ver})\n"
        f"🆕 ۲۴ ساعت اخیر: <b>{today}</b>\n🧾 سفارش‌ها: <b>{orders}</b> (در انتظار: {pend})\n"
        f"💵 فروش تحویل‌شده: <b>{fmt(rev)}</b>\n💰 مجموع کیف پول‌ها: <b>{fmt(bal)}</b>\n"
        f"🟢 تبچی فعال: <b>{ads_a}</b> / خریداری‌شده: {ads_b}\n"
        f"👥 گروه‌های تبچی: <b>{grp}</b>\n📢 کانال‌های جوین اجباری: <b>{chs}</b>",
        M([[B("⬅️ بازگشت", callback_data="admin")]]))

async def adm_orders(update, context):
    if not is_admin(update): return
    rows = q("SELECT * FROM orders WHERE status='pending' ORDER BY id DESC LIMIT 20")
    if not rows:
        return await send(update, "🧾 سفارش در انتظاری نیست.", M([[B("⬅️ بازگشت", callback_data="admin")]]))
    kb = [[B(f"ORD-{r['id']:05d} | {r['title']}", callback_data=f"adm_deliver:{r['id']}")] for r in rows]
    kb.append([B("⬅️ بازگشت", callback_data="admin")])
    await send(update, "🧾 <b>سفارش‌های در انتظار</b>", M(kb))

async def adm_deliver(update, context):
    if not is_admin(update): return
    oid = int(update.callback_query.data.split(":")[1])
    context.user_data["state"] = f"deliver:{oid}"
    await send(update, f"✍️ پاسخ/کانفیگ سفارش <code>ORD-{oid:05d}</code> را بفرستید (متن، عکس یا فایل).",
               M([[B("⬅️ انصراف", callback_data="admin")]]))

async def adm_reject(update, context):
    if not is_admin(update): return
    oid = int(update.callback_query.data.split(":")[1])
    o = q("SELECT * FROM orders WHERE id=?", (oid,), one=True)
    if not o or o["status"] != "pending":
        return await update.callback_query.answer("سفارش معتبر نیست.", show_alert=True)
    add_balance(o["user_id"], o["price"])
    q("UPDATE orders SET status='rejected' WHERE id=?", (oid,), write=True)
    try:
        await context.bot.send_message(o["user_id"],
            f"❌ سفارش <code>ORD-{oid:05d}</code> رد شد و {fmt(o['price'])} به کیف پول شما بازگشت.",
            parse_mode=ParseMode.HTML)
    except Exception: pass
    await send(update, "✅ سفارش رد و وجه بازگردانده شد.", M([[B("⬅️ بازگشت", callback_data="admin")]]))

async def adm_chs(update, context):
    if not is_admin(update): return
    rows = q("SELECT * FROM channels")
    if not rows:
        return await send(update, "📢 کانالی ثبت نشده.", M([[B("⬅️ بازگشت", callback_data="admin")]]))
    kb = [[B(f"🗑 {r['title']} ({r['ref'] or 'لینک'})" + (" ⭐️" if r["main"] else ""),
             callback_data=f"adm_delch:{r['id']}")] for r in rows]
    kb.append([B("⬅️ بازگشت", callback_data="admin")])
    await send(update, "📋 <b>کانال‌های جوین اجباری</b>\nبرای حذف روی مورد بزنید:", M(kb))

async def adm_delch(update, context):
    if not is_admin(update): return
    q("DELETE FROM channels WHERE id=?", (int(update.callback_query.data.split(":")[1]),), write=True)
    await adm_chs(update, context)

async def adm_addch(update, context):
    if not is_admin(update): return
    context.user_data["state"] = "addch"
    await send(update,
        "➕ <b>افزودن کانال / گروه جوین اجباری</b>\n\nقالب ارسال:\n"
        "<code>نام نمایشی | @username</code>\n"
        "<code>نام نمایشی | https://t.me/+AbCdEf</code>  (لینک ظرفیتی/خصوصی)\n\n"
        "برای کانال یا گروه با یوزرنیم، ربات باید ادمین باشد تا عضویت چک شود.\n"
        "لینک‌های ظرفیتی فقط نمایش داده می‌شوند و قابل بررسی خودکار نیستند.",
        M([[B("⬅️ انصراف", callback_data="admin")]]))

async def adm_bc(update, context):
    if not is_admin(update): return
    context.user_data["state"] = "bc"
    n = q("SELECT COUNT(*) c FROM users", one=True)["c"]
    await send(update, f"📢 پیام همگانی را بفرستید (هر نوع محتوا).\n👥 تعداد مخاطبان: <b>{n}</b>",
               M([[B("⬅️ انصراف", callback_data="admin")]]))

async def adm_bal(update, context):
    if not is_admin(update): return
    context.user_data["state"] = "bal"
    await send(update, "💳 قالب: <code>آیدی_عددی مقدار</code>\nمثال: <code>123456789 70000</code> "
                       "یا <code>123456789 -50000</code>", M([[B("⬅️ انصراف", callback_data="admin")]]))

async def adm_info(update, context):
    if not is_admin(update): return
    context.user_data["state"] = "info"
    await send(update, "🔎 آیدی عددی کاربر را بفرستید.", M([[B("⬅️ انصراف", callback_data="admin")]]))

async def adm_ban(update, context):
    if not is_admin(update): return
    context.user_data["state"] = "ban"
    await send(update, "🚫 آیدی عددی کاربر را بفرستید (بلاک/آنبلاک سوییچ می‌شود).",
               M([[B("⬅️ انصراف", callback_data="admin")]]))

async def broadcast_job(context: ContextTypes.DEFAULT_TYPE):
    d = context.job.data
    ids = [r["user_id"] for r in q("SELECT user_id FROM users")]
    ok = fail = 0
    for i, uid in enumerate(ids):
        try:
            await context.bot.copy_message(uid, d["chat"], d["msg"])
            ok += 1
        except Exception:
            fail += 1
        if i % 25 == 24:
            await __import__("asyncio").sleep(1)
    await context.bot.send_message(ADMIN_ID,
        f"📢 <b>پیام همگانی تمام شد</b>\n✅ ارسال‌شده: <b>{ok}</b>\n❌ ناموفق: <b>{fail}</b>\n"
        f"👥 کل: <b>{len(ids)}</b>", parse_mode=ParseMode.HTML)

# ------------------------- هندلر متن (استیت‌ها) -------------------------
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    st = context.user_data.get("state")

    if st == "captcha":
        return await captcha_answer(update, context)

    if uid == ADMIN_ID and st:
        msg = update.effective_message
        if st == "bc":
            context.user_data.pop("state")
            context.application.job_queue.run_once(
                broadcast_job, 0, data={"chat": msg.chat_id, "msg": msg.message_id})
            return await msg.reply_text("📢 ارسال شروع شد. در پایان گزارش می‌دهم.")
        if st == "addch":
            context.user_data.pop("state")
            raw = (msg.text or "").strip()
            if "|" not in raw:
                return await msg.reply_text("❌ قالب اشتباه است. مثال: کانال اصلی | @channel")
            title, ref = [x.strip() for x in raw.split("|", 1)]
            if ref.startswith("@"):
                q("INSERT INTO channels(ref,title,link,verify) VALUES(?,?,?,1)",
                  (ref, title, f"https://t.me/{ref[1:]}"), write=True)
            else:
                q("INSERT INTO channels(ref,title,link,verify) VALUES(?,?,?,0)",
                  (None, title, ref), write=True)
            return await msg.reply_text(f"✅ «{title}» اضافه شد.", reply_markup=M([[B("📋 کانال‌ها", callback_data="adm_chs")]]))
        if st == "bal":
            context.user_data.pop("state")
            try:
                tid, amt = (msg.text or "").split()
                tid, amt = int(tid), int(amt)
            except Exception:
                return await msg.reply_text("❌ قالب اشتباه.")
            if not get_user(tid):
                return await msg.reply_text("❌ کاربر یافت نشد.")
            add_balance(tid, amt)
            u = get_user(tid)
            try:
                await context.bot.send_message(tid,
                    ("💰 " + fmt(amt) + " به کیف پول شما اضافه شد.") if amt > 0
                    else ("💰 " + fmt(-amt) + " از کیف پول شما کسر شد."))
            except Exception: pass
            return await msg.reply_text(f"✅ انجام شد. موجودی جدید: {fmt(u['balance'])}")
        if st in ("info", "ban"):
            context.user_data.pop("state")
            try:
                tid = int((msg.text or "").strip())
            except ValueError:
                return await msg.reply_text("❌ آیدی نامعتبر.")
            u = get_user(tid)
            if not u:
                return await msg.reply_text("❌ کاربر یافت نشد.")
            if st == "ban":
                nb = 0 if u["banned"] else 1
                q("UPDATE users SET banned=? WHERE user_id=?", (nb, tid), write=True)
                return await msg.reply_text("🚫 بلاک شد." if nb else "✅ آنبلاک شد.")
            o = q("SELECT COUNT(*) c FROM orders WHERE user_id=?", (tid,), one=True)["c"]
            return await msg.reply_text(
                f"🔎 <code>{tid}</code>\n📝 {html.escape(u['name'] or '')}\n"
                f"🔗 {('@'+u['username']) if u['username'] else '—'}\n"
                f"💰 {fmt(u['balance'])}\n👥 زیرمجموعه: {u['refs']}\n🛒 سفارش: {o}\n"
                f"⛔️ بلاک: {'بله' if u['banned'] else 'خیر'}", parse_mode=ParseMode.HTML)
        if st.startswith("deliver:"):
            oid = int(st.split(":")[1]); context.user_data.pop("state")
            o = q("SELECT * FROM orders WHERE id=?", (oid,), one=True)
            if not o:
                return await msg.reply_text("❌ سفارش یافت نشد.")
            try:
                await context.bot.send_message(o["user_id"],
                    f"📦 <b>سفارش شما آماده است</b>\n🧾 کد: <code>ORD-{oid:05d}</code>\n📦 {o['title']}",
                    parse_mode=ParseMode.HTML)
                await context.bot.copy_message(o["user_id"], msg.chat_id, msg.message_id)
                q("UPDATE orders SET status='done' WHERE id=?", (oid,), write=True)
                return await msg.reply_text("✅ تحویل داده شد.")
            except Exception as e:
                return await msg.reply_text(f"❌ ارسال ناموفق: {e}")

    # استیت‌های تبچی
    if st == "ads_banner":
        context.user_data.pop("state")
        q("UPDATE ads SET src_chat=?, src_msg=? WHERE user_id=?",
          (update.effective_message.chat_id, update.effective_message.message_id, uid), write=True)
        return await update.effective_message.reply_text(
            "✅ بنر ثبت شد.", reply_markup=M([[B("🟢 پنل تبچی", callback_data="ads")]]))
    if st == "ads_interval":
        txt = (update.effective_message.text or "").strip()
        try:
            sec = int(txt)
        except ValueError:
            return await update.effective_message.reply_text("❌ فقط عدد (ثانیه) بفرستید.")
        if sec < MIN_INTERVAL:
            return await update.effective_message.reply_text(
                f"❌ زمان باید حتماً بالای {MIN_INTERVAL} ثانیه باشد. عدد بزرگ‌تری بفرستید.")
        context.user_data.pop("state")
        q("UPDATE ads SET interval=? WHERE user_id=?", (sec, uid), write=True)
        return await update.effective_message.reply_text(
            f"✅ فاصله ارسال روی {sec} ثانیه تنظیم شد.",
            reply_markup=M([[B("🟢 پنل تبچی", callback_data="ads")]]))

    if not await gate(update, context):
        return
    await update.effective_message.reply_text("از منوی زیر انتخاب کنید 👇", reply_markup=main_menu(uid))

async def ads_banner_ask(update, context):
    context.user_data["state"] = "ads_banner"
    await send(update, "🖼 پیام بنر خود را بفرستید (متن، عکس، ویدیو یا هر محتوایی).",
               M([[B("⬅️ بازگشت", callback_data="ads")]]))

async def ads_interval_ask(update, context):
    context.user_data["state"] = "ads_interval"
    await send(update, f"⏱ فاصله ارسال را بر حسب <b>ثانیه</b> بفرستید.\n"
                       f"⚠️ حتماً باید بالای <b>{MIN_INTERVAL}</b> ثانیه باشد.",
               M([[B("⬅️ بازگشت", callback_data="ads")]]))

# ------------------------- جاب‌های تبچی -------------------------
async def ads_sender(context: ContextTypes.DEFAULT_TYPE):
    now = int(time.time())
    for a in q("SELECT * FROM ads WHERE active=1 AND paused=0 AND bought=1 AND src_msg IS NOT NULL"):
        if now - (a["last_send"] or 0) < a["interval"]:
            continue
        gs = q("SELECT * FROM ad_groups WHERE user_id=?", (a["user_id"],))
        if not gs:
            continue
        for g in gs:
            try:
                await context.bot.copy_message(g["chat_id"], a["src_chat"], a["src_msg"])
                q("UPDATE ad_groups SET fails=0 WHERE user_id=? AND chat_id=?",
                  (a["user_id"], g["chat_id"]), write=True)
            except Exception as e:
                log.info("ads send fail %s: %s", g["chat_id"], e)
                q("UPDATE ad_groups SET fails=fails+1 WHERE user_id=? AND chat_id=?",
                  (a["user_id"], g["chat_id"]), write=True)
                if (g["fails"] or 0) + 1 >= 5:
                    q("DELETE FROM ad_groups WHERE user_id=? AND chat_id=?",
                      (a["user_id"], g["chat_id"]), write=True)
        q("UPDATE ads SET last_send=? WHERE user_id=?", (now, a["user_id"]), write=True)

async def ads_billing(context: ContextTypes.DEFAULT_TYPE):
    now = int(time.time())
    for a in q("SELECT * FROM ads WHERE bought=1 AND active=1"):
        uid = a["user_id"]
        if a["paused"]:
            u = get_user(uid)
            if u and u["balance"] >= ADS_HOURLY:
                add_balance(uid, -ADS_HOURLY)
                q("UPDATE ads SET paused=0, next_charge=? WHERE user_id=?", (now + 3600, uid), write=True)
                try:
                    await context.bot.send_message(uid,
                        f"🟢 موجودی شارژ شد؛ تبچی دوباره روشن شد. ({fmt(ADS_HOURLY)} کسر شد)")
                except Exception: pass
            continue
        if now < (a["next_charge"] or 0):
            continue
        u = get_user(uid)
        if u and u["balance"] >= ADS_HOURLY:
            add_balance(uid, -ADS_HOURLY)
            q("UPDATE ads SET next_charge=? WHERE user_id=?", ((a["next_charge"] or now) + 3600, uid), write=True)
        else:
            q("UPDATE ads SET paused=1 WHERE user_id=?", (uid,), write=True)
            try:
                await context.bot.send_message(uid,
                    f"⏸ موجودی شما برای هزینه ساعتی تبچی ({fmt(ADS_HOURLY)}) کافی نیست.\n"
                    "تبچی متوقف شد و به‌محض شارژ کیف پول خودکار روشن می‌شود.")
            except Exception: pass

# ------------------------- روتر کالبک -------------------------
ROUTES = {
    "home": lambda u, c: send(u, "🏠 منوی اصلی", main_menu(u.effective_user.id)),
    "panel": panel_menu, "wallet": wallet, "ref": referral, "acc": account, "help": help_menu,
    "checkjoin": checkjoin, "ads": ads_menu, "ads_buy": ads_buy, "ads_toggle": ads_toggle,
    "ads_groups": ads_groups, "ads_banner": ads_banner_ask, "ads_interval": ads_interval_ask,
    "admin": admin_menu, "adm_stats": adm_stats, "adm_bc": adm_bc, "adm_orders": adm_orders,
    "adm_addch": adm_addch, "adm_chs": adm_chs, "adm_bal": adm_bal, "adm_info": adm_info,
    "adm_ban": adm_ban,
}
PREFIX = {"buy": buy_confirm, "pay": do_pay, "ads_delg": ads_delg,
          "adm_deliver": adm_deliver, "adm_reject": adm_reject, "adm_delch": adm_delch}

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    qd = update.callback_query.data
    await update.callback_query.answer()
    if qd not in ("checkjoin", "home") and not await gate(update, context):
        return
    if qd in ROUTES:
        return await ROUTES[qd](update, context)
    head = qd.split(":")[0]
    if head in PREFIX:
        return await PREFIX[head](update, context)

async def on_error(update, context):
    log.error("error: %s", context.error)

# ------------------------- اجرا -------------------------
def main():
    if not TOKEN:
        raise SystemExit("BOT_TOKEN تنظیم نشده است.")
    ensure_main_channel()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("admin", admin_menu, filters=filters.ChatType.PRIVATE))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(
        filters.ChatType.GROUPS & filters.Regex(r"^\s*(تنظیم بنر|/setbanner)\s*$"), group_register))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, text_router))
    app.add_error_handler(on_error)
    jq = app.job_queue
    jq.run_repeating(ads_sender, interval=30, first=15)
    jq.run_repeating(ads_billing, interval=60, first=30)
    log.info("bot started")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
