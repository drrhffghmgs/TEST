"""
==========================================================
 سرویس ریل‌وی - بخش‌های: سلف اکانت | پیام همگانی | چالش لایکی
==========================================================
این سرویس با Worker کلادفلر (که وبهوک اصلی بات رو داره) هماهنگ کار می‌کنه:
Worker آپدیت‌های مربوط به /self ، /likey ، /broadcast و کال‌بک‌های اونها رو
به اندپوینت POST /telegram-update همینجا فوروارد می‌کنه.

⚠️ نکات امنیتی مهم:
 - Session String هر کاربر معادل رمز کامل اکانت تلگرامشه. اینجا داخل
   Redis نگه‌داری می‌شه؛ حتماً از یه Upstash Redis اختصاصی و خصوصی استفاده کن
   و دسترسی بهش رو محدود نگه دار.
 - سلف فقط با درخواست و تایید صریح خود کاربر روی اکانت خودش فعال می‌شه؛
   قابلیتی برای ارسال خودکار پیام/بنر در گروه‌های دیگر یا جوین انبوه در
   این سرویس پیاده نشده (این کارها نقض قوانین ضداسپم تلگرامه).

اجرا روی ریل‌وی: همین یک فایل + requirements.txt کافیه.
"""

import os
import re
import json
import time
import asyncio
import datetime
from typing import Optional

import httpx
from fastapi import FastAPI, Request, Header, HTTPException
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from telethon.tl.functions.account import UpdateProfileRequest

# -------------------- تنظیمات از ENV --------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = os.environ["ADMIN_ID"]
API_ID = int(os.environ["TG_API_ID"])          # از my.telegram.org
API_HASH = os.environ["TG_API_HASH"]           # از my.telegram.org
UPSTASH_URL = os.environ["UPSTASH_URL"]
UPSTASH_TOKEN = os.environ["UPSTASH_TOKEN"]
RAILWAY_SECRET = os.environ.get("RAILWAY_SECRET", "")

SELF_HOURLY_COST = 4000       # تومان در ساعت
BOT_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

app = FastAPI()
active_clients: dict[int, TelegramClient] = {}
login_sessions: dict[int, dict] = {}   # فلوی موقت لاگین (phone/code) - در حافظه

# ============================================================
# Redis (Upstash REST) - دقیقا هم‌ساختار با Worker کلادفلر
# ============================================================
from urllib.parse import quote

async def redis_cmd(*parts):
    url = f"{UPSTASH_URL}/" + "/".join(quote(str(p), safe="") for p in parts)
    async with httpx.AsyncClient() as c:
        r = await c.get(url, headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"})
        return r.json().get("result")

async def r_get(key):
    v = await redis_cmd("GET", key)
    return json.loads(v) if v else None

async def r_set(key, value):
    return await redis_cmd("SET", key, json.dumps(value))

async def r_set_raw(key, value):
    return await redis_cmd("SET", key, value)

async def r_get_raw(key):
    return await redis_cmd("GET", key)

async def r_del(key):
    return await redis_cmd("DEL", key)

async def r_smembers(key):
    v = await redis_cmd("SMEMBERS", key)
    return v or []

# ============================================================
# Bot API helper
# ============================================================
async def bot_send(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient() as c:
        return (await c.post(f"{BOT_API}/sendMessage", json=payload)).json()

async def bot_edit_markup(chat_id, message_id, reply_markup):
    async with httpx.AsyncClient() as c:
        return (await c.post(
            f"{BOT_API}/editMessageReplyMarkup",
            json={"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup},
        )).json()

async def bot_answer_cb(cb_id, text="", alert=False):
    async with httpx.AsyncClient() as c:
        await c.post(f"{BOT_API}/answerCallbackQuery",
                      json={"callback_query_id": cb_id, "text": text, "show_alert": alert})

async def get_chat_member(chat, user_id):
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{BOT_API}/getChatMember", json={"chat_id": chat, "user_id": user_id})
        return r.json()

# ============================================================
# ۵۰+ فونت برای اسم (تبدیل یونیکد حروف انگلیسی)
# ============================================================
FONT_MAPS = {}

def _map(name, mapping_str):
    """mapping_str: 52 کاراکتر یونیکد به ترتیب A-Z سپس a-z"""
    src = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    FONT_MAPS[name] = dict(zip(src, mapping_str))

_map("𝗕𝗼𝗹𝗱", "𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇")
_map("𝘐𝘵𝘢𝘭𝘪𝘤", "𝘈𝘉𝘊𝘋𝘌𝘍𝘎𝘏𝘐𝘑𝘒𝘓𝘔𝘕𝘖𝘗𝘘𝘙𝘚𝘛𝘜𝘝𝘞𝘟𝘠𝘡𝘢𝘣𝘤𝘥𝘦𝘧𝘨𝘩𝘪𝘫𝘬𝘭𝘮𝘯𝘰𝘱𝘲𝘳𝘴𝘵𝘶𝘷𝘸𝘹𝘺𝘻")
_map("𝙱𝚘𝚕𝚍𝙸𝚝𝚊𝚕𝚒𝚌", "𝘼𝘽𝘾𝘿𝙀𝙁𝙂𝙃𝙄𝙅𝙆𝙇𝙈𝙉𝙊𝙋𝙌𝙍𝙎𝙏𝙐𝙑𝙒𝙓𝙔𝙕𝙖𝙗𝙘𝙙𝙚𝙛𝙜𝙝𝙞𝙟𝙠𝙡𝙢𝙣𝙤𝙥𝙦𝙧𝙨𝙩𝙪𝙫𝙬𝙭𝙮𝙯")
_map("𝓢𝓬𝓻𝓲𝓹𝓽", "𝒜𝐵𝒞𝒟𝐸𝐹𝒢𝐻𝐼𝒥𝒦𝐿𝑀𝒩𝒪𝒫𝒬𝑅𝒮𝒯𝒰𝒱𝒲𝒳𝒴𝒵𝒶𝒷𝒸𝒹ℯ𝒻ℊ𝒽𝒾𝒿𝓀𝓁𝓂𝓃ℴ𝓅𝓆𝓇𝓈𝓉𝓊𝓋𝓌𝓍𝓎𝓏")
_map("𝕭𝖔𝖑𝖉𝕾𝖈𝖗𝖎𝖕𝖙", "𝓐𝓑𝓒𝓓𝓔𝓕𝓖𝓗𝓘𝓙𝓚𝓛𝓜𝓝𝓞𝓟𝓠𝓡𝓢𝓣𝓤𝓥𝓦𝓧𝓨𝓩𝓪𝓫𝓬𝓭𝓮𝓯𝓰𝓱𝓲𝓳𝓴𝓵𝓶𝓷𝓸𝓹𝓺𝓻𝓼𝓽𝓾𝓿𝔀𝔁𝔂𝔃")
_map("𝔉𝔯𝔞𝔨𝔱𝔲𝔯", "𝔄𝔅ℭ𝔇𝔈𝔉𝔊ℌℑ𝔍𝔎𝔏𝔐𝔑𝔒𝔓𝔔ℜ𝔖𝔗𝔘𝔙𝔚𝔛𝔜ℨ𝔞𝔟𝔠𝔡𝔢𝔣𝔤𝔥𝔦𝔧𝔨𝔩𝔪𝔫𝔬𝔭𝔮𝔯𝔰𝔱𝔲𝔳𝔴𝔵𝔶𝔷")
_map("𝕭𝖔𝖑𝖉𝕱𝖗𝖆𝖐𝖙𝖚𝖗", "𝕬𝕭𝕮𝕯𝕰𝕱𝕲𝕳𝕴𝕵𝕶𝕷𝕸𝕹𝕺𝕻𝕼𝕽𝕾𝕿𝖀𝖁𝖂𝖃𝖄𝖅𝖆𝖇𝖈𝖉𝖊𝖋𝖌𝖍𝖎𝖏𝖐𝖑𝖒𝖓𝖔𝖕𝖖𝖗𝖘𝖙𝖚𝖛𝖜𝖝𝖞𝖟")
_map("𝔻𝕠𝕦𝕓𝕝𝕖", "𝔸𝔹ℂ𝔻𝔼𝔽𝔾ℍ𝕀𝕁𝕂𝕃𝕄ℕ𝕆ℙℚℝ𝕊𝕋𝕌𝕍𝕎𝕏𝕐ℤ𝕒𝕓𝕔𝕕𝕖𝕗𝕘𝕙𝕚𝕛𝕜𝕝𝕞𝕟𝕠𝕡𝕢𝕣𝕤𝕥𝕦𝕧𝕨𝕩𝕪𝕫")
_map("𝙼𝚘𝚗𝚘", "𝙰𝙱𝙲𝙳𝙴𝙵𝙶𝙷𝙸𝙹𝙺𝙻𝙼𝙽𝙾𝙿𝚀𝚁𝚂𝚃𝚄𝚅𝚆𝚇𝚈𝚉𝚊𝚋𝚌𝚍𝚎𝚏𝚐𝚑𝚒𝚓𝚔𝚕𝚖𝚗𝚘𝚙𝚚𝚛𝚜𝚝𝚞𝚟𝚠𝚡𝚢𝚣")
_map("Ⓒⓘⓡⓒⓛⓔ", "ⒶⒷⒸⒹⒺⒻⒼⒽⒾⒿⓀⓁⓂⓃⓄⓅⓆⓇⓈⓉⓊⓋⓌⓍⓎⓏⓐⓑⓒⓓⓔⓕⓖⓗⓘⓙⓚⓛⓜⓝⓞⓟⓠⓡⓢⓣⓤⓥⓦⓧⓨⓩ")
_map("🅢🅠🅤🅐🅡🅔", "🅐🅑🅒🅓🅔🅕🅖🅗🅘🅙🅚🅛🅜🅝🅞🅟🅠🅡🅢🅣🅤🅥🅦🅧🅨🅩🅐🅑🅒🅓🅔🅕🅖🅗🅘🅙🅚🅛🅜🅝🅞🅟🅠🅡🅢🅣🅤🅥🅦🅧🅨🅩")
_map("𝐒𝐞𝐫𝐢𝐟𝐁𝐨𝐥𝐝", "𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝐚𝐛𝐜𝐝𝐞𝐟𝐠𝐡𝐢𝐣𝐤𝐥𝐦𝐧𝐨𝐩𝐪𝐫𝐬𝐭𝐮𝐯𝐰𝐱𝐲𝐳")
_map("𝑆𝑒𝑟𝑖𝑓𝐼𝑡𝑎𝑙𝑖𝑐", "𝐴𝐵𝐶𝐷𝐸𝐹𝐺𝐻𝐼𝐽𝐾𝐿𝑀𝑁𝑂𝑃𝑄𝑅𝑆𝑇𝑈𝑉𝑊𝑋𝑌𝑍𝑎𝑏𝑐𝑑𝑒𝑓𝑔ℎ𝑖𝑗𝑘𝑙𝑚𝑛𝑜𝑝𝑞𝑟𝑠𝑡𝑢𝑣𝑤𝑥𝑦𝑧")
# افکت‌های ساده متنی (بدون تغییر حروف پایه، فقط پسوند/پیشوند) برای رسیدن به بیش از ۵۰ گزینه
SIMPLE_STYLES = [
    ("★{}★", "استار"), ("『{}』", "کروشه‌ژاپنی"), ("«{}»", "گیومه"), ("➤{}", "پیکان"),
    ("✿{}✿", "گل"), ("⚡{}⚡", "برق"), ("☾{}☽", "ماه"), ("♛{}♛", "تاج"),
    ("▄︻{}══➤", "تفنگ"), ("🔥{}🔥", "آتش"), ("꧁{}꧂", "زینتی"), ("•.,¸¸,.•*`{}`*•.,¸¸,.•", "امواج"),
    (" {} ", "فاصله‌دار"), ("『{}』", "قاب"), ("○{}○", "دایره‌ساده"), ("[{}]", "کروشه"),
    ("(๑•́ω•̀๑){}", "بامزه"), ("彡{}彡", "سایه"), ("卂{}乂", "بلاک"), ("『★{}★』", "قاب‌استار"),
]

def build_font_list(name: str):
    """برمی‌گردونه لیستی از (عنوان فونت، متن تبدیل‌شده)"""
    out = []
    for font_name, table in FONT_MAPS.items():
        out.append((font_name, "".join(table.get(ch, ch) for ch in name)))
    for tpl, label in SIMPLE_STYLES:
        out.append((label, tpl.format(name)))
    for tpl, label in SIMPLE_STYLES:
        # ترکیب با یکی از فونت‌های یونیکد برای تنوع بیشتر (مجموع از ۵۰ رد می‌شه)
        bold = "".join(FONT_MAPS["𝗕𝗼𝗹𝗱"].get(ch, ch) for ch in name)
        out.append((label + "+Bold", tpl.format(bold)))
    return out  # حدود ۵۴+ آیتم

# ============================================================
# فلوی ثبت‌نام سلف (لاگین با شماره)
# ============================================================
async def self_menu_kb():
    return {"inline_keyboard": [
        [{"text": "🔐 ورود به اکانت", "callback_data": "self:login"}],
        [{"text": "🕒 روشن/خاموش ساعت کنار اسم", "callback_data": "self:toggle_clock"}],
        [{"text": "🔤 تغییر اسم با فونت", "callback_data": "self:set_font_name"}],
        [{"text": "🔒 عضویت اجباری پیوی", "callback_data": "self:set_forced_pm"}],
        [{"text": "💬 تنظیم پیام خودکار", "callback_data": "self:set_autoreply"}],
        [{"text": "💳 وضعیت / موجودی", "callback_data": "self:status"}],
    ]}

async def handle_self_message(msg):
    chat_id = msg["chat"]["id"]
    user_id = msg["from"]["id"]
    text = msg.get("text", "")

    if text == "/self":
        await bot_send(chat_id,
            f"🔷 <b>بخش سلف</b>\n\nهزینه فعال‌سازی: ۱۰۰٬۰۰۰ تومان (فقط از کیف‌پول)\nهزینه نگه‌داری: {SELF_HOURLY_COST:,} تومان در ساعت\n\nسلف با ورود به اکانت شخصی خودت فعال می‌شه (فقط با درخواست و اطلاع خودت).",
            await self_menu_kb())
        return

    state = await r_get_raw(f"self_state:{chat_id}")

    if state == "await_phone":
        phone = text.strip()
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        try:
            sent = await client.send_code_request(phone)
            login_sessions[chat_id] = {"client": client, "phone": phone, "phone_code_hash": sent.phone_code_hash}
            await r_set_raw(f"self_state:{chat_id}", "await_code")
            await bot_send(chat_id, "کد ارسال‌شده به تلگرامت رو بفرست:")
        except Exception as e:
            await bot_send(chat_id, f"خطا در ارسال کد: {e}")
            await r_del(f"self_state:{chat_id}")
        return

    if state == "await_code":
        sess = login_sessions.get(chat_id)
        if not sess:
            await bot_send(chat_id, "فلوی لاگین منقضی شده، دوباره /self رو بزن.")
            await r_del(f"self_state:{chat_id}")
            return
        try:
            await sess["client"].sign_in(sess["phone"], text.strip(), phone_code_hash=sess["phone_code_hash"])
            await finish_login(chat_id, user_id, sess["client"])
        except SessionPasswordNeededError:
            await r_set_raw(f"self_state:{chat_id}", "await_password")
            await bot_send(chat_id, "اکانتت رمز دومرحله‌ای داره. رمزو بفرست:")
        except PhoneCodeInvalidError:
            await bot_send(chat_id, "کد اشتباهه. دوباره بفرست:")
        return

    if state == "await_password":
        sess = login_sessions.get(chat_id)
        try:
            await sess["client"].sign_in(password=text.strip())
            await finish_login(chat_id, user_id, sess["client"])
        except Exception as e:
            await bot_send(chat_id, f"رمز اشتباهه یا خطا: {e}")
        return

    if state == "await_font_name":
        await r_del(f"self_state:{chat_id}")
        fonts = build_font_list(re.sub(r"[^A-Za-z0-9 ]", "", text)[:20] or "Name")
        buf = []
        for i, (label, styled) in enumerate(fonts[:60]):
            buf.append(f"{i+1}. {styled}  ({label})")
        chunk = "\n".join(buf)
        await bot_send(chat_id, f"یکی رو انتخاب کن و شماره‌ش رو بفرست:\n\n{chunk}")
        await r_set(f"self_fonts:{chat_id}", fonts)
        await r_set_raw(f"self_state:{chat_id}", "await_font_pick")
        return

    if state == "await_font_pick":
        fonts = await r_get(f"self_fonts:{chat_id}")
        try:
            idx = int(text.strip()) - 1
            label, styled = fonts[idx]
        except Exception:
            await bot_send(chat_id, "شماره نامعتبره.")
            return
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["base_name"] = styled
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        client = active_clients.get(user_id)
        if client:
            await client(UpdateProfileRequest(first_name=styled))
        await bot_send(chat_id, f"✅ اسمت تنظیم شد: {styled}")
        return

    if state == "await_forced_pm_channel":
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["forced_pm_channel"] = text.strip()
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        await bot_send(chat_id, f"✅ عضویت اجباری پیوی روی «{text.strip()}» تنظیم شد.")
        return

    if state == "await_autoreply_text":
        lines = text.strip().split("\n")
        if len(lines) > 2:
            await bot_send(chat_id, "❌ پیام باید حداکثر ۲ خط باشه. دوباره بفرست:")
            return
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["autoreply"] = text.strip()
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        await bot_send(chat_id, "✅ پیام خودکار تنظیم شد.")
        return


async def finish_login(chat_id, user_id, client):
    session_str = client.session.save()
    await r_set_raw(f"self_session:{user_id}", session_str)
    await r_del(f"self_state:{chat_id}")
    login_sessions.pop(chat_id, None)

    wallet = await get_wallet(user_id)
    if wallet < 100_000:
        await bot_send(chat_id, "❌ موجودی کیف‌پولت برای فعال‌سازی سلف (۱۰۰٬۰۰۰ تومان) کافی نیست.")
        await client.disconnect()
        return
    await add_wallet(user_id, -100_000)

    await start_self_client(user_id, session_str)
    await bot_send(chat_id, "✅ سلف با موفقیت فعال شد و اکانتت متصله! از منوی /self بقیه‌ی تنظیمات رو انجام بده.")


async def handle_self_callback(cq):
    chat_id = cq["message"]["chat"]["id"]
    user_id = cq["from"]["id"]
    data = cq["data"]

    if data == "self:login":
        await r_set_raw(f"self_state:{chat_id}", "await_phone")
        await bot_send(chat_id, "شماره تلگرامت رو با فرمت بین‌المللی بفرست (مثال: +989121234567):")
    elif data == "self:toggle_clock":
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["clock_on"] = not prof.get("clock_on", False)
        await r_set(f"self_profile:{user_id}", prof)
        await bot_send(chat_id, f"ساعت کنار اسم: {'روشن ✅' if prof['clock_on'] else 'خاموش ❌'}")
    elif data == "self:set_font_name":
        await r_set_raw(f"self_state:{chat_id}", "await_font_name")
        await bot_send(chat_id, "اسمت رو به انگلیسی بفرست:")
    elif data == "self:set_forced_pm":
        await r_set_raw(f"self_state:{chat_id}", "await_forced_pm_channel")
        await bot_send(chat_id, "یوزرنیم کانالی که میخوای عضویتش اجباری بشه رو بفرست (مثال: @channel):")
    elif data == "self:set_autoreply":
        await r_set_raw(f"self_state:{chat_id}", "await_autoreply_text")
        await bot_send(chat_id, "متن پیام خودکار رو بفرست (حداکثر ۲ خط):")
    elif data == "self:status":
        wallet = await get_wallet(user_id)
        active = user_id in active_clients
        await bot_send(chat_id, f"وضعیت سلف: {'فعال ✅' if active else 'غیرفعال ❌'}\n💰 موجودی: {wallet:,} تومان\n💸 هزینه ساعتی: {SELF_HOURLY_COST:,} تومان")
    await bot_answer_cb(cq["id"])

# ============================================================
# موتور سلف (Telethon) - ساعت کنار اسم / پیوی اجباری / پاسخ خودکار
# ============================================================
async def start_self_client(user_id: int, session_str: str):
    if user_id in active_clients:
        return
    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        return
    active_clients[user_id] = client

    @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def _pm_handler(event):
        prof = await r_get(f"self_profile:{user_id}") or {}
        sender_id = event.sender_id

        forced = prof.get("forced_pm_channel")
        if forced:
            r = await get_chat_member(forced, sender_id)
            status = (r.get("result") or {}).get("status")
            if status not in ("member", "administrator", "creator"):
                link = f"https://t.me/{forced.lstrip('@')}"
                await event.reply(f"برای گفت‌وگو با من باید عضو این کانال بشی:\n{link}")
                return

        autoreply = prof.get("autoreply")
        if autoreply:
            await event.reply(autoreply)

    asyncio.create_task(clock_loop(user_id))


async def clock_loop(user_id: int):
    while user_id in active_clients:
        prof = await r_get(f"self_profile:{user_id}") or {}
        client = active_clients.get(user_id)
        if client and prof.get("clock_on"):
            base = prof.get("base_name", "")
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=3, minutes=30)  # تهران
            clock_str = now.strftime("%H:%M")
            try:
                await client(UpdateProfileRequest(first_name=f"{base} {clock_str}".strip()))
            except Exception:
                pass
        await asyncio.sleep(60)


async def hourly_billing_loop():
    while True:
        await asyncio.sleep(3600)
        for user_id in list(active_clients.keys()):
            wallet = await get_wallet(user_id)
            if wallet < SELF_HOURLY_COST:
                client = active_clients.pop(user_id, None)
                if client:
                    await client.disconnect()
                await bot_send(user_id, "❌ موجودی کافی نبود، سلف غیرفعال شد. برای فعال‌سازی مجدد کیف‌پولت رو شارژ کن.")
                continue
            await add_wallet(user_id, -SELF_HOURLY_COST)

# ============================================================
# کیف‌پول (هم‌ساختار با Worker)
# ============================================================
async def get_wallet(user_id) -> int:
    u = await r_get(f"user:{user_id}")
    return (u or {}).get("balance", 0)

async def add_wallet(user_id, delta):
    u = await r_get(f"user:{user_id}") or {"id": user_id, "balance": 0}
    u["balance"] = u.get("balance", 0) + delta
    await r_set(f"user:{user_id}", u)

# ============================================================
# پیام همگانی (Broadcast)
# ============================================================
async def handle_broadcast_flow(update):
    if "callback_query" in update:
        cq = update["callback_query"]
        chat_id = cq["message"]["chat"]["id"]
        if str(cq["from"]["id"]) != str(ADMIN_ID):
            return
        await r_set_raw(f"bc_state:{chat_id}", "await_text")
        await bot_send(chat_id, "متن پیام همگانی رو بفرست (برای همه کاربران ارسال میشه):")
        await bot_answer_cb(cq["id"])
        return

    msg = update["message"]
    chat_id = msg["chat"]["id"]
    if str(msg["from"]["id"]) != str(ADMIN_ID):
        return
    state = await r_get_raw(f"bc_state:{chat_id}")
    if state != "await_text":
        return
    await r_del(f"bc_state:{chat_id}")
    text = msg["text"]
    users = await r_smembers("users:all")
    await bot_send(chat_id, f"شروع ارسال به {len(users)} کاربر...")
    sent, failed = 0, 0
    for uid in users:
        try:
            res = await bot_send(uid, text)
            if res.get("ok"):
                sent += 1
            else:
                failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)  # جلوگیری از فلود لیمیت تلگرام
    await bot_send(chat_id, f"✅ پیام همگانی تموم شد.\nموفق: {sent}\nناموفق: {failed}")

# ============================================================
# چالش لایکی
# ============================================================
async def handle_likey(update):
    if "callback_query" in update:
        cq = update["callback_query"]
        data = cq["data"]
        chat_id = cq["message"]["chat"]["id"]
        user_id = cq["from"]["id"]

        if data == "adm_likey":
            if str(user_id) != str(ADMIN_ID):
                return
            await r_set_raw(f"likey_state:{chat_id}", "await_channel")
            await bot_send(chat_id, "یوزرنیم کانالی که ربات توش ادمینه رو بفرست (باید ربات ادمین کانال باشه):")
            await bot_answer_cb(cq["id"])
            return

        if data.startswith("likey:"):
            post_id = data.split(":")[1]
            post = await r_get(f"likey_post:{post_id}")
            if not post:
                await bot_answer_cb(cq["id"], "این پست دیگه معتبر نیست.", True)
                return
            r = await get_chat_member(post["channel"], user_id)
            status = (r.get("result") or {}).get("status")
            if status not in ("member", "administrator", "creator"):
                await bot_answer_cb(cq["id"], "برای لایک کردن باید عضو کانال باشی!", True)
                return
            if user_id in post["likes"]:
                await bot_answer_cb(cq["id"], "قبلاً لایک کردی ✅")
                return
            post["likes"].append(user_id)
            await r_set(f"likey_post:{post_id}", post)
            kb = {"inline_keyboard": [[{"text": f"❤️ {len(post['likes'])}", "callback_data": data}]]}
            await bot_edit_markup(post["channel"], post["message_id"], kb)
            await bot_answer_cb(cq["id"], "لایک ثبت شد ❤️")
            return
        return

    msg = update["message"]
    chat_id = msg["chat"]["id"]
    user_id = msg["from"]["id"]
    if str(user_id) != str(ADMIN_ID):
        return
    state = await r_get_raw(f"likey_state:{chat_id}")

    if state == "await_channel":
        await r_set_raw(f"likey_channel:{chat_id}", msg["text"].strip())
        await r_set_raw(f"likey_state:{chat_id}", "await_name")
        await bot_send(chat_id, "حالا اسمی که میخوای پست بشه رو بفرست:")
        return

    if state == "await_name":
        channel = await r_get_raw(f"likey_channel:{chat_id}")
        post_id = str(int(time.time() * 1000))
        async with httpx.AsyncClient() as c:
            res = (await c.post(f"{BOT_API}/sendMessage", json={
                "chat_id": channel, "text": msg["text"],
                "reply_markup": {"inline_keyboard": [[{"text": "❤️ 0", "callback_data": f"likey:{post_id}"}]]},
            })).json()
        if not res.get("ok"):
            await bot_send(chat_id, f"خطا در ارسال به کانال: {res.get('description')}")
            return
        message_id = res["result"]["message_id"]
        await r_set(f"likey_post:{post_id}", {
            "channel": channel, "message_id": message_id, "name": msg["text"],
            "owner_id": user_id, "likes": [],
        })
        await r_del(f"likey_state:{chat_id}")
        await bot_send(chat_id, "✅ پست چالش لایکی ارسال شد.")
        return

# ============================================================
# اندپوینت اصلی - دریافت آپدیت‌های فوروارد شده از Worker
# ============================================================
@app.post("/telegram-update")
async def telegram_update(req: Request, x_secret: Optional[str] = Header(None)):
    if RAILWAY_SECRET and x_secret != RAILWAY_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    update = await req.json()

    if "message" in update:
        text = update["message"].get("text", "")
        chat_id = update["message"]["chat"]["id"]
        bc_state = await r_get_raw(f"bc_state:{chat_id}")
        likey_state = await r_get_raw(f"likey_state:{chat_id}")
        if text.startswith("/broadcast") or bc_state:
            await handle_broadcast_flow(update)
        elif text.startswith("/likey") or likey_state:
            await handle_likey(update)
        else:
            await handle_self_message(update["message"])

    elif "callback_query" in update:
        cq = update["callback_query"]
        data = cq.get("data", "")
        if data.startswith("self:"):
            await handle_self_callback(cq)
        elif data.startswith("likey:") or data == "adm_likey":
            await handle_likey(update)
        elif data == "bc:send" or data == "adm_broadcast":
            await handle_broadcast_flow(update)

    return {"ok": True}


@app.on_event("startup")
async def on_startup():
    # اتصال مجدد به سلف‌های قبلاً فعال (در صورت ری‌استارت سرویس)
    asyncio.create_task(hourly_billing_loop())


@app.get("/")
async def health():
    return {"status": "running", "active_selfs": len(active_clients)}
