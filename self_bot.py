"""
==========================================================
 سرویس ریل‌وی - سلف اکانت + چالش لایکی + پنل ادمین کامل
==========================================================
داده‌های کیف‌پول/کاربر/سفارش/کانال/قیمت در Worker (Cloudflare KV) است
و از طریق /api/* خونده/نوشته می‌شه. داده‌های خودِ ریل‌وی (سشن سلف،
پروفایل، پست‌های لایکی، فلوهای مکالمه) در Upstash Redis.

⚠️ Session String هر کاربر معادل رمز کامل اکانت تلگرامشه.
سلف فقط با درخواست صریح خود کاربر روی اکانت خودش فعال می‌شه.
"""

import os
import re
import time
import json
import asyncio
import datetime
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request, Header, HTTPException
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from telethon.tl.functions.account import UpdateProfileRequest, SetPrivacyRequest, GetAuthorizationsRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest
from telethon.tl.functions.contacts import BlockRequest
from telethon.tl.types import InputPrivacyKeyStatusTimestamp, InputPrivacyValueAllowAll, InputPrivacyValueDisallowAll

# -------------------- تنظیمات از ENV --------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = os.environ["ADMIN_ID"]
API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
UPSTASH_URL = os.environ["UPSTASH_URL"]
UPSTASH_TOKEN = os.environ["UPSTASH_TOKEN"]
RAILWAY_SECRET = os.environ.get("RAILWAY_SECRET", "")
WORKER_URL = os.environ["WORKER_URL"]

SELF_ACTIVATION_COST = 100_000
SELF_HOURLY_COST = 4_000
LIKEYCH_ACTIVATION_COST = 200_000
LIKEYCH_DURATION_SECONDS = 7 * 24 * 3600
BOT_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

app = FastAPI()
active_clients: dict[int, TelegramClient] = {}
login_sessions: dict[int, dict] = {}
BOT_USER_ID: Optional[int] = None  # آیدی عددی خودِ بات - برای نادیده گرفتنش تو سلف

# ============================================================
# Redis (Upstash REST)
# ============================================================
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

# ============================================================
# Worker API helpers
# ============================================================
async def worker_api(method, path, **kwargs):
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.request(method, f"{WORKER_URL}{path}", headers={"X-Secret": RAILWAY_SECRET}, **kwargs)
            try:
                data = r.json()
            except Exception:
                return {"ok": False, "error": f"invalid_response_status_{r.status_code}"}
            if not isinstance(data, dict):
                return {"ok": False, "error": "unexpected_response"}
            return data
    except Exception as e:
        return {"ok": False, "error": str(e)}

async def get_wallet(user_id) -> int:
    data = await worker_api("POST", "/api/wallet/get", json={"user_id": user_id})
    return data.get("balance", 0) if data.get("ok") else 0

async def add_wallet(user_id, delta) -> int:
    data = await worker_api("POST", "/api/wallet/add", json={"user_id": user_id, "delta": delta})
    return data.get("balance", 0) if data.get("ok") else 0

async def bulk_add_wallet(delta) -> int:
    data = await worker_api("POST", "/api/wallet/bulk-add", json={"delta": delta})
    return data.get("count", 0) if data.get("ok") else 0

async def get_all_users():
    data = await worker_api("GET", "/api/users")
    return data.get("users", []) if data.get("ok") else []

async def get_user_info(user_id):
    data = await worker_api("GET", "/api/user/get", params={"id": user_id})
    return data.get("user") if data.get("ok") else None

async def set_ban(user_id, banned):
    return await worker_api("POST", "/api/user/ban", json={"user_id": user_id, "banned": banned})

async def get_forced_channels():
    data = await worker_api("GET", "/api/forced-channels")
    return data.get("channels", []) if data.get("ok") else []

async def add_forced_channel(name, chat):
    return await worker_api("POST", "/api/forced-channels/add", json={"name": name, "chat": chat})

async def remove_forced_channel(chat):
    return await worker_api("POST", "/api/forced-channels/remove", json={"chat": chat})

async def get_panel_prices():
    data = await worker_api("GET", "/api/panel-prices")
    return data.get("panels", []) if data.get("ok") else []

async def set_panel_price(panel_id, price):
    return await worker_api("POST", "/api/panel-prices/set", json={"id": panel_id, "price": price})

async def get_pending_orders():
    data = await worker_api("GET", "/api/orders", params={"status": "pending"})
    return data.get("orders", []) if data.get("ok") else []

async def deliver_order(order_id, text):
    return await worker_api("POST", "/api/order/deliver", json={"order_id": order_id, "text": text})

async def cancel_order(order_id):
    return await worker_api("POST", "/api/order/cancel", json={"order_id": order_id})

async def get_referred_users():
    data = await worker_api("GET", "/api/referred-users")
    return data.get("list", []) if data.get("ok") else []

async def reclaim_referral(user_id):
    return await worker_api("POST", "/api/referral/reclaim", json={"user_id": user_id})

async def get_stats():
    data = await worker_api("GET", "/api/stats")
    return data if data.get("ok") else {}

async def set_route_state(chat_id, active: bool):
    await worker_api("POST", "/api/route/set", json={"chat_id": chat_id, "active": active})

# ============================================================
# Bot API helper
# ============================================================
async def bot_send(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient(timeout=20) as c:
        return (await c.post(f"{BOT_API}/sendMessage", json=payload)).json()

async def bot_edit_text(chat_id, message_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient(timeout=20) as c:
        return (await c.post(f"{BOT_API}/editMessageText", json=payload)).json()

async def bot_send_document(chat_id, file_bytes, filename, caption=""):
    async with httpx.AsyncClient(timeout=30) as c:
        files = {"document": (filename, file_bytes, "application/json")}
        data = {"chat_id": str(chat_id), "caption": caption}
        return (await c.post(f"{BOT_API}/sendDocument", data=data, files=files)).json()

async def bot_edit_markup(chat_id, message_id, reply_markup):
    async with httpx.AsyncClient(timeout=20) as c:
        return (await c.post(f"{BOT_API}/editMessageReplyMarkup",
                              json={"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup})).json()

async def bot_answer_cb(cb_id, text="", alert=False):
    async with httpx.AsyncClient(timeout=20) as c:
        await c.post(f"{BOT_API}/answerCallbackQuery", json={"callback_query_id": cb_id, "text": text, "show_alert": alert})

async def get_chat_member(chat, user_id):
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(f"{BOT_API}/getChatMember", json={"chat_id": chat, "user_id": user_id})
        return r.json()

async def get_file_bytes(file_id: str) -> Optional[bytes]:
    async with httpx.AsyncClient(timeout=30) as c:
        r = (await c.get(f"{BOT_API}/getFile", params={"file_id": file_id})).json()
        if not r.get("ok"):
            return None
        file_path = r["result"]["file_path"]
        file_res = await c.get(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}")
        return file_res.content

# ============================================================
# فونت‌های اسم (۵۰+) و فونت‌های عدد ساعت (۷ مدل)
# ============================================================
FONT_MAPS = {}
def _map(name, mapping_str):
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

SIMPLE_STYLES = [
    ("★{}★", "استار"), ("『{}』", "قاب"), ("«{}»", "گیومه"), ("➤{}", "پیکان"),
    ("✿{}✿", "گل"), ("⚡{}⚡", "برق"), ("☾{}☽", "ماه"), ("♛{}♛", "تاج"),
    ("▄︻{}══➤", "تفنگ"), ("🔥{}🔥", "آتش"), ("꧁{}꧂", "زینتی"), ("•.,¸¸,.•*`{}`*•.,¸¸,.•", "امواج"),
    (" {} ", "فاصله‌دار"), ("○{}○", "دایره"), ("[{}]", "کروشه"), ("(๑•́ω•̀๑){}", "بامزه"),
    ("彡{}彡", "سایه"), ("卂{}乂", "بلاک"), ("『★{}★』", "قاب‌استار"), ("-{}-", "خط‌تیره"),
]

def build_font_list(name: str):
    out = []
    for font_name, table in FONT_MAPS.items():
        out.append((font_name, "".join(table.get(ch, ch) for ch in name)))
    bold = "".join(FONT_MAPS["𝗕𝗼𝗹𝗱"].get(ch, ch) for ch in name)
    for tpl, label in SIMPLE_STYLES:
        out.append((label, tpl.format(name)))
    for tpl, label in SIMPLE_STYLES:
        out.append((label + "+Bold", tpl.format(bold)))
    return out

DIGIT_FONTS = {
    "معمولی": "0123456789",
    "توپر (Bold)": "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵",
    "دوخط (Double-struck)": "𝟘𝟙𝟚𝟛𝟜𝟝𝟞𝟟𝟠𝟡",
    "مونو (Mono)": "𝟶𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿",
    "دایره‌ای (Circle)": "⓪①②③④⑤⑥⑦⑧⑨",
    "عریض (Fullwidth)": "０１２３４５６７８９",
    "بالانویس (Superscript)": "⁰¹²³⁴⁵⁶⁷⁸⁹",
}
DIGIT_FONT_NAMES = list(DIGIT_FONTS.keys())

def render_clock(time_str: str, font_name: str) -> str:
    table = DIGIT_FONTS.get(font_name, DIGIT_FONTS["معمولی"])
    return "".join(table[int(ch)] if ch.isdigit() else ch for ch in time_str)

# ============================================================
# منوها
# ============================================================
async def self_menu_kb(user_id, page=1):
    profile = await r_get(f"self_profile:{user_id}") or {}
    if not profile.get("active"):
        return {"inline_keyboard": [[{"text": f"🚀 فعال‌سازی سلف ({SELF_ACTIVATION_COST:,} تومان)", "callback_data": "self:activate"}]]}
    if page == 2:
        return {
            "inline_keyboard": [
                [{"text": "📸 تغییر عکس پروفایل", "callback_data": "self:set_photo"}, {"text": "🔔 هشدار ورود جدید", "callback_data": "self:toggle_login_alert"}],
                [{"text": "🚫 مسدودسازی کلمه‌کلیدی", "callback_data": "self:set_blockwords"}, {"text": "🌙 حالت غیبت زمان‌بندی", "callback_data": "self:set_dnd"}],
                [{"text": "⬅️ صفحه قبل", "callback_data": "self:page1"}],
            ]
        }
    return {
        "inline_keyboard": [
            [{"text": "🕒 ساعت کنار اسم", "callback_data": "self:toggle_clock"}, {"text": "🔠 فونت ساعت", "callback_data": "self:set_clock_font"}],
            [{"text": "🔤 فونت اسم", "callback_data": "self:set_font_name"}, {"text": "🔒 عضویت اجباری پیوی", "callback_data": "self:set_forced_pm"}],
            [{"text": "💬 پیام خودکار", "callback_data": "self:set_autoreply"}, {"text": "📝 بیوگرافی", "callback_data": "self:set_bio"}],
            [{"text": "🙈 مخفی آخرین بازدید", "callback_data": "self:toggle_lastseen"}, {"text": "♻️ حذف خودکار پیام‌ها", "callback_data": "self:set_selfdestruct"}],
            [{"text": "📩 فوروارد پیوی به Saved", "callback_data": "self:toggle_fwd_saved"}, {"text": "💳 وضعیت", "callback_data": "self:status"}],
            [{"text": "🚪 خروج از حساب", "callback_data": "self:logout"}],
            [{"text": "➡️ صفحه بعد (۴ قابلیت جدید)", "callback_data": "self:page2"}],
        ]
    }

def admin_menu_kb():
    return {
        "inline_keyboard": [
            [{"text": "📊 آمار بات", "callback_data": "admin:stats"}, {"text": "📣 پیام همگانی", "callback_data": "admin:broadcast"}],
            [{"text": "➕ افزودن کانال اجباری", "callback_data": "admin:addch"}, {"text": "➖ حذف کانال اجباری", "callback_data": "admin:delch"}],
            [{"text": "💰 افزایش موجودی", "callback_data": "admin:addbal"}, {"text": "➖ کاهش موجودی", "callback_data": "admin:subbal"}],
            [{"text": "🎁 افزایش موجودی همگانی", "callback_data": "admin:bulkbal"}, {"text": "🚫 مسدود/رفع مسدودی", "callback_data": "admin:ban"}],
            [{"text": "🔍 جستجوی کاربر", "callback_data": "admin:search"}, {"text": "💵 تغییر قیمت پلن‌ها", "callback_data": "admin:prices"}],
            [{"text": "📦 سفارش‌های در انتظار", "callback_data": "admin:orders"}, {"text": "📤 بکاپ اطلاعات کاربران", "callback_data": "admin:backup"}],
            [{"text": "✉️ پیام به کاربر خاص", "callback_data": "admin:dm"}, {"text": "📋 لیست کانال‌های اجباری", "callback_data": "admin:listch"}],
            [{"text": "🗑 لغو سفارش", "callback_data": "admin:cancelorder"}],
        ]
    }

def likeych_activate_kb():
    return {"inline_keyboard": [[{"text": f"🚀 فعال‌سازی ({LIKEYCH_ACTIVATION_COST:,} تومان / ۷ روز)", "callback_data": "likeych:activate"}]]}

def likeych_menu_kb():
    return {
        "inline_keyboard": [
            [{"text": "🆕 ساخت پست جدید", "callback_data": "likeych:new"}, {"text": "⚙️ تنظیمات", "callback_data": "likeych:settings"}],
            [{"text": "💳 وضعیت اشتراک", "callback_data": "likeych:status"}],
        ]
    }

# ============================================================
# اعمال تنظیمات ذخیره‌شده روی اکانت
# ============================================================
async def apply_profile_to_client(user_id: int, client: TelegramClient):
    prof = await r_get(f"self_profile:{user_id}") or {}
    base = prof.get("base_name", "")
    try:
        if base:
            await client(UpdateProfileRequest(first_name=base))
        if prof.get("clock_on"):
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=3, minutes=30)
            clk = render_clock(now.strftime("%H:%M"), prof.get("clock_font", "معمولی"))
            await client(UpdateProfileRequest(last_name=clk))
        else:
            await client(UpdateProfileRequest(last_name=""))
        if prof.get("bio") is not None:
            await client(UpdateProfileRequest(about=prof.get("bio", "")))
        if prof.get("hide_last_seen"):
            await client(SetPrivacyRequest(key=InputPrivacyKeyStatusTimestamp(), rules=[InputPrivacyValueDisallowAll()]))
        else:
            await client(SetPrivacyRequest(key=InputPrivacyKeyStatusTimestamp(), rules=[InputPrivacyValueAllowAll()]))
    except Exception:
        pass

# ============================================================
# فلوی سلف (لاگین + تنظیمات)
# ============================================================
async def handle_self_message(msg):
    chat_id = msg["chat"]["id"]
    user_id = msg["from"]["id"]
    text = msg.get("text", "")
    state = await r_get_raw(f"self_state:{chat_id}")

    if state == "await_phone":
        phone = text.strip()
        await bot_send(chat_id, "⏳ در حال ارسال کد به اکانت... چند لحظه صبر کن.")
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        try:
            sent = await client.send_code_request(phone)
            login_sessions[chat_id] = {"client": client, "phone": phone, "phone_code_hash": sent.phone_code_hash}
            await r_set_raw(f"self_state:{chat_id}", "await_code")
            await set_route_state(chat_id, True)
            await bot_send(chat_id,
                "✅ کد به تلگرامت ارسال شد.\n\n"
                "⚠️ کد رو <b>با فاصله بین هر عدد</b> وارد کن (مثلا اگه کد ۱۲۳۴۵ هست بنویس: <code>1 2 3 4 5</code>)\n"
                "این کار جلوی نامعتبر شدن خودکار کد رو می‌گیره.")
        except Exception as e:
            await bot_send(chat_id, f"❌ خطا در ارسال کد: {e}\n\nمبلغ فعال‌سازی بهت برگردونده شد.")
            await add_wallet(user_id, SELF_ACTIVATION_COST)
            await r_del(f"self_state:{chat_id}")
            await set_route_state(chat_id, False)
        return

    if state == "await_code":
        sess = login_sessions.get(chat_id)
        if not sess:
            await bot_send(chat_id, "فلوی لاگین منقضی شده. دوباره از منوی سلف اقدام کن.")
            await r_del(f"self_state:{chat_id}")
            await set_route_state(chat_id, False)
            return
        code = text.replace(" ", "").strip()
        try:
            await sess["client"].sign_in(sess["phone"], code, phone_code_hash=sess["phone_code_hash"])
            await finish_login(chat_id, user_id, sess["client"])
        except SessionPasswordNeededError:
            await r_set_raw(f"self_state:{chat_id}", "await_password")
            await bot_send(chat_id, "اکانتت رمز دومرحله‌ای داره. رمزو بفرست:")
        except PhoneCodeInvalidError:
            await bot_send(chat_id, "❌ کد اشتباهه. دوباره بفرست (یادت نره با فاصله بین اعداد):")
        except Exception as e:
            await bot_send(chat_id, f"خطا: {e}\nدوباره امتحان کن:")
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
        clean = re.sub(r"[^A-Za-z0-9 ]", "", text)[:20] or "Name"
        fonts = build_font_list(clean)
        buf = [f"{i+1}. {styled}  ({label})" for i, (label, styled) in enumerate(fonts[:60])]
        await bot_send(chat_id, "یکی رو انتخاب کن و شماره‌ش رو بفرست:\n\n" + "\n".join(buf))
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
        await set_route_state(chat_id, False)
        client = active_clients.get(user_id)
        if client:
            await apply_profile_to_client(user_id, client)
        await bot_send(chat_id, f"✅ اسمت تنظیم شد: {styled}")
        return

    if state == "await_clock_font_pick":
        try:
            idx = int(text.strip()) - 1
            font_name = DIGIT_FONT_NAMES[idx]
        except Exception:
            await bot_send(chat_id, "شماره نامعتبره.")
            return
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["clock_font"] = font_name
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        await set_route_state(chat_id, False)
        client = active_clients.get(user_id)
        if client:
            await apply_profile_to_client(user_id, client)
        await bot_send(chat_id, f"✅ فونت ساعت تنظیم شد: {font_name}")
        return

    if state == "await_forced_pm_channel":
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["forced_pm_channel"] = text.strip()
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        await set_route_state(chat_id, False)
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
        await set_route_state(chat_id, False)
        await bot_send(chat_id, "✅ پیام خودکار تنظیم شد (فقط برای کسایی که به اکانتت پیام بدن ارسال می‌شه).")
        return

    if state == "await_bio_text":
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["bio"] = text.strip()
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        await set_route_state(chat_id, False)
        client = active_clients.get(user_id)
        if client:
            await apply_profile_to_client(user_id, client)
        await bot_send(chat_id, "✅ بیوگرافی تنظیم شد.")
        return

    if state == "await_selfdestruct_seconds":
        try:
            seconds = int(text.strip())
            if seconds < 0:
                raise ValueError
        except ValueError:
            await bot_send(chat_id, "یه عدد صحیح (ثانیه) بفرست، برای غیرفعال کردن 0 بفرست.")
            return
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["selfdestruct_seconds"] = seconds
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, "✅ تنظیم شد." if seconds > 0 else "✅ غیرفعال شد.")
        return

    if state == "await_photo_upload":
        photo = msg.get("photo")
        if not photo:
            await bot_send(chat_id, "لطفاً یه عکس بفرست (نه متن).")
            return
        file_id = photo[-1]["file_id"]
        img_bytes = await get_file_bytes(file_id)
        client = active_clients.get(user_id)
        if not client or not img_bytes:
            await bot_send(chat_id, "❌ خطا در دریافت عکس یا سلف غیرفعاله.")
            await r_del(f"self_state:{chat_id}")
            await set_route_state(chat_id, False)
            return
        try:
            import io
            uploaded = await client.upload_file(io.BytesIO(img_bytes), file_name="profile.jpg")
            await client(UploadProfilePhotoRequest(file=uploaded))
            await bot_send(chat_id, "✅ عکس پروفایلت تغییر کرد.")
        except Exception as e:
            await bot_send(chat_id, f"❌ خطا: {e}")
        await r_del(f"self_state:{chat_id}")
        await set_route_state(chat_id, False)
        return

    if state == "await_blockwords":
        words = [w.strip() for w in text.split(",") if w.strip()]
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["block_keywords"] = words
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, f"✅ {len(words)} کلمه‌کلیدی ثبت شد. هر پیامی حاوی این کلمات باشه، فرستنده‌ش خودکار بلاک می‌شه.")
        return

    if state == "await_dnd":
        m = re.match(r"^\s*(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s+([\s\S]+)$", text)
        if not m:
            await bot_send(chat_id, "فرمت درست نیست. مثال: <code>23:00-08:00 الان خوابم، بعدا جواب میدم</code>")
            return
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["dnd_start"], prof["dnd_end"], prof["dnd_message"] = m.group(1), m.group(2), m.group(3).strip()
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, f"✅ حالت غیبت از {m.group(1)} تا {m.group(2)} تنظیم شد.")
        return


async def finish_login(chat_id, user_id, client):
    session_str = client.session.save()
    await r_set_raw(f"self_session:{user_id}", session_str)
    await r_del(f"self_state:{chat_id}")
    await set_route_state(chat_id, False)
    login_sessions.pop(chat_id, None)

    prof = await r_get(f"self_profile:{user_id}") or {}
    prof["active"] = True
    prof["next_bill_at"] = time.time() + 3600
    await r_set(f"self_profile:{user_id}", prof)
    await register_self_user(user_id)

    await start_self_client(user_id, session_str)
    await bot_send(chat_id, "✅ سلف با موفقیت فعال شد و اکانتت متصله! تنظیماتی که قبلاً انتخاب کرده باشی خودکار روی اکانتت اعمال شد.",
                    await self_menu_kb(user_id))


async def handle_self_callback(cq):
    chat_id = cq["message"]["chat"]["id"]
    user_id = cq["from"]["id"]
    data = cq["data"]

    if data == "self:menu" or data == "self:page1":
        await bot_send(chat_id,
            f"🔷 <b>بخش سلف</b>\n\nهزینه فعال‌سازی: {SELF_ACTIVATION_COST:,} تومان\nهزینه نگه‌داری: {SELF_HOURLY_COST:,} تومان در ساعت",
            await self_menu_kb(user_id, page=1))
        await bot_answer_cb(cq["id"])
        return

    if data == "self:page2":
        await bot_edit_markup(chat_id, cq["message"]["message_id"], await self_menu_kb(user_id, page=2))
        await bot_answer_cb(cq["id"])
        return

    if data == "self:activate":
        wallet = await get_wallet(user_id)
        if wallet < SELF_ACTIVATION_COST:
            await bot_answer_cb(cq["id"], f"موجودی کافی نیست! حداقل {SELF_ACTIVATION_COST:,} تومان لازمه.", True)
            return
        result = await worker_api("POST", "/api/wallet/add", json={"user_id": user_id, "delta": -SELF_ACTIVATION_COST})
        if not result.get("ok"):
            await bot_answer_cb(cq["id"], "خطا در کسر موجودی!", True)
            await bot_send(chat_id, f"❌ کسر موجودی انجام نشد: {result.get('error', 'نامشخص')}\nWORKER_URL و RAILWAY_SECRET رو چک کن.")
            return
        new_balance = result.get("balance", 0)
        await r_set_raw(f"self_state:{chat_id}", "await_phone")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"], f"✅ {SELF_ACTIVATION_COST:,} تومان کسر شد. موجودی جدید: {new_balance:,}")
        await bot_send(chat_id, "شماره تلگرامت رو با فرمت بین‌المللی بفرست (مثال: +989121234567):")
        return

    if data == "self:toggle_clock":
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["clock_on"] = not prof.get("clock_on", False)
        await r_set(f"self_profile:{user_id}", prof)
        client = active_clients.get(user_id)
        if client:
            await apply_profile_to_client(user_id, client)
        await bot_answer_cb(cq["id"], f"ساعت کنار اسم: {'روشن ✅' if prof['clock_on'] else 'خاموش ❌'}")
        return

    if data == "self:set_clock_font":
        lines = [f"{i+1}. {render_clock('12:34', name)} ({name})" for i, name in enumerate(DIGIT_FONT_NAMES)]
        await r_set_raw(f"self_state:{chat_id}", "await_clock_font_pick")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "شماره فونت ساعت رو انتخاب کن:\n\n" + "\n".join(lines))
        return

    if data == "self:set_font_name":
        await r_set_raw(f"self_state:{chat_id}", "await_font_name")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "اسمت رو به انگلیسی بفرست:")
        return

    if data == "self:set_forced_pm":
        await r_set_raw(f"self_state:{chat_id}", "await_forced_pm_channel")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "یوزرنیم کانالی که میخوای عضویتش اجباری بشه رو بفرست (مثال: @channel):")
        return

    if data == "self:set_autoreply":
        await r_set_raw(f"self_state:{chat_id}", "await_autoreply_text")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "متن پیام خودکار رو بفرست (حداکثر ۲ خط):")
        return

    if data == "self:set_bio":
        await r_set_raw(f"self_state:{chat_id}", "await_bio_text")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "متن بیوگرافی جدید رو بفرست:")
        return

    if data == "self:toggle_lastseen":
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["hide_last_seen"] = not prof.get("hide_last_seen", False)
        await r_set(f"self_profile:{user_id}", prof)
        client = active_clients.get(user_id)
        if client:
            await apply_profile_to_client(user_id, client)
        await bot_answer_cb(cq["id"], f"مخفی‌سازی آخرین بازدید: {'روشن ✅' if prof['hide_last_seen'] else 'خاموش ❌'}")
        return

    if data == "self:set_selfdestruct":
        await r_set_raw(f"self_state:{chat_id}", "await_selfdestruct_seconds")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "بعد از چند ثانیه پیام‌های ارسالیت خودکار حذف بشن؟ (برای غیرفعال کردن 0 بفرست):")
        return

    if data == "self:toggle_fwd_saved":
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["forward_to_saved"] = not prof.get("forward_to_saved", False)
        await r_set(f"self_profile:{user_id}", prof)
        await bot_answer_cb(cq["id"], f"فوروارد پیوی‌ها به Saved Messages: {'روشن ✅' if prof['forward_to_saved'] else 'خاموش ❌'}")
        return

    if data == "self:status":
        wallet = await get_wallet(user_id)
        active = user_id in active_clients
        hours_left = wallet // SELF_HOURLY_COST if active else 0
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id,
            f"وضعیت سلف: {'فعال ✅' if active else 'غیرفعال ❌'}\n💰 موجودی: {wallet:,} تومان\n💸 هزینه ساعتی: {SELF_HOURLY_COST:,} تومان\n⏳ حدود {hours_left} ساعت دیگه روشن می‌مونه.")
        return

    if data == "self:logout":
        client = active_clients.pop(user_id, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["active"] = False
        await r_set(f"self_profile:{user_id}", prof)
        await r_del(f"self_session:{user_id}")
        await unregister_self_user(user_id)
        await bot_answer_cb(cq["id"], "از حساب خارج شدی.")
        await bot_send(chat_id, f"🚪 با موفقیت از اکانتت خارج شدی.\n\n⚠️ برای فعال‌سازی مجدد سلف، باید دوباره {SELF_ACTIVATION_COST:,} تومان پرداخت کنی.", await self_menu_kb(user_id))
        return

    # ---- صفحه دوم: ۴ قابلیت جدید ----
    if data == "self:set_photo":
        if user_id not in active_clients:
            await bot_answer_cb(cq["id"], "اول سلف رو فعال کن.", True)
            return
        await r_set_raw(f"self_state:{chat_id}", "await_photo_upload")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "عکسی که میخوای پروفایلت بشه رو بفرست:")
        return

    if data == "self:toggle_login_alert":
        prof = await r_get(f"self_profile:{user_id}") or {}
        prof["login_alert"] = not prof.get("login_alert", False)
        if prof["login_alert"] and "known_sessions" not in prof:
            prof["known_sessions"] = []
        await r_set(f"self_profile:{user_id}", prof)
        await bot_answer_cb(cq["id"], f"هشدار ورود جدید: {'روشن ✅' if prof['login_alert'] else 'خاموش ❌'}")
        return

    if data == "self:set_blockwords":
        await r_set_raw(f"self_state:{chat_id}", "await_blockwords")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "کلمات کلیدی رو با کاما جدا کن، مثلا:\n<code>تبلیغ, اسپم, فروش فالوور</code>")
        return

    if data == "self:set_dnd":
        await r_set_raw(f"self_state:{chat_id}", "await_dnd")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "بازه‌ی زمانی و پیام غیبت رو بفرست، مثلا:\n<code>23:00-08:00 الان خوابم، بعدا جواب میدم</code>")
        return

    await bot_answer_cb(cq["id"])

# ============================================================
# موتور سلف (Telethon)
# ============================================================
# ============================================================
# ردیابی کاربرهایی که سلف‌شون فعاله (برای اتصال مجدد بعد از ری‌استارت سرویس)
# ============================================================
async def register_self_user(user_id):
    users = await r_get("self_active_users") or []
    if user_id not in users:
        users.append(user_id)
        await r_set("self_active_users", users)

async def unregister_self_user(user_id):
    users = await r_get("self_active_users") or []
    if user_id in users:
        users.remove(user_id)
        await r_set("self_active_users", users)

async def reconnect_all_self_clients():
    """هر بار سرویس ریل‌وی ری‌استارت/دیپلوی می‌شه، حافظه خالی می‌شه و سلف‌های قبلی
    باید دوباره وصل بشن، وگرنه هیچ‌کدوم از قابلیت‌هاشون (پیام خودکار، ساعت، عضویت
    اجباری) در پس‌زمینه اجرا نمی‌شه، حتی اگه تو دیتابیس "فعال" ثبت شده باشن."""
    users = await r_get("self_active_users") or []
    for user_id in users:
        try:
            prof = await r_get(f"self_profile:{user_id}") or {}
            if not prof.get("active"):
                continue
            session_str = await r_get_raw(f"self_session:{user_id}")
            if not session_str:
                continue
            await start_self_client(user_id, session_str)
        except Exception:
            pass


def _in_dnd_window(start: str, end: str) -> bool:
    now = (datetime.datetime.utcnow() + datetime.timedelta(hours=3, minutes=30)).time()
    sh, sm = map(int, start.split(":"))
    eh, em = map(int, end.split(":"))
    s = datetime.time(sh, sm)
    e = datetime.time(eh, em)
    if s <= e:
        return s <= now <= e
    return now >= s or now <= e  # بازه‌ی شبانه که از نیمه‌شب رد می‌شه


async def start_self_client(user_id: int, session_str: str):
    if user_id in active_clients:
        return
    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        return
    active_clients[user_id] = client
    await apply_profile_to_client(user_id, client)

    @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def _pm_handler(event):
        sender_id = event.sender_id
        # پیام‌های خودِ بات اصلی رو نادیده بگیر (چون همون اکانت کاربر با بات هم پیوی داره)
        if BOT_USER_ID and sender_id == BOT_USER_ID:
            return

        prof = await r_get(f"self_profile:{user_id}") or {}

        # مسدودسازی خودکار بر اساس کلمه‌کلیدی
        keywords = prof.get("block_keywords") or []
        msg_text = (event.raw_text or "").lower()
        if keywords and any(kw.lower() in msg_text for kw in keywords):
            try:
                await client(BlockRequest(id=sender_id))
            except Exception:
                pass
            return

        if prof.get("forward_to_saved"):
            try:
                await client.forward_messages("me", event.message)
            except Exception:
                pass

        # عضویت اجباری پیوی - فقط برای کسایی که به سلف پیام میدن
        forced = prof.get("forced_pm_channel")
        if forced:
            r = await get_chat_member(forced, sender_id)
            status = (r.get("result") or {}).get("status")
            if status not in ("member", "administrator", "creator"):
                link = f"https://t.me/{forced.lstrip('@')}"
                await event.reply("برای گفت‌وگو با من باید عضو این کانال بشی 👇", buttons=[[Button.url("📢 عضویت در کانال", link)]])
                return

        # حالت غیبت زمان‌بندی‌شده - اولویت با اینه
        if prof.get("dnd_start") and prof.get("dnd_end") and _in_dnd_window(prof["dnd_start"], prof["dnd_end"]):
            if prof.get("dnd_message"):
                await event.reply(prof["dnd_message"])
            return

        autoreply = prof.get("autoreply")
        if autoreply:
            await event.reply(autoreply)

    @client.on(events.NewMessage(outgoing=True))
    async def _selfdestruct_handler(event):
        prof = await r_get(f"self_profile:{user_id}") or {}
        seconds = prof.get("selfdestruct_seconds", 0)
        if seconds and seconds > 0:
            async def _delete_later():
                await asyncio.sleep(seconds)
                try:
                    await event.delete()
                except Exception:
                    pass
            asyncio.create_task(_delete_later())

    asyncio.create_task(clock_loop(user_id))


async def clock_loop(user_id: int):
    while user_id in active_clients:
        prof = await r_get(f"self_profile:{user_id}") or {}
        client = active_clients.get(user_id)
        if client and prof.get("clock_on"):
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=3, minutes=30)
            clk = render_clock(now.strftime("%H:%M"), prof.get("clock_font", "معمولی"))
            try:
                await client(UpdateProfileRequest(last_name=clk))
            except Exception:
                pass
        await asyncio.sleep(60)


async def hourly_billing_loop():
    while True:
        await asyncio.sleep(20)
        now = time.time()
        for user_id in list(active_clients.keys()):
            prof = await r_get(f"self_profile:{user_id}") or {}
            next_bill = prof.get("next_bill_at", now + 3600)
            if now < next_bill:
                continue
            wallet = await get_wallet(user_id)
            if wallet < SELF_HOURLY_COST:
                client = active_clients.pop(user_id, None)
                if client:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                prof["active"] = False
                await r_set(f"self_profile:{user_id}", prof)
                await unregister_self_user(user_id)
                await bot_send(user_id, f"❌ موجودی کافی نبود، سلف خاموش شد. برای فعال‌سازی مجدد {SELF_ACTIVATION_COST:,} تومان لازمه.")
                continue
            await add_wallet(user_id, -SELF_HOURLY_COST)
            prof["next_bill_at"] = next_bill + 3600
            await r_set(f"self_profile:{user_id}", prof)


async def login_alert_loop():
    """هر ۱۰ دقیقه چک می‌کنه سشن جدیدی به اکانت اضافه نشده باشه."""
    while True:
        await asyncio.sleep(600)
        for user_id, client in list(active_clients.items()):
            prof = await r_get(f"self_profile:{user_id}") or {}
            if not prof.get("login_alert"):
                continue
            try:
                res = await client(GetAuthorizationsRequest())
                current_hashes = {str(a.hash) for a in res.authorizations}
                known = set(prof.get("known_sessions") or [])
                if not known:
                    prof["known_sessions"] = list(current_hashes)
                    await r_set(f"self_profile:{user_id}", prof)
                    continue
                new_ones = current_hashes - known
                if new_ones:
                    devices = [a for a in res.authorizations if str(a.hash) in new_ones]
                    for d in devices:
                        await bot_send(user_id, f"🔔 ورود جدید به اکانتت:\n📱 {d.device_model}\n🌍 {d.country}\n🕐 {d.date_created}")
                    prof["known_sessions"] = list(current_hashes)
                    await r_set(f"self_profile:{user_id}", prof)
            except Exception:
                pass


async def referral_reversal_loop():
    while True:
        await asyncio.sleep(1200)
        try:
            channels = await get_forced_channels()
            referred = await get_referred_users()
            for item in referred:
                if item.get("reward_reclaimed"):
                    continue
                uid = item["id"]
                still_member = True
                for ch in channels:
                    r = await get_chat_member(ch["chat"], uid)
                    status = (r.get("result") or {}).get("status")
                    if status not in ("member", "administrator", "creator"):
                        still_member = False
                        break
                if not still_member:
                    await reclaim_referral(uid)
        except Exception:
            pass


async def likeych_expiry_loop():
    """هر ساعت چک می‌کنه اشتراک چالش‌لایکی کسی تموم نشده باشه."""
    while True:
        await asyncio.sleep(3600)
        try:
            users = await r_get("likeych_users_all") or []
            now = time.time()
            for uid in users:
                prof = await r_get(f"likeych_profile:{uid}") or {}
                if prof.get("active") and prof.get("expires_at", 0) < now:
                    prof["active"] = False
                    await r_set(f"likeych_profile:{uid}", prof)
                    await bot_send(uid, "⌛️ اشتراک چالش‌لایکیت تموم شد. برای ادامه باید دوباره فعالش کنی.")
        except Exception:
            pass

# ============================================================
# پیام همگانی
# ============================================================
async def handle_broadcast_flow(update):
    if "callback_query" in update:
        cq = update["callback_query"]
        chat_id = cq["message"]["chat"]["id"]
        await r_set_raw(f"admin_state:{chat_id}", "await_broadcast_text")
        await set_route_state(chat_id, True)
        await bot_send(chat_id, "متن پیام همگانی رو بفرست (برای همه کاربران ارسال میشه):")
        await bot_answer_cb(cq["id"])
        return

    msg = update["message"]
    chat_id = msg["chat"]["id"]
    text = msg["text"]
    users = await get_all_users()
    await bot_send(chat_id, f"شروع ارسال به {len(users)} کاربر...")
    sent, failed = 0, 0
    for uid in users:
        try:
            res = await bot_send(uid, text)
            sent += 1 if res.get("ok") else 0
            failed += 0 if res.get("ok") else 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await bot_send(chat_id, f"✅ پیام همگانی تموم شد.\nموفق: {sent}\nناموفق: {failed}")

# ============================================================
# چالش لایکی (کاملاً مستقل از سلف)
# ============================================================
async def get_likeych_profile(user_id):
    prof = await r_get(f"likeych_profile:{user_id}") or {"active": False, "expires_at": 0, "require_membership": True}
    return prof

async def register_likeych_user(user_id):
    users = await r_get("likeych_users_all") or []
    if user_id not in users:
        users.append(user_id)
        await r_set("likeych_users_all", users)

async def handle_likeych_callback(cq):
    chat_id = cq["message"]["chat"]["id"]
    user_id = cq["from"]["id"]
    data = cq["data"]

    if data == "likeych:menu":
        prof = await get_likeych_profile(user_id)
        active = prof.get("active") and prof.get("expires_at", 0) > time.time()
        await bot_answer_cb(cq["id"])
        if active:
            days_left = int((prof["expires_at"] - time.time()) // 86400) + 1
            await bot_send(chat_id, f"🎯 <b>چالش لایکی</b>\n\nاشتراک فعاله ({days_left} روز مونده).", likeych_menu_kb())
        else:
            await bot_send(chat_id,
                f"🎯 <b>چالش لایکی</b>\n\nبا این بخش می‌تونی تو کانالت پست اسم بسازی و لایک‌هاشو مدیریت کنی.\n\n"
                f"هزینه: {LIKEYCH_ACTIVATION_COST:,} تومان برای ۷ روز.", likeych_activate_kb())
        return

    if data == "likeych:activate":
        wallet = await get_wallet(user_id)
        if wallet < LIKEYCH_ACTIVATION_COST:
            await bot_answer_cb(cq["id"], f"موجودی کافی نیست! حداقل {LIKEYCH_ACTIVATION_COST:,} تومان لازمه.", True)
            return
        result = await worker_api("POST", "/api/wallet/add", json={"user_id": user_id, "delta": -LIKEYCH_ACTIVATION_COST})
        if not result.get("ok"):
            await bot_answer_cb(cq["id"], "خطا در کسر موجودی!", True)
            await bot_send(chat_id, f"❌ کسر موجودی انجام نشد: {result.get('error', 'نامشخص')}\nقبل از فعال‌سازی WORKER_URL و RAILWAY_SECRET رو چک کن.")
            return
        new_balance = result.get("balance", 0)
        prof = await get_likeych_profile(user_id)
        prof["active"] = True
        prof["expires_at"] = time.time() + LIKEYCH_DURATION_SECONDS
        prof.setdefault("require_membership", True)
        await r_set(f"likeych_profile:{user_id}", prof)
        await register_likeych_user(user_id)
        await bot_answer_cb(cq["id"], "✅ فعال شد!")
        await bot_send(chat_id, f"✅ چالش لایکی برای ۷ روز فعال شد.\n💰 {LIKEYCH_ACTIVATION_COST:,} تومان کسر شد. موجودی جدید: {new_balance:,} تومان", likeych_menu_kb())
        return

    prof = await get_likeych_profile(user_id)
    if not (prof.get("active") and prof.get("expires_at", 0) > time.time()):
        await bot_answer_cb(cq["id"], "اشتراکت فعال نیست یا تموم شده.", True)
        return

    if data == "likeych:new":
        await r_set_raw(f"likeych_state:{chat_id}", "await_channel")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "یوزرنیم کانالی که ربات توش ادمینه رو بفرست:")
        return

    if data == "likeych:settings":
        req_mem = prof.get("require_membership", True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id,
            f"⚙️ تنظیمات چالش لایکی\n\nالزام عضویت کانال برای لایک: {'فعال ✅' if req_mem else 'غیرفعال ❌'}\n\n"
            f"وقتی فعاله، فقط کسایی که عضو کانال باشن می‌تونن لایک کنن.",
            {"inline_keyboard": [[{"text": "🔁 تغییر وضعیت", "callback_data": "likeych:toggle_req"}]]})
        return

    if data == "likeych:toggle_req":
        prof["require_membership"] = not prof.get("require_membership", True)
        await r_set(f"likeych_profile:{user_id}", prof)
        await bot_answer_cb(cq["id"], f"الزام عضویت: {'فعال ✅' if prof['require_membership'] else 'غیرفعال ❌'}")
        return

    if data == "likeych:status":
        days_left = int((prof["expires_at"] - time.time()) // 86400) + 1
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, f"💳 اشتراک چالش‌لایکی: {days_left} روز مونده.")
        return

    # مدیریت لایک یک پست خاص: افزایش/کاهش دستی
    if data.startswith("likeych:inc:") or data.startswith("likeych:dec:"):
        post_id = data.split(":")[2]
        post = await r_get(f"likeych_post:{post_id}")
        if not post or post.get("owner_id") != user_id:
            await bot_answer_cb(cq["id"], "این پست مال تو نیست یا پیدا نشد.", True)
            return
        delta = 1 if data.startswith("likeych:inc:") else -1
        post["manual"] = max(0, post.get("manual", 0) + delta)
        await r_set(f"likeych_post:{post_id}", post)
        total = len(post.get("likes", [])) + post.get("manual", 0)
        await bot_edit_markup(post["channel"], post["message_id"], {"inline_keyboard": [[{"text": f"❤️ {total}", "callback_data": f"likey:{post_id}"}]]})
        await bot_edit_text(chat_id, cq["message"]["message_id"],
            f"🎯 پنل مدیریت: <b>{post['name']}</b>\n\n❤️ مجموع لایک: {total}",
            {"inline_keyboard": [[{"text": "➕ لایک", "callback_data": f"likeych:inc:{post_id}"}, {"text": "➖ لایک", "callback_data": f"likeych:dec:{post_id}"}]]})
        await bot_answer_cb(cq["id"])
        return


async def handle_likeych_message(msg):
    chat_id = msg["chat"]["id"]
    user_id = msg["from"]["id"]
    text = msg.get("text", "")
    state = await r_get_raw(f"likeych_state:{chat_id}")

    if state == "await_channel":
        await r_set_raw(f"likeych_channel:{chat_id}", text.strip())
        await r_set_raw(f"likeych_state:{chat_id}", "await_name")
        await bot_send(chat_id, "حالا اسمی که میخوای پست بشه رو بفرست:")
        return

    if state == "await_name":
        channel = await r_get_raw(f"likeych_channel:{chat_id}")
        post_id = str(int(time.time() * 1000))
        async with httpx.AsyncClient(timeout=20) as c:
            res = (await c.post(f"{BOT_API}/sendMessage", json={
                "chat_id": channel, "text": text,
                "reply_markup": {"inline_keyboard": [[{"text": "❤️ 0", "callback_data": f"likey:{post_id}"}]]},
            })).json()
        if not res.get("ok"):
            await bot_send(chat_id, f"❌ خطا در ارسال به کانال: {res.get('description')}\nمطمئن شو ربات ادمین اون کانال باشه.")
        else:
            message_id = res["result"]["message_id"]
            await r_set(f"likeych_post:{post_id}", {"channel": channel, "message_id": message_id, "name": text, "owner_id": user_id, "likes": [], "manual": 0})
            await bot_send(chat_id, f"✅ پست ارسال شد. با دکمه‌های زیر می‌تونی لایکش رو مدیریت کنی:",
                {"inline_keyboard": [[{"text": "➕ لایک", "callback_data": f"likeych:inc:{post_id}"}, {"text": "➖ لایک", "callback_data": f"likeych:dec:{post_id}"}]]})
        await r_del(f"likeych_state:{chat_id}")
        await set_route_state(chat_id, False)
        return


async def handle_likey_click(cq):
    """کلیک عمومی هر کاربری روی دکمه‌ی قلب زیر پست کانال."""
    data = cq["data"]
    user_id = cq["from"]["id"]
    post_id = data.split(":")[1]
    post = await r_get(f"likeych_post:{post_id}")
    if not post:
        await bot_answer_cb(cq["id"], "این پست دیگه معتبر نیست.", True)
        return

    likeych_prof = await get_likeych_profile(post["owner_id"])
    require_membership = likeych_prof.get("require_membership", True)
    if require_membership:
        r = await get_chat_member(post["channel"], user_id)
        status = (r.get("result") or {}).get("status")
        if status not in ("member", "administrator", "creator"):
            await bot_answer_cb(cq["id"], "برای لایک کردن باید عضو کانال باشی!", True)
            return

    if user_id in post["likes"]:
        await bot_answer_cb(cq["id"], "قبلاً لایک کردی ✅")
        return
    post["likes"].append(user_id)
    await r_set(f"likeych_post:{post_id}", post)
    total = len(post["likes"]) + post.get("manual", 0)
    kb = {"inline_keyboard": [[{"text": f"❤️ {total}", "callback_data": data}]]}
    await bot_edit_markup(post["channel"], post["message_id"], kb)
    await bot_answer_cb(cq["id"], "لایک ثبت شد ❤️")

# ============================================================
# پنل ادمین
# ============================================================
async def handle_admin_message(msg):
    chat_id = msg["chat"]["id"]
    text = msg.get("text", "")
    if str(msg["from"]["id"]) != str(ADMIN_ID):
        return

    if text.startswith("/admin"):
        await bot_send(chat_id, "پنل مدیریت:", admin_menu_kb())
        return

    state = await r_get_raw(f"admin_state:{chat_id}")

    if state == "await_broadcast_text":
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await handle_broadcast_flow({"message": msg})
        return

    if state == "await_addch":
        parts = text.strip().split(" ", 1)
        chat = parts[0]
        name = parts[1] if len(parts) > 1 else chat
        before = await get_forced_channels()
        res = await add_forced_channel(name, chat)
        after = res.get("channels", []) if res.get("ok") else before
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        if res.get("ok") and len(after) > len(before):
            await bot_send(chat_id, f"✅ «{name}» ({chat}) اضافه شد.\nتعداد کل کانال‌های اجباری الان: {len(after)}")
        else:
            await bot_send(chat_id, "❌ اضافه نشد، دوباره امتحان کن یا لاگ ریل‌وی رو چک کن.")
        return

    if state == "await_delch":
        target = text.strip()
        before = await get_forced_channels()
        res = await remove_forced_channel(target)
        after = res.get("channels", []) if res.get("ok") else before
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        if len(after) < len(before):
            await bot_send(chat_id, f"✅ حذف شد. تعداد باقی‌مانده: {len(after)}")
        else:
            await bot_send(chat_id, f"⚠️ هیچ کانالی با «{target}» پیدا نشد. دقیقاً همون مقداری رو بفرست که تو لیست «📋 لیست کانال‌های اجباری» می‌بینی.")
        return

    if state == "await_addbal_id":
        await r_set_raw(f"admin_target:{chat_id}", text.strip())
        await r_set_raw(f"admin_state:{chat_id}", "await_addbal_amount")
        await bot_send(chat_id, "مبلغ افزایش (تومان) رو بفرست:")
        return
    if state == "await_addbal_amount":
        uid = await r_get_raw(f"admin_target:{chat_id}")
        try:
            amount = int(text.strip())
        except ValueError:
            await bot_send(chat_id, "عدد نامعتبر.")
            return
        new_balance = await add_wallet(uid, amount)
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, f"✅ موجودی کاربر {uid} حالا: {new_balance:,} تومان")
        await bot_send(uid, f"💰 مبلغ {amount:,} تومان توسط ادمین به کیف‌پولت اضافه شد.")
        return

    if state == "await_subbal_id":
        await r_set_raw(f"admin_target:{chat_id}", text.strip())
        await r_set_raw(f"admin_state:{chat_id}", "await_subbal_amount")
        await bot_send(chat_id, "مبلغ کاهش (تومان) رو بفرست:")
        return
    if state == "await_subbal_amount":
        uid = await r_get_raw(f"admin_target:{chat_id}")
        try:
            amount = int(text.strip())
        except ValueError:
            await bot_send(chat_id, "عدد نامعتبر.")
            return
        new_balance = await add_wallet(uid, -amount)
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, f"✅ موجودی کاربر {uid} حالا: {new_balance:,} تومان")
        return

    if state == "await_bulkbal_amount":
        try:
            amount = int(text.strip())
        except ValueError:
            await bot_send(chat_id, "عدد نامعتبر.")
            return
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, "⏳ در حال اعمال روی همه کاربران...")
        result = await worker_api("POST", "/api/wallet/bulk-add", json={"delta": amount})
        if not result.get("ok"):
            await bot_send(chat_id, f"❌ عملیات شکست خورد: {result.get('error', 'نامشخص')}\nمطمئن شو Workerت آخرین نسخه‌ست (این قابلیت نیاز به آخرین کد Worker داره) و WORKER_URL/RAILWAY_SECRET درسته.")
            return
        count = result.get("count", 0)
        if count == 0:
            await bot_send(chat_id, "⚠️ هیچ کاربری تو دیتابیس پیدا نشد که موجودیش اضافه بشه.")
        else:
            await bot_send(chat_id, f"✅ به موجودی {count} کاربر، {amount:,} تومان اضافه شد.")
        return

    if state == "await_ban_id":
        uid = text.strip()
        info = await get_user_info(uid)
        if not info:
            await bot_send(chat_id, "کاربر پیدا نشد.")
            await r_del(f"admin_state:{chat_id}")
            await set_route_state(chat_id, False)
            return
        new_status = not info.get("banned", False)
        await set_ban(uid, new_status)
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, f"✅ کاربر {uid} حالا: {'مسدود 🚫' if new_status else 'آزاد ✅'}")
        return

    if state == "await_search_id":
        uid = text.strip()
        info = await get_user_info(uid)
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        if not info:
            await bot_send(chat_id, "کاربر پیدا نشد.")
            return
        await bot_send(chat_id,
            f"🆔 <code>{info['id']}</code>\n📛 یوزرنیم: {('@' + info['username']) if info.get('username') else 'ندارد'}\n"
            f"💰 موجودی: {info.get('balance', 0):,} تومان\n👥 زیرمجموعه: {info.get('ref_count', 0)}\n"
            f"🚫 مسدود: {'بله' if info.get('banned') else 'خیر'}")
        return

    if state == "await_price_pick":
        panels = await r_get(f"admin_panels_cache:{chat_id}") or []
        try:
            idx = int(text.strip()) - 1
            panel = panels[idx]
        except Exception:
            await bot_send(chat_id, "شماره نامعتبر.")
            return
        await r_set_raw(f"admin_price_target:{chat_id}", panel["id"])
        await r_set_raw(f"admin_state:{chat_id}", "await_price_amount")
        await bot_send(chat_id, f"قیمت جدید «{panel['title']}» رو به تومان بفرست:")
        return
    if state == "await_price_amount":
        panel_id = await r_get_raw(f"admin_price_target:{chat_id}")
        try:
            price = int(text.strip())
        except ValueError:
            await bot_send(chat_id, "عدد نامعتبر.")
            return
        await set_panel_price(panel_id, price)
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, "✅ قیمت بروزرسانی شد.")
        return

    if state == "await_deliver_text":
        order_id = await r_get_raw(f"admin_deliver_target:{chat_id}")
        await deliver_order(order_id, text)
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, "✅ تحویل ثبت شد.")
        return

    if state == "await_dm_id":
        await r_set_raw(f"admin_target:{chat_id}", text.strip())
        await r_set_raw(f"admin_state:{chat_id}", "await_dm_text")
        await bot_send(chat_id, "متن پیام رو بفرست:")
        return
    if state == "await_dm_text":
        uid = await r_get_raw(f"admin_target:{chat_id}")
        res = await bot_send(uid, text)
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, "✅ پیام ارسال شد." if res.get("ok") else f"❌ ارسال نشد: {res.get('description')}")
        return

    if state == "await_cancelorder_id":
        order_id = text.strip()
        await cancel_order(order_id)
        await r_del(f"admin_state:{chat_id}")
        await set_route_state(chat_id, False)
        await bot_send(chat_id, "✅ در صورت وجود، سفارش لغو و مبلغش به کاربر برگشت.")
        return


async def handle_admin_callback(cq):
    chat_id = cq["message"]["chat"]["id"]
    data = cq["data"]
    if str(cq["from"]["id"]) != str(ADMIN_ID):
        await bot_answer_cb(cq["id"])
        return

    if data == "admin:menu":
        await bot_send(chat_id, "پنل مدیریت:", admin_menu_kb())
        await bot_answer_cb(cq["id"])
        return

    if data == "admin:stats":
        stats = await get_stats()
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id,
            f"📊 آمار بات:\n👥 کاربران: {stats.get('users', 0)}\n📦 کل سفارش‌ها: {stats.get('orders', 0)}\n"
            f"✅ تحویل‌شده: {stats.get('delivered', 0)}\n⏳ در انتظار: {stats.get('pending', 0)}\n"
            f"💰 مجموع فروش: {stats.get('totalSales', 0):,} تومان")
        return

    if data == "admin:broadcast":
        await handle_broadcast_flow({"callback_query": cq})
        return

    if data == "admin:addch":
        await r_set_raw(f"admin_state:{chat_id}", "await_addch")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "یوزرنیم/آیدی کانال یا گروه رو همراه یه اسم بفرست، مثلا:\n<code>@channelusername کانال دوم</code>")
        return

    if data == "admin:delch":
        await r_set_raw(f"admin_state:{chat_id}", "await_delch")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "یوزرنیم کانالی که میخوای حذف بشه رو دقیقا مثل لیست بفرست.")
        return

    if data == "admin:addbal":
        await r_set_raw(f"admin_state:{chat_id}", "await_addbal_id")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "آیدی عددی کاربر رو بفرست:")
        return

    if data == "admin:subbal":
        await r_set_raw(f"admin_state:{chat_id}", "await_subbal_id")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "آیدی عددی کاربر رو بفرست:")
        return

    if data == "admin:bulkbal":
        await r_set_raw(f"admin_state:{chat_id}", "await_bulkbal_amount")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "چقدر (تومان) به موجودی همه‌ی کاربرها اضافه بشه؟")
        return

    if data == "admin:ban":
        await r_set_raw(f"admin_state:{chat_id}", "await_ban_id")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "آیدی عددی کاربر رو بفرست (وضعیت مسدودیتش برعکس می‌شه):")
        return

    if data == "admin:search":
        await r_set_raw(f"admin_state:{chat_id}", "await_search_id")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "آیدی عددی کاربر رو بفرست:")
        return

    if data == "admin:prices":
        panels = await get_panel_prices()
        await r_set(f"admin_panels_cache:{chat_id}", panels)
        lines = [f"{i+1}. {p['title']} — {p['price']:,} تومان" for i, p in enumerate(panels)]
        await r_set_raw(f"admin_state:{chat_id}", "await_price_pick")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "شماره پنلی که میخوای قیمتش رو عوض کنی بفرست:\n\n" + "\n".join(lines))
        return

    if data == "admin:orders":
        orders = await get_pending_orders()
        await bot_answer_cb(cq["id"])
        if not orders:
            await bot_send(chat_id, "سفارش در انتظاری وجود نداره.")
            return
        for o in orders[:15]:
            await bot_send(chat_id,
                f"🔢 کد: <code>{o['id']}</code>\n📦 {o['panel']}\n💰 {o['price']:,} تومان\n👤 کاربر: <code>{o['user_id']}</code>",
                {"inline_keyboard": [[{"text": "✅ تحویل بده", "callback_data": f"admin:deliver:{o['id']}"}, {"text": "🗑 لغو", "callback_data": f"admin:cancel:{o['id']}"}]]})
        return

    if data.startswith("admin:deliver:"):
        order_id = data.split(":")[2]
        await r_set_raw(f"admin_deliver_target:{chat_id}", order_id)
        await r_set_raw(f"admin_state:{chat_id}", "await_deliver_text")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "متن تحویل رو بفرست:")
        return

    if data.startswith("admin:cancel:"):
        order_id = data.split(":")[2]
        await cancel_order(order_id)
        await bot_answer_cb(cq["id"], "✅ سفارش لغو شد.")
        return

    if data == "admin:backup":
        await bot_answer_cb(cq["id"], "در حال آماده‌سازی...")
        resp = await worker_api("GET", "/api/users/full")
        if not resp.get("ok"):
            await bot_send(chat_id, f"❌ خطا در دریافت اطلاعات از Worker: {resp.get('error', 'نامشخص')}\nWORKER_URL و RAILWAY_SECRET رو چک کن.")
            return
        users = resp.get("users", [])
        if not users:
            await bot_send(chat_id, "⚠️ هیچ کاربری تو دیتابیس پیدا نشد.")
            return
        payload = json.dumps(users, ensure_ascii=False, indent=2).encode("utf-8")
        res = await bot_send_document(chat_id, payload, "users_backup.json", f"📤 بکاپ {len(users)} کاربر")
        if not res.get("ok"):
            await bot_send(chat_id, f"❌ ارسال فایل شکست خورد: {res.get('description')}")
        return

    if data == "admin:dm":
        await r_set_raw(f"admin_state:{chat_id}", "await_dm_id")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "آیدی عددی کاربری که میخوای پیام بدی رو بفرست:")
        return

    if data == "admin:listch":
        channels = await get_forced_channels()
        await bot_answer_cb(cq["id"])
        lines = [f"• {c['name']} — <code>{c['chat']}</code>" for c in channels]
        await bot_send(chat_id, "📋 کانال‌های جوین اجباری:\n\n" + ("\n".join(lines) if lines else "خالیه."))
        return

    if data == "admin:cancelorder":
        await r_set_raw(f"admin_state:{chat_id}", "await_cancelorder_id")
        await set_route_state(chat_id, True)
        await bot_answer_cb(cq["id"])
        await bot_send(chat_id, "کد سفارشی که میخوای لغو بشه رو بفرست:")
        return

    await bot_answer_cb(cq["id"])

# ============================================================
# اندپوینت اصلی
# ============================================================
@app.post("/telegram-update")
async def telegram_update(req: Request, x_secret: Optional[str] = Header(None)):
    if RAILWAY_SECRET and x_secret != RAILWAY_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    update = await req.json()

    if "message" in update:
        chat_id = update["message"]["chat"]["id"]
        is_admin = str(update["message"]["from"]["id"]) == str(ADMIN_ID)
        admin_state = await r_get_raw(f"admin_state:{chat_id}") if is_admin else None
        likeych_state = await r_get_raw(f"likeych_state:{chat_id}")
        text = update["message"].get("text", "")
        if is_admin and (text.startswith("/admin") or admin_state):
            await handle_admin_message(update["message"])
        elif likeych_state:
            await handle_likeych_message(update["message"])
        else:
            await handle_self_message(update["message"])

    elif "callback_query" in update:
        cq = update["callback_query"]
        data = cq.get("data", "")
        if data.startswith("self:"):
            await handle_self_callback(cq)
        elif data.startswith("likeych:"):
            await handle_likeych_callback(cq)
        elif data.startswith("likey:"):
            await handle_likey_click(cq)
        elif data.startswith("admin:"):
            await handle_admin_callback(cq)

    return {"ok": True}


@app.on_event("startup")
async def on_startup():
    global BOT_USER_ID
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = (await c.get(f"{BOT_API}/getMe")).json()
            if r.get("ok"):
                BOT_USER_ID = r["result"]["id"]
    except Exception:
        pass
    await reconnect_all_self_clients()
    asyncio.create_task(hourly_billing_loop())
    asyncio.create_task(referral_reversal_loop())
    asyncio.create_task(login_alert_loop())
    asyncio.create_task(likeych_expiry_loop())


@app.get("/")
async def health():
    return {"status": "running", "active_selfs": len(active_clients), "bot_user_id": BOT_USER_ID}
