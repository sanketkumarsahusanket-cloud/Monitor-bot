#!/usr/bin/env python3
"""
monitor_bot.py — Telegram Relay-Based Owner Monitoring Bot
Koi HTTP API nahi — main bot ka data ek PRIVATE relay channel
ke through aata hai, monitor bot usko parse karke SQLite me
store karta hai aur owner ko instant notification bhejta hai.

Wispbyte env vars:
  MONITOR_BOT_TOKEN  — @BotFather ka naya token (monitor bot)
  OWNER_CHAT_ID       — tumhara personal Telegram ID
  RELAY_SECRET        — shared secret code (main bot me same hoga)
  MONITOR_DB_PATH     — optional, default: monitor_data.db
  PORT                — optional, default: 8080
"""

import os
import html
import json
import sqlite3
import logging
import asyncio
from datetime import datetime

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage

# ================= CONFIG =================
MONITOR_BOT_TOKEN = os.environ.get("MONITOR_BOT_TOKEN", "").strip()
OWNER_CHAT_ID_RAW = os.environ.get("OWNER_CHAT_ID", "0").strip()
OWNER_CHAT_ID = int(OWNER_CHAT_ID_RAW) if OWNER_CHAT_ID_RAW.lstrip("-").isdigit() else 0
RELAY_SECRET = os.environ.get("RELAY_SECRET", os.environ.get("MONITOR_API_KEY", "")).strip()
DB_PATH = os.environ.get("MONITOR_DB_PATH", "monitor_data.db").strip() or "monitor_data.db"
PORT = int(os.environ.get("PORT", 8080))
DELETE_RELAY_MSGS = os.environ.get("DELETE_RELAY_MSGS", "1").strip() in ("1", "true", "yes")

RELAY_PREFIX = "MONITOR::"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("MonitorBot")

_ROUTER = Router()

# ================= DATABASE =================
def init_db() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id        TEXT PRIMARY KEY,
            uid            INTEGER NOT NULL,
            username       TEXT DEFAULT '',
            first_name     TEXT DEFAULT '',
            activity       TEXT DEFAULT '',
            feature        TEXT DEFAULT '',
            target         TEXT DEFAULT '',
            message_content TEXT DEFAULT '',
            attempts       INTEGER DEFAULT 0,
            success        INTEGER DEFAULT 0,
            failed         INTEGER DEFAULT 0,
            status         TEXT DEFAULT '',
            start_time     INTEGER DEFAULT 0,
            end_time       INTEGER DEFAULT 0,
            created_at     INTEGER DEFAULT 0
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at)")
    con.commit()
    con.close()
    log.info(f"SQLite ready: {DB_PATH}")

def db_insert_event(ev: dict) -> bool:
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            """INSERT OR IGNORE INTO events
               (event_id, uid, username, first_name, activity, feature, target,
                message_content, attempts, success, failed, status,
                start_time, end_time, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ev["event_id"], ev["uid"], ev.get("username",""), ev.get("first_name",""),
             ev.get("activity",""), ev.get("feature",""), ev.get("target",""),
             ev.get("message_content",""), int(ev.get("attempts",0) or 0),
             int(ev.get("success",0) or 0), int(ev.get("failed",0) or 0),
             ev.get("status",""), int(ev.get("start_time",0) or 0),
             int(ev.get("end_time",0) or 0), int(ev.get("end_time",0) or 0)))
        inserted = con.total_changes > 0
        con.commit()
        con.close()
        return inserted
    except sqlite3.Error as e:
        log.error(f"DB insert error: {e}")
        return False

def db_recent(limit: int = 10):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT * FROM events ORDER BY created_at DESC, rowid DESC LIMIT ?", (limit,)).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except sqlite3.Error as e:
        log.error(f"DB recent error: {e}")
        return []

def db_count() -> int:
    try:
        con = sqlite3.connect(DB_PATH)
        n = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        con.close()
        return n
    except sqlite3.Error as e:
        log.error(f"DB count error: {e}")
        return -1

def fmt_time(ts) -> str:
    try:
        return datetime.fromtimestamp(int(ts or 0)).strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return "unknown"

# ================ RELAY PARSER ================
def _parse_relay(text: str):
    """Returns: dict (valid event) / 'BADKEY' / None"""
    if not text or not text.startswith(RELAY_PREFIX):
        return None
    try:
        data = json.loads(text[len(RELAY_PREFIX):])
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    if data.get("key") != RELAY_SECRET:
        return "BADKEY"
    if not data.get("event_id") or "uid" not in data or not data.get("activity"):
        return None
    for k in ("attempts", "success", "failed"):
        try:
            data[k] = int(data.get(k, 0) or 0)
        except (TypeError, ValueError):
            data[k] = 0
    return data

# ============ OWNER NOTIFICATION ============
def build_notification(ev: dict) -> str:
    uname = ev.get("username") or "No Username"
    ts = ev.get("end_time") or ev.get("start_time") or ev.get("created_at", 0)
    msg_content = ev.get("message_content") or ""
    if len(msg_content) > 500:
        msg_content = msg_content[:500] + "..."
    lines = [
        "🔔 <b>MAIN BOT ACTIVITY</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"👤 <b>User:</b> {html.escape(str(ev.get('first_name','') or 'Unknown'))}",
        f"🆔 <b>User ID:</b> <code>{ev.get('uid')}</code>",
        f"🌐 <b>Username:</b> {html.escape(str(uname))}",
        f"⚙️ <b>Activity:</b> {html.escape(str(ev.get('activity','')))}",
        f"🧩 <b>Feature:</b> {html.escape(str(ev.get('feature','')))}",
    ]
    if ev.get("target"):
        lines.append(f"📱 <b>Target:</b> <code>{html.escape(str(ev.get('target')))}</code>")
    if msg_content:
        lines.append(f"✉️ <b>Message:</b> <i>{html.escape(msg_content)}</i>")
    if int(ev.get("attempts", 0) or 0):
        lines.append(f"🔢 <b>Total Attempts:</b> <b>{ev.get('attempts')}</b>")
        lines.append(f"✅ <b>Successful:</b> <b>{ev.get('success',0)}</b>   ❌ <b>Failed:</b> <b>{ev.get('failed',0)}</b>")
    if ev.get("start_time"):
        lines.append(f"🟢 <b>Started:</b> {fmt_time(ev.get('start_time'))}")
        lines.append(f"🏁 <b>Finished:</b> {fmt_time(ev.get('end_time') or ev.get('start_time'))}")
    lines.append(f"📌 <b>Status:</b> {html.escape(str(ev.get('status','') or 'N/A'))}")
    lines.append(f"🕒 <b>Time:</b> {fmt_time(ts)}")
    lines.append("━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)

# ============ CHANNEL POST HANDLER (MAIN LOGIC) ============
@_ROUTER.channel_post()
async def relay_channel_post(post: Message):
    text = post.text or ""
    parsed = _parse_relay(text)

    if parsed == "BADKEY":
        log.warning("Relay message with WRONG secret received — ignoring")
        if DELETE_RELAY_MSGS:
            try: await post.delete()
            except Exception: pass
        return

    if isinstance(parsed, dict):
        # Valid event from main bot
        inserted = await asyncio.to_thread(db_insert_event, parsed)
        if inserted:
            try:
                await post.bot.send_message(OWNER_CHAT_ID, build_notification(parsed), parse_mode="HTML")
            except Exception as e:
                log.error(f"Owner notify failed: {e}")
            log.info(f"Stored event {parsed['event_id']} | uid={parsed['uid']}")
        else:
            log.info(f"Duplicate event ignored: {parsed['event_id']}")
        if DELETE_RELAY_MSGS:
            try: await post.delete()
            except Exception: pass
        return

    # Plain message — agar owner ne khud post kiya to usko Channel ID bata do (auto-detect)
    if post.from_user and post.from_user.id == OWNER_CHAT_ID:
        try:
            await post.bot.send_message(
                OWNER_CHAT_ID,
                f"📡 <b>Relay Channel Detected!</b>\n\n"
                f"Ye tumhara relay channel hai:\n"
                f"<code>{post.chat.id}</code>\n\n"
                f"➡️ Isko main bot me <code>_MONITOR_RELAY_CHANNEL</code> me daalo.",
                parse_mode="HTML")
        except Exception:
            pass

# ============ OWNER COMMANDS ============
def _is_owner(msg: Message) -> bool:
    return bool(msg.from_user and msg.from_user.id == OWNER_CHAT_ID)

@_ROUTER.message(CommandStart())
async def cmd_start(msg: Message):
    if not _is_owner(msg):
        await msg.answer("🚫 This monitoring bot is private. Owner only.")
        return
    await msg.answer(
        "📡 <b>MAIN BOT MONITOR</b> — Online!\n\n"
        "Relay-mode active (no HTTP API needed).\n\n"
        "Commands: /recent, /count, /help",
        parse_mode="HTML")

@_ROUTER.message(Command("recent"))
async def cmd_recent(msg: Message):
    if not _is_owner(msg):
        await msg.answer("🚫 Owner only.")
        return
    rows = await asyncio.to_thread(db_recent, 10)
    if not rows:
        await msg.answer("📭 No activity recorded yet.")
        return
    lines = ["📋 <b>Latest 10 Activities</b>\n━━━━━━━━━━━━━━━━━━"]
    for i, r in enumerate(rows, 1):
        lines.append(
            f"{i}. <b>{html.escape(str(r.get('activity','')))}</b> — "
            f"{html.escape(str(r.get('first_name','') or '?'))} (<code>{r.get('uid')}</code>)\n"
            f"   🌐 {html.escape(str(r.get('username') or 'No Username'))} | 📱 "
            f"<code>{html.escape(str(r.get('target','') or '—'))}</code>\n"
            f"   🔢 {r.get('attempts',0)} | ✅ {r.get('success',0)} | ❌ {r.get('failed',0)} "
            f"| 📌 {html.escape(str(r.get('status','') or '—'))}\n"
            f"   🕒 {fmt_time(r.get('end_time') or r.get('start_time'))}")
    await msg.answer("\n".join(lines), parse_mode="HTML")

@_ROUTER.message(Command("count"))
async def cmd_count(msg: Message):
    if not _is_owner(msg):
        await msg.answer("🚫 Owner only.")
        return
    n = await asyncio.to_thread(db_count)
    await msg.answer(f"📊 <b>Total recorded activities:</b> <b>{n}</b>", parse_mode="HTML")

@_ROUTER.message(Command("help"))
async def cmd_help(msg: Message):
    if not _is_owner(msg):
        await msg.answer("🚫 Owner only.")
        return
    await msg.answer(
        "🛠 <b>Monitor Bot Commands</b>\n\n"
        "/start — Check monitor is online\n"
        "/recent — Latest 10 activity records\n"
        "/count — Total recorded activities\n"
        "/help — This help\n\n"
        "Setup reminder: /setup dekho backup ke liye (agar channel code bhool jao).\n"
        "Sab automatic — main bot relay channel me event bhejta hai.",
        parse_mode="HTML")

async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "monitor-bot"})

# ================= MAIN =================
async def main():
    if not MONITOR_BOT_TOKEN:
        raise SystemExit("MONITOR_BOT_TOKEN env var missing!")
    if not RELAY_SECRET:
        raise SystemExit("RELAY_SECRET env var missing!")
    if not OWNER_CHAT_ID:
        raise SystemExit("OWNER_CHAT_ID env var missing!")

    init_db()
    bot = Bot(token=MONITOR_BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(_ROUTER)

    # Chhota web server — sirf Wispbyte keep-alive/health ke liye
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"Health server on port {PORT}")

    me = await bot.get_me()
    log.info(f"Monitoring bot @{me.username} started (owner: {OWNER_CHAT_ID})")
    try:
        await bot.send_message(
            OWNER_CHAT_ID,
            "📡 <b>MONITORING BOT ONLINE</b> (Relay Mode)\n"
            f"🤖 @{me.username}\n\n"
            "Setup steps:\n"
            "1. Ek private channel banao\n"
            "2. Monitor bot ko admin banao\n"
            "3. Us channel me 'hi' likho — main tumhe Channel ID DM kar dunga\n"
            "4. Main bot ko bhi channel me admin banao\n"
            "5. Channel ID main bot me daalo",
            parse_mode="HTML")
    except Exception as e:
        log.warning(f"Could not DM owner on startup: {e}")

    await dp.start_polling(bot, allowed_updates=["message", "channel_post"])

if __name__ == "__main__":
    asyncio.run(main())