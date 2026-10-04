# -*- coding: utf-8 -*-
"""
Менеджер рассылок Telegram.
Управление через бота-менеджера: аккаунты, сессии рассылки,
источники и цели. Всё хранится в SQLite и переживает рестарт.
"""

import asyncio
import json
import logging
import random
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any

import aiosqlite
from pyrogram import Client, filters, idle
from pyrogram.errors import (
    FloodWait, PeerIdInvalid, ChatWriteForbidden,
    UserBannedInChannel, ChannelPrivate, SessionPasswordNeeded,
    PhoneCodeInvalid, PhoneCodeExpired,
)
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument,
    InputMediaAudio, InputMediaAnimation,
)
from pyrogram.enums import ParseMode

import config as cfg_module


# =========================================================
# CONFIG
# =========================================================
def load_config() -> dict:
    return {
        "bot": cfg_module.BOT,
        "defaults": cfg_module.DEFAULTS,
        "settings": cfg_module.SETTINGS,
    }


_DEFAULTS: dict = {}


def apply_defaults(defaults: dict):
    _DEFAULTS.update(defaults)


def _defaults_dict():
    return dict(_DEFAULTS)


# =========================================================
# LOGGER
# =========================================================
def setup_logger(cfg: dict) -> logging.Logger:
    log_level = cfg.get("settings", {}).get("log_level", "INFO")
    log_file = cfg.get("settings", {}).get("log_file", "data/logs/bot.log")
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("manager")
    logger.setLevel(log_level)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# =========================================================
# DB
# =========================================================
SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    api_id INTEGER NOT NULL,
    api_hash TEXT NOT NULL,
    phone TEXT NOT NULL,
    session_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    account_id INTEGER NOT NULL,
    sources TEXT NOT NULL,
    targets TEXT NOT NULL,
    interval_sec REAL DEFAULT 20,
    target_delay_sec REAL DEFAULT 1.5,
    copy_mode INTEGER DEFAULT 1,
    loop_forever INTEGER DEFAULT 1,
    loop_pause_sec REAL DEFAULT 60,
    history_limit INTEGER DEFAULT 0,
    live INTEGER DEFAULT 1,
    state TEXT DEFAULT 'stopped',
    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
"""


class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    async def init(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def close(self):
        if self.conn:
            await self.conn.close()

    # ---------- ACCOUNTS ----------
    async def add_account(self, name, api_id, api_hash, phone, session_name) -> int:
        cur = await self.conn.execute(
            "INSERT INTO accounts (name, api_id, api_hash, phone, session_name) "
            "VALUES (?,?,?,?,?)",
            (name, api_id, api_hash, phone, session_name),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def list_accounts(self) -> List[aiosqlite.Row]:
        cur = await self.conn.execute("SELECT * FROM accounts ORDER BY id")
        return await cur.fetchall()

    async def get_account(self, acc_id: int):
        cur = await self.conn.execute("SELECT * FROM accounts WHERE id=?", (acc_id,))
        return await cur.fetchone()

    async def delete_account(self, acc_id: int):
        await self.conn.execute("DELETE FROM accounts WHERE id=?", (acc_id,))
        await self.conn.commit()

    # ---------- SESSIONS ----------
    async def add_session(self, name, account_id, sources: list, targets: list, **kw) -> int:
        d = _defaults_dict()
        d.update({k: v for k, v in kw.items() if v is not None})
        cur = await self.conn.execute(
            """INSERT INTO sessions
               (name, account_id, sources, targets,
                interval_sec, target_delay_sec, copy_mode,
                loop_forever, loop_pause_sec, history_limit, live)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                name, account_id, json.dumps(sources), json.dumps(targets),
                d["interval_sec"], d["target_delay_sec"], int(d["copy_mode"]),
                int(d["loop_forever"]), d["loop_pause_sec"],
                d["history_limit"], int(d["live"]),
            ),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def list_sessions(self) -> List[aiosqlite.Row]:
        cur = await self.conn.execute("SELECT * FROM sessions ORDER BY id")
        return await cur.fetchall()

    async def get_session(self, sid: int):
        cur = await self.conn.execute("SELECT * FROM sessions WHERE id=?", (sid,))
        return await cur.fetchone()

    async def update_session(self, sid: int, **kw):
        if not kw:
            return
        keys = ", ".join(f"{k}=?" for k in kw)
        vals = list(kw.values()) + [sid]
        await self.conn.execute(f"UPDATE sessions SET {keys} WHERE id=?", vals)
        await self.conn.commit()

    async def delete_session(self, sid: int):
        await self.conn.execute("DELETE FROM sessions WHERE id=?", (sid,))
        await self.conn.commit()


# =========================================================
# HELPERS
# =========================================================
def parse_targets_line(text: str) -> List[str]:
    """'a, b, c' или 'a b c' -> ['a','b','c']"""
    if not text:
        return []
    text = text.replace(",", " ")
    return [t.strip() for t in text.split() if t.strip()]


async def resolve_peer(client: Client, link: str):
    link = link.strip()
    if link.startswith("https://t.me/") or link.startswith("t.me/"):
        slug = link.split("t.me/")[-1]
        head = slug.split("/")[0]
        if head.startswith("+") or head in ("joinchat", "c"):
            chat = await client.join_chat(link)
            return chat.id
        return head
    if link.startswith("@"):
        return link
    try:
        return int(link)
    except ValueError:
        return link


# =========================================================
# ALBUM COLLECTOR
# =========================================================
class AlbumCollector:
    def __init__(self, wait_sec: float):
        self.wait_sec = wait_sec
        self.buffers: Dict[str, List[Message]] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        self.flush_cb = None

    def set_flush(self, cb):
        self.flush_cb = cb

    async def add(self, msg: Message):
        gid = msg.media_group_id
        if gid is None:
            await self.flush_cb([msg])
            return
        key = str(gid)
        if key not in self.buffers:
            self.buffers[key] = []
            self.tasks[key] = asyncio.create_task(self._timer(key))
        self.buffers[key].append(msg)

    async def _timer(self, key: str):
        await asyncio.sleep(self.wait_sec)
        msgs = self.buffers.pop(key, [])
        self.tasks.pop(key, None)
        if msgs:
            msgs.sort(key=lambda m: m.id)
            try:
                await self.flush_cb(msgs)
            except Exception:
                logging.getLogger("manager").exception("Album flush error")


# =========================================================
# BROADCAST TASK
# =========================================================
class BroadcastTask:
    def __init__(self, session_row, account_row, app: "ManagerBot"):
        self.sid = session_row["id"]
        self.name = session_row["name"]
        self.account_cfg = {
            "name": account_row["name"],
            "api_id": account_row["api_id"],
            "api_hash": account_row["api_hash"],
            "phone": account_row["phone"],
            "session_name": account_row["session_name"],
        }
        self.sources_raw = json.loads(session_row["sources"])
        self.targets_raw = json.loads(session_row["targets"])
        self.interval_sec = float(session_row["interval_sec"])
        self.target_delay_sec = float(session_row["target_delay_sec"])
        self.copy_mode = bool(session_row["copy_mode"])
        self.loop_forever = bool(session_row["loop_forever"])
        self.loop_pause_sec = float(session_row["loop_pause_sec"])
        self.history_limit = int(session_row["history_limit"])
        self.live = bool(session_row["live"])

        self.app = app
        self.log = app.log
        self.rng = random.Random()
        self.client: Optional[Client] = None
        self.source_ids: List[int] = []
        self.target_ids: List[int] = []
        self.queue: asyncio.Queue = asyncio.Queue()
        self._history_done = asyncio.Event()
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._albums: Dict[int, AlbumCollector] = {}

    async def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=15)
            except Exception:
                self._task.cancel()
        if self.client:
            try:
                await self.client.stop()
            except Exception:
                pass

    async def _run(self):
        try:
            self.client = Client(
                name=self.account_cfg["session_name"],
                api_id=self.account_cfg["api_id"],
                api_hash=self.account_cfg["api_hash"],
                phone_number=self.account_cfg["phone"],
                workdir=str(self.app.sessions_dir),
            )
            self.log.info(f"[S{self.sid}:{self.name}] Старт сессии...")
            await self.client.start()
            me = await self.client.get_me()
            self.log.info(f"[S{self.sid}:{self.name}] Вход: {me.first_name} (@{me.username})")

            # resolve sources
            for src in self.sources_raw:
                try:
                    peer = await resolve_peer(self.client, src)
                    chat = await self.client.get_chat(peer)
                    self.source_ids.append(chat.id)
                    self._albums[chat.id] = AlbumCollector(1.5)
                    self._albums[chat.id].set_flush(self._enqueue_batch)
                    self.log.info(
                        f"[S{self.sid}:{self.name}] Источник OK: {chat.title} ({chat.id})"
                    )
                except Exception as e:
                    self.log.warning(f"[S{self.sid}:{self.name}] Источник {src}: {e}")

            # resolve targets
            for t in self.targets_raw:
                try:
                    peer = await resolve_peer(self.client, t)
                    chat = await self.client.get_chat(peer)
                    self.target_ids.append(chat.id)
                    self.log.info(
                        f"[S{self.sid}:{self.name}] Цель OK: {chat.title} ({chat.id})"
                    )
                except Exception as e:
                    self.log.warning(f"[S{self.sid}:{self.name}] Цель {t}: {e}")

            if not self.target_ids:
                self.log.error(f"[S{self.sid}:{self.name}] Нет целей — стоп.")
                await self.app.set_session_state(self.sid, "stopped")
                return

            # handler
            @self.client.on_message(group=100)
            async def _handler(_client, message: Message):
                if not message.chat or message.chat.id not in self.source_ids:
                    return
                if not self.live:
                    return
                if not self._history_done.is_set():
                    return
                coll = self._albums.get(message.chat.id)
                if coll:
                    await coll.add(message)

            asyncio.create_task(self._queue_worker())
            asyncio.create_task(self._load_history())

            await self._stop.wait()
            self.log.info(f"[S{self.sid}:{self.name}] Остановлен.")
            try:
                await self.client.stop()
            except Exception:
                pass
        except Exception as e:
            self.log.exception(f"[S{self.sid}:{self.name}] Фатальная ошибка: {e}")
        finally:
            await self.app.set_session_state(self.sid, "stopped")

    async def _load_history(self):
        all_batches: List[List[Message]] = []
        for src_id in self.source_ids:
            self.log.info(f"[S{self.sid}:{self.name}] Читаю историю источника {src_id}")
            msgs: List[Message] = []
            collected = 0
            try:
                async for m in self.client.get_chat_history(src_id):
                    msgs.append(m)
                    collected += 1
                    if self.history_limit and collected >= self.history_limit:
                        break
            except Exception as e:
                self.log.error(f"[S{self.sid}:{self.name}] Ошибка истории {src_id}: {e}")

            albums: Dict[str, List[Message]] = {}
            singles: List[Message] = []
            for m in msgs:
                if m.media_group_id is None:
                    singles.append(m)
                else:
                    albums.setdefault(str(m.media_group_id), []).append(m)
            album_batches = [sorted(v, key=lambda x: x.id) for v in albums.values()]
            all_batches.extend([[m] for m in singles] + album_batches)
            self.log.info(f"[S{self.sid}:{self.name}] Из {src_id}: {len(msgs)} сообщений")

        self._history_done.set()
        if not all_batches:
            self.log.warning(f"[S{self.sid}:{self.name}] История пуста.")
            return

        pass_num = 0
        while not self._stop.is_set():
            pass_num += 1
            items = list(all_batches)
            self.rng.shuffle(items)
            self.log.info(
                f"[S{self.sid}:{self.name}] ===== Проход #{pass_num}: "
                f"{len(items)} батчей ====="
            )
            for batch in items:
                if self._stop.is_set():
                    return
                await self.queue.put(batch)
            if not self.loop_forever:
                self.log.info(f"[S{self.sid}:{self.name}] Проход завершён (loop_forever=false)")
                return
            self.log.info(
                f"[S{self.sid}:{self.name}] Пауза {self.loop_pause_sec}s до след. прохода"
            )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.loop_pause_sec)
                return
            except asyncio.TimeoutError:
                pass

    async def _enqueue_batch(self, msgs: List[Message]):
        await self.queue.put(msgs)

    async def _queue_worker(self):
        while not self._stop.is_set():
            try:
                batch = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._send_batch(batch)
            except Exception as e:
                self.log.error(f"[S{self.sid}:{self.name}] send error: {e}")

    async def _send_batch(self, msgs: List[Message]):
        first = msgs[0]
        for target in self.target_ids:
            if self._stop.is_set():
                return
            for attempt in (1, 2):
                try:
                    await self._deliver(msgs, target)
                    label = f"album x{len(msgs)}" if len(msgs) > 1 else "msg"
                    self.log.info(
                        f"[S{self.sid}:{self.name}] -> {target} | {label} id={first.id}"
                    )
                    break
                except FloodWait as e:
                    wait = int(e.value * 1.2)
                    self.log.warning(
                        f"[S{self.sid}:{self.name}] FloodWait {e.value}s -> {wait}s"
                    )
                    await asyncio.sleep(wait)
                except ChatWriteForbidden:
                    self.log.warning(f"[S{self.sid}:{self.name}] Нет прав в {target}")
                    break
                except (UserBannedInChannel, ChannelPrivate) as e:
                    self.log.warning(f"[S{self.sid}:{self.name}] Недоступен {target}: {e}")
                    break
                except PeerIdInvalid:
                    self.log.warning(f"[S{self.sid}:{self.name}] PeerIdInvalid {target}")
                    break
                except Exception as e:
                    self.log.error(f"[S{self.sid}:{self.name}] err -> {target}: {e}")
                    break
            await asyncio.sleep(self.target_delay_sec)

        await asyncio.sleep(self.interval_sec)

    async def _deliver(self, msgs: List[Message], target: int):
        if len(msgs) == 1:
            m = msgs[0]
            if self.copy_mode:
                await m.copy(chat_id=target, parse_mode=ParseMode.DISABLED)
            else:
                await m.forward(chat_id=target)
            return

        if not self.copy_mode:
            for m in msgs:
                await m.forward(chat_id=target)
            return

        media = []
        for m in msgs:
            cap = m.caption.markdown if m.caption else None
            if m.photo:
                media.append(InputMediaPhoto(media=m.photo.file_id, caption=cap))
            elif m.video:
                media.append(InputMediaVideo(media=m.video.file_id, caption=cap))
            elif m.animation:
                media.append(InputMediaAnimation(media=m.animation.file_id, caption=cap))
            elif m.audio:
                media.append(InputMediaAudio(media=m.audio.file_id, caption=cap))
            elif m.document:
                media.append(InputMediaDocument(media=m.document.file_id, caption=cap))
            else:
                await m.copy(chat_id=target, parse_mode=ParseMode.DISABLED)
        if media:
            try:
                await self.client.send_media_group(chat_id=target, media=media)
            except Exception as e:
                self.log.warning(f"[S{self.sid}:{self.name}] media_group fail: {e}")
                for m in msgs:
                    await m.copy(chat_id=target, parse_mode=ParseMode.DISABLED)


# =========================================================
# MANAGER BOT
# =========================================================
class ManagerBot:
    def __init__(self, cfg: dict, db: DB, log: logging.Logger):
        self.cfg = cfg
        self.db = db
        self.log = log
        self.owner_id = int(cfg["bot"]["owner_id"])
        self.sessions_dir = Path(cfg["settings"]["sessions_dir"])
        self.sessions_dir.mkdir(parents=True, exist_ok=True)

        self.app = Client(
            name="manager_bot",
            api_id=cfg["bot"]["api_id"],
            api_hash=cfg["bot"]["api_hash"],
            bot_token=cfg["bot"]["bot_token"],
            workdir=str(self.sessions_dir),
        )
        self.running: Dict[int, BroadcastTask] = {}
        self.fsm: Dict[str, Any] = {}

        self._register_handlers()

    async def set_session_state(self, sid: int, state: str):
        await self.db.update_session(sid, state=state)

    # ---------- handlers ----------
    def _register_handlers(self):
        app = self.app
        owner_filter = filters.private & filters.user(self.owner_id)

        @app.on_message(filters.command("start") & owner_filter)
        async def cmd_start(_, m: Message):
            self.fsm.pop("state", None)
            await m.reply(
                "👋 **Менеджер рассылок**\n\n"
                "• /accounts — аккаунты (от чьего имени шлём)\n"
                "• /sessions — сессии рассылки\n"
                "• /new — создать сессию\n"
                "• /cancel — отменить действие\n"
            )

        @app.on_message(filters.command("cancel") & owner_filter)
        async def cmd_cancel(_, m: Message):
            # чистим временный клиент, если был
            cli: Optional[Client] = self.fsm.pop("_cli", None)
            if cli:
                try:
                    await cli.disconnect()
                except Exception:
                    pass
            self.fsm.pop("state", None)
            await m.reply("Отменено.")

        # ====== ACCOUNTS ======
        @app.on_message(filters.command("accounts") & owner_filter)
        async def cmd_accounts(_, m: Message):
            rows = await self.db.list_accounts()
            text = "📱 **Аккаунты**\n\n"
            if not rows:
                text += "Пока нет ни одного аккаунта."
            else:
                for r in rows:
                    text += f"`#{r['id']}` **{r['name']}** — `{r['phone']}`\n"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="acc:add")],
                [InlineKeyboardButton("🗑 Удалить", callback_data="acc:del_menu")],
            ])
            await m.reply(text, reply_markup=kb)

        # ====== SESSIONS ======
        @app.on_message(filters.command("sessions") & owner_filter)
        async def cmd_sessions(_, m: Message):
            rows = await self.db.list_sessions()
            text = "📋 **Сессии рассылки**\n\n"
            if not rows:
                text += "Пока нет ни одной сессии."
            else:
                for r in rows:
                    emoji = "🟢" if r["state"] == "running" else "⚪️"
                    text += f"{emoji} `#{r['id']}` **{r['name']}**\n"
            buttons = []
            for r in rows:
                buttons.append([InlineKeyboardButton(
                    f"#{r['id']} {r['name']}", callback_data=f"ses:view:{r['id']}"
                )])
            buttons.append([InlineKeyboardButton("➕ Создать сессию", callback_data="ses:add")])
            await m.reply(text, reply_markup=InlineKeyboardMarkup(buttons))

        @app.on_message(filters.command("new") & owner_filter)
        async def cmd_new(_, m: Message):
            if not await self.db.list_accounts():
                await m.reply("Сначала добавь аккаунт: /accounts")
                return
            self.fsm["state"] = "ses:name"
            await m.reply("Введи **название сессии** рассылки:")

        # ====== CALLBACKS ======
        @app.on_callback_query(filters.user(self.owner_id))
        async def on_cb(_, cq: CallbackQuery):
            data = cq.data or ""
            try:
                # ---------- ACC ----------
                if data == "acc:add":
                    self.fsm["state"] = "acc:name"
                    await cq.message.edit_text(
                        "Введи **имя аккаунта** (произвольное, например `main`):"
                    )
                    await cq.answer()
                    return

                if data == "acc:del_menu":
                    rows = await self.db.list_accounts()
                    if not rows:
                        await cq.answer("Нет аккаунтов", show_alert=True)
                        return
                    buttons = [
                        [InlineKeyboardButton(
                            f"❌ {r['name']} ({r['phone']})",
                            callback_data=f"acc:del:{r['id']}"
                        )]
                        for r in rows
                    ]
                    await cq.message.edit_text(
                        "Кого удалить?", reply_markup=InlineKeyboardMarkup(buttons)
                    )
                    await cq.answer()
                    return

                if data.startswith("acc:del:"):
                    acc_id = int(data.split(":")[2])
                    await self.db.delete_account(acc_id)
                    await cq.message.edit_text("✅ Аккаунт удалён.")
                    await cq.answer()
                    return

                # ---------- SES ----------
                if data == "ses:add":
                    if not await self.db.list_accounts():
                        await cq.answer("Сначала добавь аккаунт", show_alert=True)
                        return
                    self.fsm["state"] = "ses:name"
                    await cq.message.edit_text("Введи **название сессии** рассылки:")
                    await cq.answer()
                    return

                if data == "ses:list":
                    rows = await self.db.list_sessions()
                    text = "📋 **Сессии рассылки**\n\n"
                    if not rows:
                        text += "Пока нет ни одной сессии."
                    else:
                        for r in rows:
                            emoji = "🟢" if r["state"] == "running" else "⚪️"
                            text += f"{emoji} `#{r['id']}` **{r['name']}**\n"
                    buttons = []
                    for r in rows:
                        buttons.append([InlineKeyboardButton(
                            f"#{r['id']} {r['name']}",
                            callback_data=f"ses:view:{r['id']}"
                        )])
                    buttons.append([InlineKeyboardButton(
                        "➕ Создать сессию", callback_data="ses:add"
                    )])
                    await cq.message.edit_text(
                        text, reply_markup=InlineKeyboardMarkup(buttons)
                    )
                    await cq.answer()
                    return

                if data.startswith("ses:view:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    if not row:
                        await cq.answer("Не найдено", show_alert=True)
                        return
                    acc = await self.db.get_account(row["account_id"])
                    sources = json.loads(row["sources"])
                    targets = json.loads(row["targets"])
                    state = row["state"]
                    txt = (
                        f"⚙️ **Сессия #{row['id']}** — `{row['name']}`\n"
                        f"Аккаунт: **{acc['name'] if acc else '?'}** "
                        f"({acc['phone'] if acc else '?'})\n"
                        f"Статус: **{state}**\n\n"
                        f"📥 Источники ({len(sources)}):\n"
                        + "\n".join(f"  • `{s}`" for s in sources)
                        + "\n\n"
                        f"📤 Цели ({len(targets)}):\n"
                        + "\n".join(f"  • `{t}`" for t in targets)
                        + "\n\n"
                        f"⏱ interval: `{row['interval_sec']}s` | "
                        f"target_delay: `{row['target_delay_sec']}s`\n"
                        f"🔁 loop: `{bool(row['loop_forever'])}` | "
                        f"copy: `{bool(row['copy_mode'])}` | "
                        f"live: `{bool(row['live'])}` | "
                        f"hist_limit: `{row['history_limit']}`"
                    )
                    buttons = []
                    if state == "running":
                        buttons.append([InlineKeyboardButton(
                            "⏹ Остановить", callback_data=f"ses:stop:{sid}"
                        )])
                    else:
                        buttons.append([InlineKeyboardButton(
                            "▶️ Запустить", callback_data=f"ses:start:{sid}"
                        )])
                    buttons.append([
                        InlineKeyboardButton(
                            "✏️ Источники", callback_data=f"ses:edit_src:{sid}"
                        ),
                        InlineKeyboardButton(
                            "✏️ Цели", callback_data=f"ses:edit_tgt:{sid}"
                        ),
                    ])
                    buttons.append([
                        InlineKeyboardButton(
                            "⏱ Интервал", callback_data=f"ses:edit_int:{sid}"
                        ),
                        InlineKeyboardButton(
                            "🔁 Loop", callback_data=f"ses:toggle_loop:{sid}"
                        ),
                    ])
                    buttons.append([
                        InlineKeyboardButton(
                            "📋 Copy", callback_data=f"ses:toggle_copy:{sid}"
                        ),
                        InlineKeyboardButton(
                            "🎞 Live", callback_data=f"ses:toggle_live:{sid}"
                        ),
                    ])
                    buttons.append([InlineKeyboardButton(
                        "🗑 Удалить", callback_data=f"ses:del:{sid}"
                    )])
                    buttons.append([InlineKeyboardButton(
                        "⬅️ К списку", callback_data="ses:list"
                    )])
                    await cq.message.edit_text(
                        txt, reply_markup=InlineKeyboardMarkup(buttons)
                    )
                    await cq.answer()
                    return

                if data.startswith("ses:start:"):
                    sid = int(data.split(":")[2])
                    await self._start_session(sid)
                    await cq.message.edit_text(f"▶️ Сессия #{sid} запускается...")
                    await cq.answer()
                    return

                if data.startswith("ses:stop:"):
                    sid = int(data.split(":")[2])
                    await self._stop_session(sid)
                    await cq.message.edit_text(f"⏹ Сессия #{sid} остановлена.")
                    await cq.answer()
                    return

                if data.startswith("ses:del:"):
                    sid = int(data.split(":")[2])
                    await self._stop_session(sid)
                    await self.db.delete_session(sid)
                    await cq.message.edit_text(f"🗑 Сессия #{sid} удалена.")
                    await cq.answer()
                    return

                if data.startswith("ses:edit_src:"):
                    sid = int(data.split(":")[2])
                    self.fsm["state"] = f"edit_src:{sid}"
                    await cq.message.edit_text(
                        "Пришли ссылки на **источники** через запятую или пробел.\n"
                        "Отмена: /cancel"
                    )
                    await cq.answer()
                    return

                if data.startswith("ses:edit_tgt:"):
                    sid = int(data.split(":")[2])
                    self.fsm["state"] = f"edit_tgt:{sid}"
                    await cq.message.edit_text(
                        "Пришли список **целей** через запятую.\nОтмена: /cancel"
                    )
                    await cq.answer()
                    return

                if data.startswith("ses:edit_int:"):
                    sid = int(data.split(":")[2])
                    self.fsm["state"] = f"edit_int:{sid}"
                    await cq.message.edit_text(
                        "Введи интервал в секундах (например 20):"
                    )
                    await cq.answer()
                    return

                if data.startswith("ses:toggle_loop:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    await self.db.update_session(
                        sid, loop_forever=0 if row["loop_forever"] else 1
                    )
                    await cq.answer("Переключено")
                    return

                if data.startswith("ses:toggle_copy:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    await self.db.update_session(
                        sid, copy_mode=0 if row["copy_mode"] else 1
                    )
                    await cq.answer("Переключено")
                    return

                if data.startswith("ses:toggle_live:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    await self.db.update_session(
                        sid, live=0 if row["live"] else 1
                    )
                    await cq.answer("Переключено")
                    return

                if data.startswith("ses:pick_acc:"):
                    acc_id = int(data.split(":")[2])
                    self.fsm["ses