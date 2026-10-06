# -*- coding: utf-8 -*-
"""
Менеджер рассылок Telegram. Только строковые сессии (Pyrogram string session).
Параллельная отправка во все цели одновременно.
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
    UserBannedInChannel, ChannelPrivate,
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
    log_file = cfg.get("settings", {}).get("log_file", "logs/bot.log")
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
    session_string TEXT NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    account_ids TEXT NOT NULL,
    sources TEXT NOT NULL,
    targets TEXT NOT NULL,
    interval_sec REAL DEFAULT 60,
    target_delay_sec REAL DEFAULT 5,
    copy_mode INTEGER DEFAULT 0,
    loop_forever INTEGER DEFAULT 1,
    loop_pause_sec REAL DEFAULT 300,
    history_limit INTEGER DEFAULT 0,
    live INTEGER DEFAULT 1,
    state TEXT DEFAULT 'stopped'
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

    async def add_account_string(self, name: str, session_string: str) -> int:
        cur = await self.conn.execute(
            "INSERT INTO accounts (name, session_string) VALUES (?, ?)",
            (name, session_string),
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

    async def add_session(self, name, account_ids, sources, targets, **kw) -> int:
        d = _defaults_dict()
        d.update({k: v for k, v in kw.items() if v is not None})
        cur = await self.conn.execute(
            """INSERT INTO sessions
               (name, account_ids, sources, targets,
                interval_sec, target_delay_sec, copy_mode,
                loop_forever, loop_pause_sec, history_limit, live)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                name, json.dumps(account_ids),
                json.dumps(sources), json.dumps(targets),
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


async def resolve_target_with_fallback(client: Client, link: str, log: logging.Logger, tag: str):
    try:
        peer = await resolve_peer(client, link)
        try:
            return await client.get_chat(peer)
        except Exception as e:
            log.warning(f"{tag} get_chat('{peer}') упал ({e}), пробую join_chat")
            return await client.join_chat(link)
    except Exception as e:
        log.warning(f"{tag} join_chat('{link}') не удался: {e}")
        raise


def distribute_targets(targets: List[str], n: int) -> List[List[str]]:
    buckets: List[List[str]] = [[] for _ in range(n)]
    for i, t in enumerate(targets):
        buckets[i % n].append(t)
    return buckets


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
    def __init__(self, session_row, app: "ManagerBot", account_id: int, account_row):
        self.sid = session_row["id"]
        self.name = session_row["name"]
        self.account_id = account_id
        self.account = account_row

        self.sources_raw = json.loads(session_row["sources"])
        self.targets_raw: List[str] = []
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
                await asyncio.wait_for(self._task, timeout=20)
            except Exception:
                self._task.cancel()
        if self.client:
            try:
                await self.client.stop()
            except Exception:
                pass

    def _make_client(self) -> Client:
        if not self.account["session_string"]:
            raise RuntimeError(
                f"аккаунт #{self.account_id}: нет строковой сессии"
            )
        self.log.info(
            f"[S{self.sid}:{self.name}:acc{self.account_id}] string session"
        )
        return Client(
            name=f"acc_{self.account_id}",
            api_id=self.app.cfg["bot"]["api_id"],
            api_hash=self.app.cfg["bot"]["api_hash"],
            session_string=self.account["session_string"],
            in_memory=True,
            sleep_threshold=120,
        )

    async def _run(self):
        try:
            self.client = self._make_client()
            self.log.info(f"[S{self.sid}:{self.name}:acc{self.account_id}] Старт...")
            await self.client.start()
            me = await self.client.get_me()
            self.log.info(
                f"[S{self.sid}:{self.name}:acc{self.account_id}] "
                f"Вход: {me.first_name} (@{me.username})"
            )

            try:
                count = 0
                async for _d in self.client.get_dialogs():
                    count += 1
                self.log.info(
                    f"[S{self.sid}:{self.name}:acc{self.account_id}] "
                    f"Прогрев: {count} диалогов"
                )
            except Exception as e:
                self.log.warning(f"[S{self.sid}:{self.name}] Прогрев: {e}")

            for src in self.sources_raw:
                try:
                    chat = await resolve_target_with_fallback(
                        self.client, src, self.log,
                        f"[S{self.sid}:{self.name}:acc{self.account_id}] Источник"
                    )
                    self.source_ids.append(chat.id)
                    self._albums[chat.id] = AlbumCollector(1.5)
                    self._albums[chat.id].set_flush(self._enqueue_batch)
                    self.log.info(
                        f"[S{self.sid}:{self.name}:acc{self.account_id}] "
                        f"Источник OK: {chat.title} ({chat.id})"
                    )
                except Exception as e:
                    self.log.warning(f"[S{self.sid}:{self.name}] Источник {src}: {e}")

            for t in self.targets_raw:
                try:
                    chat = await resolve_target_with_fallback(
                        self.client, t, self.log,
                        f"[S{self.sid}:{self.name}:acc{self.account_id}] Цель"
                    )
                    self.target_ids.append(chat.id)
                    self.log.info(
                        f"[S{self.sid}:{self.name}:acc{self.account_id}] "
                        f"Цель OK: {chat.title} ({chat.id})"
                    )
                except Exception as e:
                    self.log.warning(f"[S{self.sid}:{self.name}] Цель {t}: {e}")

            if not self.target_ids:
                self.log.error(
                    f"[S{self.sid}:{self.name}:acc{self.account_id}] Нет целей — стоп."
                )
                return

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
            self.log.info(f"[S{self.sid}:{self.name}:acc{self.account_id}] Остановлен.")
            try:
                await self.client.stop()
            except Exception:
                pass
        except Exception as e:
            self.log.exception(
                f"[S{self.sid}:{self.name}:acc{self.account_id}] Фатальная ошибка: {e}"
            )

    async def _load_history(self):
        all_batches: List[List[Message]] = []
        for src_id in self.source_ids:
            msgs: List[Message] = []
            collected = 0
            try:
                async for m in self.client.get_chat_history(src_id):
                    msgs.append(m)
                    collected += 1
                    if self.history_limit and collected >= self.history_limit:
                        break
            except Exception as e:
                self.log.error(f"[S{self.sid}:{self.name}] История {src_id}: {e}")

            albums: Dict[str, List[Message]] = {}
            singles: List[Message] = []
            for m in msgs:
                if getattr(m, "service", None):
                    continue
                if m.media_group_id is None:
                    singles.append(m)
                else:
                    albums.setdefault(str(m.media_group_id), []).append(m)
            album_batches = [sorted(v, key=lambda x: x.id) for v in albums.values()]
            all_batches.extend([[m] for m in singles] + album_batches)

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
                f"[S{self.sid}:{self.name}:acc{self.account_id}] ===== "
                f"Проход #{pass_num}: {len(items)} батчей ====="
            )
            for batch in items:
                if self._stop.is_set():
                    return
                await self.queue.put(batch)
            if not self.loop_forever:
                return
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
        label = f"album x{len(msgs)}" if len(msgs) > 1 else "msg"

        async def send_one(target: int):
            """Отправка в одну цель с обработкой ошибок."""
            if self._stop.is_set():
                return
            try:
                await self._deliver(msgs, target)
                self.log.info(
                    f"[S{self.sid}:{self.name}:acc{self.account_id}] -> "
                    f"{target} | {label} id={first.id}"
                )
            except FloodWait as e:
                wait = int(e.value * 1.2)
                self.log.warning(
                    f"[S{self.sid}:{self.name}] FloodWait {e.value}s -> {wait}s"
                )
                await asyncio.sleep(wait)
                if self._stop.is_set():
                    return
                try:
                    await self._deliver(msgs, target)
                    self.log.info(
                        f"[S{self.sid}:{self.name}:acc{self.account_id}] -> "
                        f"{target} | {label} id={first.id} (retry ok)"
                    )
                except Exception as e2:
                    self.log.warning(
                        f"[S{self.sid}:{self.name}] retry fail {target}: {e2}"
                    )
            except ChatWriteForbidden:
                self.log.warning(f"[S{self.sid}:{self.name}] Нет прав в {target}")
            except (UserBannedInChannel, ChannelPrivate) as e:
                self.log.warning(f"[S{self.sid}:{self.name}] {target}: {e}")
            except PeerIdInvalid:
                self.log.warning(f"[S{self.sid}:{self.name}] PeerIdInvalid {target}")
            except Exception as e:
                self.log.error(f"[S{self.sid}:{self.name}] err {target}: {e}")

        # ПАРАЛЛЕЛЬНАЯ ОТПРАВКА ВО ВСЕ ЦЕЛИ
        if self.target_ids:
            await asyncio.gather(*(send_one(t) for t in self.target_ids))

        await asyncio.sleep(self.interval_sec)

    async def _deliver(self, msgs: List[Message], target: int):
        real_msgs = [m for m in msgs if not getattr(m, "service", None)]
        if not real_msgs:
            return

        if len(real_msgs) == 1:
            m = real_msgs[0]
            try:
                if self.copy_mode:
                    await m.copy(chat_id=target, parse_mode=ParseMode.DISABLED)
                else:
                    await m.forward(chat_id=target)
            except Exception as e:
                self.log.warning(f"[S{self.sid}:{self.name}] id={m.id}: {e}")
            return

        if not self.copy_mode:
            for m in real_msgs:
                try:
                    await m.forward(chat_id=target)
                except Exception as e:
                    self.log.warning(f"[S{self.sid}:{self.name}] fwd id={m.id}: {e}")
            return

        media = []
        for m in real_msgs:
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
        if media:
            try:
                await self.client.send_media_group(chat_id=target, media=media)
            except Exception as e:
                self.log.warning(f"[S{self.sid}:{self.name}] media_group: {e}")
                for m in real_msgs:
                    try:
                        await m.forward(chat_id=target)
                    except Exception as e2:
                        self.log.warning(f"[S{self.sid}:{self.name}] fwd {m.id}: {e2}")


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
        self.running: Dict[int, List[BroadcastTask]] = {}
        self.fsm: Dict[str, Any] = {}

        self._register_handlers()

    async def set_session_state(self, sid: int, state: str):
        await self.db.update_session(sid, state=state)

    async def _show_accounts(self, message: Message, edit: bool = False):
        rows = await self.db.list_accounts()
        text = "📱 **Аккаунты**\n\n"
        if not rows:
            text += "Пока нет ни одного аккаунта."
        else:
            for r in rows:
                text += f"`#{r['id']}` **{r['name']}** — 🔑 string\n"
        buttons = [
            [InlineKeyboardButton("➕ Добавить сессию", callback_data="acc:add")],
            [InlineKeyboardButton("🗑 Удалить", callback_data="acc:del_menu")],
        ]
        if edit:
            await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))

    async def _show_sessions(self, message: Message, edit: bool = False):
        rows = await self.db.list_sessions()
        text = "📋 **Сессии рассылки**\n\n"
        if not rows:
            text += "Пока нет ни одной сессии."
        else:
            for r in rows:
                emoji = "🟢" if r["state"] == "running" else "⚪️"
                try:
                    acc_ids = json.loads(r["account_ids"])
                    accs = len(acc_ids)
                except Exception:
                    accs = 0
                text += f"{emoji} `#{r['id']}` **{r['name']}** — акк: {accs}\n"
        buttons = []
        for r in rows:
            buttons.append([InlineKeyboardButton(
                f"#{r['id']} {r['name']}", callback_data=f"ses:view:{r['id']}"
            )])
        buttons.append([InlineKeyboardButton(
            "➕ Создать сессию", callback_data="ses:add"
        )])
        if edit:
            await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))

    async def _render_account_picker(self, message: Message, edit: bool = False):
        rows = await self.db.list_accounts()
        sel = self.fsm.get("ses_selected", [])
        buttons = []
        for a in rows:
            mark = "✅" if a["id"] in sel else "▫️"
            buttons.append([InlineKeyboardButton(
                f"{mark} {a['name']}",
                callback_data=f"pick:{a['id']}"
            )])
        buttons.append([InlineKeyboardButton("✔️ Готово", callback_data="pick:done")])
        text = (
            f"Выбери аккаунты для рассылки (отмечено: {len(sel)}).\n"
            "Можно несколько. Когда закончишь — «Готово»."
        )
        if edit:
            try:
                await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
            except Exception:
                pass
        else:
            await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))

    def _register_handlers(self):
        app = self.app
        owner_filter = filters.private & filters.user(self.owner_id)

        @app.on_message(filters.command("start") & owner_filter)
        async def cmd_start(_, m: Message):
            self.fsm.pop("state", None)
            await m.reply(
                "👋 **Менеджер рассылок**\n\n"
                "• /accounts — аккаунты (строки сессий)\n"
                "• /sessions — сессии рассылки\n"
                "• /new — создать рассылку\n"
                "• /cancel — отменить действие"
            )

        @app.on_message(filters.command("cancel") & owner_filter)
        async def cmd_cancel(_, m: Message):
            self.fsm.pop("state", None)
            await m.reply("Отменено.")

        @app.on_message(filters.command("accounts") & owner_filter)
        async def cmd_accounts(_, m: Message):
            await self._show_accounts(m)

        @app.on_message(filters.command("sessions") & owner_filter)
        async def cmd_sessions(_, m: Message):
            await self._show_sessions(m)

        @app.on_message(filters.command("new") & owner_filter)
        async def cmd_new(_, m: Message):
            rows = await self.db.list_accounts()
            if not rows:
                await m.reply("❌ Сначала добавь аккаунт через /accounts")
                return
            self.fsm["state"] = "ses:name"
            await m.reply("Введи название рассылки:")

        @app.on_message(filters.text & owner_filter, group=1)
        async def fsm_handler(_, m: Message):
            if self.fsm.get("state") == "acc:string_wait":
                text = m.text.strip()
                if not text or len(text) < 100:
                    await m.reply("❌ Это не похоже на строку сессии. Пришли ещё раз или /cancel.")
                    return
                name = self.fsm.get("acc_pending_name") or f"acc_{int(asyncio.get_event_loop().time())}"
                try:
                    await self.db.add_account_string(name, text)
                except Exception as e:
                    await m.reply(f"❌ Не удалось сохранить: {e}")
                    self.fsm.pop("state", None)
                    return
                self.fsm.pop("state", None)
                await m.reply(f"✅ Аккаунт `{name}` добавлен (string).")
                return

            state = self.fsm.get("state")
            if not state:
                return
            text = m.text.strip()

            if state == "acc:name":
                self.fsm["acc_pending_name"] = text
                self.fsm["state"] = "acc:string_wait"
                await m.reply(
                    "Пришли **строку сессии** сюда одним сообщением "
                    "(начинается с `AgGB...`).\n"
                    "Отмена: /cancel"
                )
                return

            if state == "ses:name":
                self.fsm["ses_name"] = text
                rows = await self.db.list_accounts()
                if not rows:
                    await m.reply("❌ Нет аккаунтов.")
                    self.fsm.pop("state", None)
                    return
                self.fsm["ses_selected"] = []
                self.fsm["state"] = "ses:pick_acc"
                await self._render_account_picker(m)
                return

            if state == "ses:sources":
                items = parse_targets_line(text)
                if not items:
                    await m.reply("Пусто. /cancel")
                    return
                self.fsm["ses_sources"] = items
                self.fsm["state"] = "ses:targets"
                await m.reply(
                    f"Принято {len(items)} источников. Теперь цели через запятую:"
                )
                return

            if state == "ses:targets":
                items = parse_targets_line(text)
                if not items:
                    await m.reply("Пусто. /cancel")
                    return
                account_ids = self.fsm.get("ses_selected", [])
                if not account_ids:
                    await m.reply("❌ Не выбран ни один аккаунт. /cancel и заново.")
                    return
                name = self.fsm["ses_name"]
                sources = self.fsm["ses_sources"]
                sid = await self.db.add_session(name, account_ids, sources, items)
                self.fsm.pop("state", None)
                await m.reply(
                    f"✅ Сессия #{sid} **{name}** создана.\n"
                    f"Аккаунтов: {len(account_ids)}\n"
                    f"Запустить: /sessions → выбери её."
                )
                return

            if state.startswith("edit_src:"):
                sid = int(state.split(":")[1])
                items = parse_targets_line(text)
                if not items:
                    await m.reply("Пусто. /cancel")
                    return
                await self.db.update_session(sid, sources=json.dumps(items))
                self.fsm.pop("state", None)
                await m.reply(f"✅ Источники обновлены ({len(items)}).")
                return

            if state.startswith("edit_tgt:"):
                sid = int(state.split(":")[1])
                items = parse_targets_line(text)
                if not items:
                    await m.reply("Пусто. /cancel")
                    return
                await self.db.update_session(sid, targets=json.dumps(items))
                self.fsm.pop("state", None)
                await m.reply(f"✅ Цели обновлены ({len(items)}).")
                return

            if state.startswith("edit_int:"):
                sid = int(state.split(":")[1])
                try:
                    val = float(text)
                    if val <= 0:
                        raise ValueError
                except ValueError:
                    await m.reply("Введи положительное число:")
                    return
                await self.db.update_session(sid, interval_sec=val)
                self.fsm.pop("state", None)
                await m.reply(f"✅ Интервал = {val}s")
                return

        @app.on_callback_query(filters.user(self.owner_id))
        async def on_cb(_, cq: CallbackQuery):
            data = cq.data or ""
            try:
                if data == "acc:add":
                    self.fsm["state"] = "acc:name"
                    await cq.message.edit_text(
                        "Введи **имя** для нового аккаунта (например `acc1`):"
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
                            f"❌ {r['name']}",
                            callback_data=f"acc:del:{r['id']}"
                        )] for r in rows
                    ]
                    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="acc:back")])
                    await cq.message.edit_text(
                        "Кого удалить?", reply_markup=InlineKeyboardMarkup(buttons)
                    )
                    await cq.answer()
                    return

                if data == "acc:back":
                    await self._show_accounts(cq.message, edit=True)
                    await cq.answer()
                    return

                if data.startswith("acc:del:"):
                    acc_id = int(data.split(":")[2])
                    await self.db.delete_account(acc_id)
                    await cq.message.edit_text("✅ Аккаунт удалён.")
                    await cq.answer()
                    return

                if data == "ses:add":
                    rows = await self.db.list_accounts()
                    if not rows:
                        await cq.answer("Сначала добавь аккаунт", show_alert=True)
                        return
                    self.fsm["state"] = "ses:name"
                    await cq.message.edit_text("Введи название рассылки:")
                    await cq.answer()
                    return

                if data == "ses:list":
                    await self._show_sessions(cq.message, edit=True)
                    await cq.answer()
                    return

                if data.startswith("pick:"):
                    if data == "pick:done":
                        sel = self.fsm.get("ses_selected", [])
                        if not sel:
                            await cq.answer("Выбери хотя бы один аккаунт", show_alert=True)
                            return
                        self.fsm["state"] = "ses:sources"
                        await cq.message.edit_text(
                            f"Выбрано аккаунтов: {len(sel)}.\n\n"
                            "Пришли **источники** (откуда пересылать) через запятую "
                            "или пробел."
                        )
                        await cq.answer()
                        return
                    acc_id = int(data.split(":")[1])
                    sel = self.fsm.setdefault("ses_selected", [])
                    if acc_id in sel:
                        sel.remove(acc_id)
                    else:
                        sel.append(acc_id)
                    await self._render_account_picker(cq.message, edit=True)
                    await cq.answer()
                    return

                if data.startswith("ses:view:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    if not row:
                        await cq.answer("Не найдено", show_alert=True)
                        return
                    sources = json.loads(row["sources"])
                    targets = json.loads(row["targets"])
                    try:
                        acc_ids = json.loads(row["account_ids"])
                    except Exception:
                        acc_ids = []
                    state = row["state"]
                    txt = (
                        f"⚙️ **Сессия #{row['id']}** — `{row['name']}`\n"
                        f"Статус: **{state}**\n"
                        f"Аккаунтов: {len(acc_ids)}\n\n"
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
                        f"live: `{bool(row['live'])}`"
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
                        InlineKeyboardButton("✏️ Источники", callback_data=f"ses:edit_src:{sid}"),
                        InlineKeyboardButton("✏️ Цели", callback_data=f"ses:edit_tgt:{sid}"),
                    ])
                    buttons.append([
                        InlineKeyboardButton("⏱ Интервал", callback_data=f"ses:edit_int:{sid}"),
                        InlineKeyboardButton("🔁 Loop", callback_data=f"ses:toggle_loop:{sid}"),
                    ])
                    buttons.append([
                        InlineKeyboardButton("📋 Copy", callback_data=f"ses:toggle_copy:{sid}"),
                        InlineKeyboardButton("🎞 Live", callback_data=f"ses:toggle_live:{sid}"),
                    ])
                    buttons.append([
                        InlineKeyboardButton("👥 Аккаунты", callback_data=f"ses:edit_accs:{sid}")
                    ])
                    buttons.append([InlineKeyboardButton("🗑 Удалить", callback_data=f"ses:del:{sid}")])
                    buttons.append([InlineKeyboardButton("⬅️ К списку", callback_data="ses:list")])
                    await cq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(buttons))
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
                        "Пришли ссылки на источники через запятую или пробел.\n"
                        "Отмена: /cancel"
                    )
                    await cq.answer()
                    return

                if data.startswith("ses:edit_tgt:"):
                    sid = int(data.split(":")[2])
                    self.fsm["state"] = f"edit_tgt:{sid}"
                    await cq.message.edit_text(
                        "Пришли список целей через запятую.\nОтмена: /cancel"
                    )
                    await cq.answer()
                    return

                if data.startswith("ses:edit_int:"):
                    sid = int(data.split(":")[2])
                    self.fsm["state"] = f"edit_int:{sid}"
                    await cq.message.edit_text("Введи интервал в секундах:")
                    await cq.answer()
                    return

                if data.startswith("ses:edit_accs:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    if not row:
                        await cq.answer("Не найдено", show_alert=True)
                        return
                    try:
                        cur_ids = json.loads(row["account_ids"])
                    except Exception:
                        cur_ids = []
                    self.fsm[f"edit_accs_pending:{sid}"] = list(cur_ids)
                    accs = await self.db.list_accounts()
                    buttons = []
                    for a in accs:
                        mark = "✅" if a["id"] in cur_ids else "▫️"
                        buttons.append([InlineKeyboardButton(
                            f"{mark} {a['name']}",
                            callback_data=f"edit_accs_pick:{sid}:{a['id']}"
                        )])
                    buttons.append([InlineKeyboardButton(
                        "💾 Сохранить", callback_data=f"edit_accs_save:{sid}"
                    )])
                    buttons.append([InlineKeyboardButton(
                        "⬅️ Назад", callback_data=f"ses:view:{sid}"
                    )])
                    await cq.message.edit_text(
                        "Отметь аккаунты кнопками, потом «Сохранить»:",
                        reply_markup=InlineKeyboardMarkup(buttons),
                    )
                    await cq.answer()
                    return

                if data.startswith("edit_accs_pick:"):
                    _, sid_s, acc_id_s = data.split(":")
                    sid = int(sid_s)
                    acc_id = int(acc_id_s)
                    pending = self.fsm.setdefault(f"edit_accs_pending:{sid}", [])
                    if acc_id in pending:
                        pending.remove(acc_id)
                    else:
                        pending.append(acc_id)
                    accs = await self.db.list_accounts()
                    buttons = []
                    for a in accs:
                        mark = "✅" if a["id"] in pending else "▫️"
                        buttons.append([InlineKeyboardButton(
                            f"{mark} {a['name']}",
                            callback_data=f"edit_accs_pick:{sid}:{a['id']}"
                        )])
                    buttons.append([InlineKeyboardButton(
                        "💾 Сохранить", callback_data=f"edit_accs_save:{sid}"
                    )])
                    buttons.append([InlineKeyboardButton(
                        "⬅️ Назад", callback_data=f"ses:view:{sid}"
                    )])
                    await cq.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
                    await cq.answer()
                    return

                if data.startswith("edit_accs_save:"):
                    sid = int(data.split(":")[1])
                    pending = self.fsm.pop(f"edit_accs_pending:{sid}", [])
                    if not pending:
                        await cq.answer("Нужен хотя бы один аккаунт", show_alert=True)
                        return
                    await self.db.update_session(sid, account_ids=json.dumps(pending))
                    await cq.message.edit_text(
                        f"✅ Аккаунтов сохранено: {len(pending)}"
                    )
                    await cq.answer()
                    return

                if data.startswith("ses:toggle_loop:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    await self.db.update_session(sid, loop_forever=0 if row["loop_forever"] else 1)
                    await cq.answer("Переключено")
                    return

                if data.startswith("ses:toggle_copy:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    await self.db.update_session(sid, copy_mode=0 if row["copy_mode"] else 1)
                    await cq.answer("Переключено")
                    return

                if data.startswith("ses:toggle_live:"):
                    sid = int(data.split(":")[2])
                    row = await self.db.get_session(sid)
                    await self.db.update_session(sid, live=0 if row["live"] else 1)
                    await cq.answer("Переключено")
                    return

            except Exception as e:
                self.log.exception(f"callback error: {e}")
                try:
                    await cq.answer("Ошибка", show_alert=True)
                except Exception:
                    pass

    async def _start_session(self, sid: int):
        if sid in self.running:
            return
        row = await self.db.get_session(sid)
        if not row:
            return
        try:
            account_ids = json.loads(row["account_ids"])
        except Exception:
            account_ids = []
        targets = json.loads(row["targets"])

        valid_ids = []
        for acc_id in account_ids:
            acc = await self.db.get_account(acc_id)
            if acc and acc["session_string"]:
                valid_ids.append(acc_id)

        if not valid_ids:
            self.log.error(f"[S{sid}] нет аккаунтов со строковой сессией")
            await self.db.update_session(sid, state="stopped")
            return

        buckets = distribute_targets(targets, len(valid_ids))
        tasks: List[BroadcastTask] = []
        for i, acc_id in enumerate(valid_ids):
            acc = await self.db.get_account(acc_id)
            task = BroadcastTask(row, self, acc_id, acc)
            task.targets_raw = buckets[i] if len(buckets) > i else []
            if not task.targets_raw:
                self.log.info(f"[S{sid}] {acc['name']}: 0 целей, пропуск")
                continue
            tasks.append(task)
            await task.start()

        if not tasks:
            self.log.error(f"[S{sid}] нет активных задач")
            await self.db.update_session(sid, state="stopped")
            return
        self.running[sid] = tasks
        await self.db.update_session(sid, state="running")

    async def _stop_session(self, sid: int):
        tasks = self.running.pop(sid, None)
        if tasks:
            for t in tasks:
                try:
                    await t.stop()
                except Exception:
                    pass
        await self.db.update_session(sid, state="stopped")

    async def run(self):
        await self.app.start()
        me = await self.app.get_me()
        self.log.info(f"Бот запущен: @{me.username}")

        rows = await self.db.list_sessions()
        for r in rows:
            if r["state"] == "running":
                self.log.info(f"Возобновляю сессию #{r['id']} {r['name']}")
                await self._start_session(r["id"])

        await idle()

    async def stop(self):
        for sid in list(self.running.keys()):
            await self._stop_session(sid)
        await self.app.stop()


# =========================================================
# MAIN
# =========================================================
async def main():
    cfg = load_config()
    apply_defaults(cfg["defaults"])
    log = setup_logger(cfg)

    db = DB(cfg["settings"]["db_path"])
    await db.init()

    bot = ManagerBot(cfg, db, log)
    try:
        await bot.run()
    finally:
        await bot.stop()
        await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено.")