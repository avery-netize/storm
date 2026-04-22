"""
Discord-бот Storm: панель заявок (embed + меню), модалки РП/VZP, приватные тикет-каналы,
join-to-create голосовые комнаты с панелью управления.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import traceback
from pathlib import Path
from typing import Any, Callable, Iterable

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("storm-bot")

BRAND = "Storm famq"

SETTINGS_PATH = Path(__file__).resolve().parent / "application_settings.json"
DB_PATH = Path(__file__).resolve().parent / "applications.db"

# Кэш application_settings.json по mtime — быстрый ответ на select (иначе 10062)
_settings_file_cache: dict[str, Any] | None = None
_settings_file_mtime: float | None = None

# Положи картинку рядом с bot.py или укажи PANEL_THUMBNAIL_PATH в .env (имя в embed: attachment://…)
DEFAULT_PANEL_THUMBNAIL = "panel_thumbnail.png"


def _panel_thumbnail_path() -> Path:
    raw = os.getenv("PANEL_THUMBNAIL_PATH", "").strip()
    base = Path(__file__).resolve().parent
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else (base / p)
    default = base / DEFAULT_PANEL_THUMBNAIL
    if default.is_file():
        return default
    candidates = sorted(base.glob("*.png"))
    if candidates:
        return candidates[0]
    return default


def _ensure_applications_schema(conn: sqlite3.Connection) -> None:
    """Таблица заявок + миграции столбцов (канал, статус, закрытие)."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL
        )
        """
    )
    cur.execute("PRAGMA table_info(applications)")
    have = {row[1] for row in cur.fetchall()}
    for col, typ in (
        ("guild_id", "INTEGER"),
        ("channel_id", "INTEGER"),
        ("status", "TEXT DEFAULT 'open'"),
        ("created_at", "TEXT"),
        ("closed_at", "TEXT"),
        ("close_kind", "TEXT"),
        ("close_note", "TEXT"),
    ):
        if col not in have:
            cur.execute(f"ALTER TABLE applications ADD COLUMN {col} {typ}")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_applications_channel ON applications(channel_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_applications_status ON applications(status)"
    )


def _insert_application(user_id: int, kind: str, guild_id: int) -> int:
    """Новая заявка (ещё без канала тикета)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        _ensure_applications_schema(conn)
        cur.execute(
            """
            INSERT INTO applications(user_id, kind, guild_id, status, created_at)
            VALUES (?, ?, ?, 'open', datetime('now'))
            """,
            (user_id, kind, guild_id),
        )
        app_id = int(cur.lastrowid)
        conn.commit()
        return app_id
    finally:
        conn.close()


def _bind_ticket_channel(app_id: int, channel_id: int) -> None:
    """Привязать созданный тикет-канал к записи заявки."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        _ensure_applications_schema(conn)
        cur.execute(
            "UPDATE applications SET channel_id = ? WHERE id = ?",
            (channel_id, app_id),
        )
        conn.commit()
    finally:
        conn.close()


def _close_ticket_in_db(
    channel_id: int,
    *,
    close_kind: str,
    close_note: str | None = None,
) -> None:
    """Пометить заявку закрытой (канал может быть уже удалён в Discord)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        _ensure_applications_schema(conn)
        cur.execute(
            """
            UPDATE applications
            SET status = 'closed',
                closed_at = datetime('now'),
                close_kind = ?,
                close_note = ?
            WHERE channel_id = ?
            """,
            (close_kind, close_note, channel_id),
        )
        conn.commit()
    finally:
        conn.close()


def _parse_id_list(raw: str | None) -> list[int]:
    if not raw:
        return []
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out


def _staff_role_ids() -> list[int]:
    return _parse_id_list(os.getenv("STAFF_ROLE_IDS"))


def _ticket_category_id() -> int | None:
    raw = os.getenv("TICKET_CATEGORY_ID", "").strip()
    return int(raw) if raw.isdigit() else None


def _guild_id() -> int | None:
    raw = os.getenv("GUILD_ID", "").strip()
    return int(raw) if raw.isdigit() else None


def _accept_role_rp_env() -> int | None:
    raw = os.getenv("ACCEPT_ROLE_STORM_ID", "").strip()
    return int(raw) if raw.isdigit() else None


def _accept_role_vzp_env() -> int | None:
    raw = os.getenv("ACCEPT_ROLE_VZP_ID", "").strip()
    return int(raw) if raw.isdigit() else None


def get_guild_accept_role_rp_id(guild_id: int) -> int | None:
    """Роль для кнопки «Принять в Storm»; иначе ACCEPT_ROLE_STORM_ID из .env."""
    data = _load_settings_file()
    g = data.get(str(guild_id))
    if isinstance(g, dict) and "accept_role_rp_id" in g:
        raw = g["accept_role_rp_id"]
        if str(raw).isdigit():
            return int(raw)
        return None
    return _accept_role_rp_env()


def get_guild_accept_role_vzp_id(guild_id: int) -> int | None:
    """Роль для кнопки «Принять в VZP»; иначе ACCEPT_ROLE_VZP_ID из .env."""
    data = _load_settings_file()
    g = data.get(str(guild_id))
    if isinstance(g, dict) and "accept_role_vzp_id" in g:
        raw = g["accept_role_vzp_id"]
        if str(raw).isdigit():
            return int(raw)
        return None
    return _accept_role_vzp_env()


def set_guild_accept_role_rp_id(guild_id: int, role_id: int | None) -> None:
    data = _load_settings_file()
    gr = data.setdefault(str(guild_id), {})
    if not isinstance(gr, dict):
        gr = {}
        data[str(guild_id)] = gr
    if role_id is None:
        gr.pop("accept_role_rp_id", None)
    else:
        gr["accept_role_rp_id"] = role_id
    _save_settings_file(data)


def set_guild_accept_role_vzp_id(guild_id: int, role_id: int | None) -> None:
    data = _load_settings_file()
    gr = data.setdefault(str(guild_id), {})
    if not isinstance(gr, dict):
        gr = {}
        data[str(guild_id)] = gr
    if role_id is None:
        gr.pop("accept_role_vzp_id", None)
    else:
        gr["accept_role_vzp_id"] = role_id
    _save_settings_file(data)


def _voice_hub_channel_id() -> int | None:
    """ID «хаб»-канала: заход в него создаёт личный войс (VOICE_HUB_CHANNEL_ID в .env)."""
    raw = os.getenv("VOICE_HUB_CHANNEL_ID", "").strip()
    return int(raw) if raw.isdigit() else None


def _voice_create_category_id() -> int | None:
    """Категория для новых войсов; если пусто — категория хаба."""
    raw = os.getenv("VOICE_CREATE_CATEGORY_ID", "").strip()
    return int(raw) if raw.isdigit() else None


# --- Join-to-create: состояние в памяти ---
_voice_owner_channel: dict[int, int] = {}  # owner_id -> voice_channel_id
_voice_channel_owner: dict[int, int] = {}  # voice_channel_id -> owner_id
_voice_friends: dict[int, set[int]] = {}
_voice_bans: dict[int, set[int]] = {}
_voice_hallway: dict[int, bool] = {}
_voice_panel_message: dict[int, int] = {}  # channel_id -> message_id панели
_voice_cleanup_tasks: dict[int, asyncio.Task[None]] = {}


def _embed_thumbnail_url() -> str | None:
    u = os.getenv("EMBED_THUMBNAIL_URL", "").strip()
    return u or None


def _load_settings_file() -> dict[str, Any]:
    global _settings_file_cache, _settings_file_mtime
    if not SETTINGS_PATH.is_file():
        _settings_file_cache = None
        _settings_file_mtime = None
        return {}
    try:
        mtime = SETTINGS_PATH.stat().st_mtime
        if (
            _settings_file_cache is not None
            and _settings_file_mtime is not None
            and mtime == _settings_file_mtime
        ):
            return _settings_file_cache
        raw = SETTINGS_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        out = data if isinstance(data, dict) else {}
        _settings_file_cache = out
        _settings_file_mtime = mtime
        return out
    except (OSError, json.JSONDecodeError):
        log.warning("Не удалось прочитать %s", SETTINGS_PATH)
        return {}


def _save_settings_file(data: dict[str, Any]) -> None:
    global _settings_file_cache, _settings_file_mtime
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _settings_file_cache = data
    try:
        _settings_file_mtime = SETTINGS_PATH.stat().st_mtime
    except OSError:
        _settings_file_mtime = None


def get_guild_app_settings(guild_id: int) -> tuple[bool, bool]:
    """(rp_enabled, vzp_enabled), по умолчанию оба True."""
    data = _load_settings_file()
    g = data.get(str(guild_id))
    if not isinstance(g, dict):
        return True, True
    rp = g.get("rp", True)
    vzp = g.get("vzp", True)
    return bool(rp), bool(vzp)


def set_guild_rp_enabled(guild_id: int, enabled: bool) -> None:
    data = _load_settings_file()
    g = data.setdefault(str(guild_id), {})
    if not isinstance(g, dict):
        g = {}
        data[str(guild_id)] = g
    g["rp"] = enabled
    _save_settings_file(data)


def set_guild_vzp_enabled(guild_id: int, enabled: bool) -> None:
    data = _load_settings_file()
    g = data.setdefault(str(guild_id), {})
    if not isinstance(g, dict):
        g = {}
        data[str(guild_id)] = g
    g["vzp"] = enabled
    _save_settings_file(data)


def get_ticket_view_role_ids(guild_id: int) -> list[int]:
    """Роли с доступом к тикет-каналам. Если ключ не задан — STAFF_ROLE_IDS из .env."""
    data = _load_settings_file()
    g = data.get(str(guild_id))
    if isinstance(g, dict) and "ticket_role_ids" in g:
        raw = g.get("ticket_role_ids")
        if isinstance(raw, list):
            return [int(x) for x in raw if str(x).isdigit()]
    return _staff_role_ids()


def get_moderation_role_ids(guild_id: int) -> list[int]:
    """Роли с доступом к /модерация заявок. Если ключ не задан — STAFF_ROLE_IDS из .env."""
    data = _load_settings_file()
    g = data.get(str(guild_id))
    if isinstance(g, dict) and "moderation_role_ids" in g:
        raw = g.get("moderation_role_ids")
        if isinstance(raw, list):
            return [int(x) for x in raw if str(x).isdigit()]
    return _staff_role_ids()


def _set_guild_role_list(guild_id: int, key: str, ids: list[int]) -> None:
    data = _load_settings_file()
    g = data.setdefault(str(guild_id), {})
    if not isinstance(g, dict):
        g = {}
        data[str(guild_id)] = g
    g[key] = sorted(set(ids))
    _save_settings_file(data)


def add_ticket_view_role(guild_id: int, role_id: int) -> None:
    cur = list(dict.fromkeys(get_ticket_view_role_ids(guild_id)))
    if role_id not in cur:
        cur.append(role_id)
    _set_guild_role_list(guild_id, "ticket_role_ids", cur)


def remove_ticket_view_role(guild_id: int, role_id: int) -> None:
    cur = [r for r in get_ticket_view_role_ids(guild_id) if r != role_id]
    _set_guild_role_list(guild_id, "ticket_role_ids", cur)


def add_moderation_role(guild_id: int, role_id: int) -> None:
    cur = list(dict.fromkeys(get_moderation_role_ids(guild_id)))
    if role_id not in cur:
        cur.append(role_id)
    _set_guild_role_list(guild_id, "moderation_role_ids", cur)


def remove_moderation_role(guild_id: int, role_id: int) -> None:
    cur = [r for r in get_moderation_role_ids(guild_id) if r != role_id]
    _set_guild_role_list(guild_id, "moderation_role_ids", cur)


def _ticket_channel_name(app_id: int) -> str:
    return f"ticket-{app_id:04d}"


def _member_has_staff_role(member: discord.Member, role_ids: Iterable[int]) -> bool:
    rid = {r.id for r in member.roles}
    return any(r in rid for r in role_ids)


async def _resolve_guild_member(interaction: discord.Interaction) -> discord.Member | None:
    """Участник из кэша или через API (нужно, если нет Server Members Intent / не в кэше)."""
    guild = interaction.guild
    if not guild:
        return None
    m = getattr(interaction, "member", None)
    if isinstance(m, discord.Member):
        return m
    uid = interaction.user.id
    m = guild.get_member(uid)
    if m is not None:
        return m
    try:
        return await guild.fetch_member(uid)
    except discord.NotFound:
        return None
    except discord.HTTPException:
        log.warning("fetch_member не удался для user_id=%s", uid, exc_info=True)
        return None


_MSG_ADMIN_ONLY = (
    "Эту команду могут использовать только участники с правом "
    "**Администратор** на сервере (или владелец сервера)."
)


async def _has_guild_administrator(interaction: discord.Interaction) -> bool:
    """Владелец сервера или включённое право Administrator у участника."""
    if not interaction.guild:
        return False
    if interaction.user.id == interaction.guild.owner_id:
        return True
    member = await _resolve_guild_member(interaction)
    if isinstance(member, discord.Member) and member.guild_permissions.administrator:
        return True
    return False


async def _can_moderate_tickets(interaction: discord.Interaction) -> bool:
    """Админ или роль модерации заявок или роль доступа к тикетам (принять/отказать)."""
    if not interaction.guild:
        return False
    perms = getattr(interaction, "permissions", None)
    if perms and perms.administrator:
        return True
    ticket_roles = get_ticket_view_role_ids(interaction.guild.id)
    mod_roles = get_moderation_role_ids(interaction.guild.id)
    combined = list(dict.fromkeys(ticket_roles + mod_roles))
    if not combined:
        return False
    member = await _resolve_guild_member(interaction)
    if not isinstance(member, discord.Member):
        return False
    return _member_has_staff_role(member, combined)


async def _create_ticket_channel(
    guild: discord.Guild,
    applicant: discord.Member,
    *,
    app_id: int,
    topic: str,
) -> discord.TextChannel:
    staff_ids = get_ticket_view_role_ids(guild.id)
    category_id = _ticket_category_id()
    category = guild.get_channel(category_id) if category_id else None
    if category is not None and not isinstance(category, discord.CategoryChannel):
        category = None

    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
        ),
        applicant: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True,
        ),
    }
    for rid in staff_ids:
        role = guild.get_role(rid)
        if role:
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_messages=True,
            )

    return await guild.create_text_channel(
        name=_ticket_channel_name(app_id),
        category=category,
        overwrites=overwrites,
        topic=topic[:512] if topic else None,
        reason=f"Заявка от {applicant} ({topic})",
    )


async def _try_refresh_application_panel(panel_message: discord.Message | None) -> None:
    """Сбрасывает выпадающий список: Discord не шлёт повторный клик по тому же пункту."""
    if panel_message is None:
        return
    try:
        await panel_message.edit(view=ApplicationPanelView())
    except (discord.HTTPException, discord.Forbidden):
        log.debug("Не удалось обновить панель заявок", exc_info=True)


def _schedule_application_panel_refresh(panel_message: discord.Message | None) -> None:
    """Обновление панели после send_modal — не блокирует event loop до ответа Discord."""

    async def _run() -> None:
        try:
            await _try_refresh_application_panel(panel_message)
        except Exception:
            log.exception("Ошибка отложенного обновления панели заявок")

    try:
        asyncio.get_running_loop().create_task(_run())
    except RuntimeError:
        log.debug("Нет event loop для отложенного обновления панели заявок")


class RPApplicationModal(discord.ui.Modal, title=f"Заявка РП — {BRAND}"):
    age = discord.ui.TextInput(
        label="Ваш ник | имя | возраст*",
        placeholder="Пример: Sasha | Саша | 20",
        required=True,
        max_length=50,
        style=discord.TextStyle.short,
    )
    online = discord.ui.TextInput(
        label="Семьи | Сервера | Часы*",
        placeholder="Пример: Storm | Davis | 2000",
        required=True,
        max_length=100,
        style=discord.TextStyle.short,
    )
    families = discord.ui.TextInput(
        label="Ваш часовой пояс*",
        placeholder="Пример: -1 от МСК",
        required=True,
        max_length=400,
        style=discord.TextStyle.paragraph,
    )
    source = discord.ui.TextInput(
        label=f"Откуда узнали о {BRAND} *",
        placeholder="Пример: От друга | Из рекламы",
        required=True,
        max_length=2000,
        style=discord.TextStyle.paragraph,
    )
    clip = discord.ui.TextInput(
        label="Почему выбрали именно нас*",
        placeholder="Пример: Дружный колектив",
        required=True,
        max_length=2000,
        style=discord.TextStyle.paragraph,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await _handle_application_submit(
            interaction,
            kind="РП",
            fields={
                "Ник | Имя | Возраст": str(self.age.value),
                "Семьи | Сервера | Часы": str(self.online.value),
                "Часовой пояс": str(self.families.value),
                f"Откуда узнали ({BRAND})": str(self.source.value),
                "Почему выбрали именно нас": str(self.clip.value),
            },
        )


class VZPApplicationModal(discord.ui.Modal, title=f"Форма заявки VZP — {BRAND}"):
    # У Discord label поля макс. 45 символов (иначе 50035).
    age = discord.ui.TextInput(
        label="Хорошее понимание колла? *",
        placeholder="Пример: Да/Нет",
        required=True,
        max_length=50,
        style=discord.TextStyle.short,
    )
    online = discord.ui.TextInput(
        label="Прайм-тайм *",
        placeholder="Пример: 12–24",
        required=True,
        max_length=100,
        style=discord.TextStyle.short,
    )
    families = discord.ui.TextInput(
        label="Суточный онлайн от 4 часов? *",
        placeholder="Пример: Да/Нет",
        required=True,
        max_length=400,
        style=discord.TextStyle.short,
    )
    proof = discord.ui.TextInput(
        label="Откат DM, арена (10 чел.) *",
        placeholder="Ссылка на YouTube",
        required=True,
        max_length=2000,
        style=discord.TextStyle.paragraph,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await _handle_application_submit(
            interaction,
            kind="VZP",
            fields={
                "Хорошее понимание колла": str(self.age.value),
                "Прайм-тайм": str(self.online.value),
                "Суточный онлайн от 4 часов": str(self.families.value),
                "Откат с ВЗП/DM": str(self.proof.value),
            },
        )


async def _handle_application_submit(
    interaction: discord.Interaction,
    *,
    kind: str,
    fields: dict[str, str],
) -> None:
    if not interaction.guild:
        await interaction.response.send_message(
            "Заявки принимаются только на сервере.", ephemeral=True
        )
        return

    member = await _resolve_guild_member(interaction)
    if not isinstance(member, discord.Member):
        await interaction.response.send_message(
            "Не удалось определить участника. Попробуйте снова или обратитесь к админу.",
            ephemeral=True,
        )
        return

    staff_ids = _staff_role_ids()
    if not staff_ids:
        await interaction.response.send_message(
            "Администратор не настроил STAFF_ROLE_IDS в .env — тикет не создан.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)

    app_id = _insert_application(member.id, kind, interaction.guild.id)
    topic = f"Заявка #{app_id:04d} ({kind}) — {member}"
    try:
        channel = await _create_ticket_channel(
            interaction.guild,
            member,
            app_id=app_id,
            topic=topic,
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "Нет прав на создание канала. Выдайте боту «Управление каналами».",
            ephemeral=True,
        )
        return
    except Exception as e:  # noqa: BLE001
        log.exception("create channel failed")
        await interaction.followup.send(
            f"Ошибка при создании канала: {e}", ephemeral=True
        )
        return

    lines = [f"**Заявка #{app_id:04d} ({kind})** от {member.mention}", ""]
    for k, v in fields.items():
        lines.append(f"**{k}**\n{v}")

    ticket_embed = discord.Embed(
        title="Анкета",
        description="\n".join(lines),
        color=discord.Color.dark_theme(),
    )
    ticket_embed.set_footer(text=f"ID пользователя: {member.id}")

    await channel.send(
        content=member.mention,
        embed=ticket_embed,
        view=TicketModerationView(),
    )
    _bind_ticket_channel(app_id, channel.id)

    await interaction.followup.send(
        f"Ваш тикет подан:\n{channel.jump_url}",
        ephemeral=True,
    )


def _applicant_id_from_ticket_embed(message: discord.Message) -> int | None:
    if not message.embeds:
        return None
    text = (message.embeds[0].footer and message.embeds[0].footer.text) or ""
    prefix = "ID пользователя:"
    if prefix not in text:
        return None
    tail = text.split(prefix, 1)[1].strip()
    return int(tail) if tail.isdigit() else None


class RejectReasonModal(discord.ui.Modal, title="Отказ заявки"):
    reason = discord.ui.TextInput(
        label="Причина отказа",
        placeholder="Кратко укажите причину…",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000,
    )

    def __init__(self, ticket_message: discord.Message) -> None:
        super().__init__()
        self._ticket_message = ticket_message

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await _can_moderate_tickets(interaction):
            await interaction.response.send_message(
                "Недостаточно прав для отказа по заявке.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        ch = self._ticket_message.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.followup.send("Некорректный канал тикета.", ephemeral=True)
            return
        uid = _applicant_id_from_ticket_embed(self._ticket_message)
        mention = f"<@{uid}>" if uid else "Участник"
        reason_text = str(self.reason.value).strip()
        mod = interaction.user.mention
        try:
            await ch.send(
                f"{mention}\n**Отказ по заявке**\n{reason_text}\n— модератор {mod}"
            )
        except (discord.HTTPException, discord.Forbidden):
            log.warning("Не удалось отправить сообщение об отказе", exc_info=True)
        _close_ticket_in_db(
            ch.id,
            close_kind="reject",
            close_note=reason_text[:1900] if reason_text else None,
        )
        # followup нужно отправить до удаления канала — иначе Discord: 10003 Unknown Channel
        await interaction.followup.send(
            "Тикет закрыт, причина отправлена в канал.",
            ephemeral=True,
        )
        try:
            await ch.delete(reason=f"Отказ заявки: {interaction.user}")
        except (discord.HTTPException, discord.Forbidden):
            log.warning("Не удалось удалить тикет после отказа", exc_info=True)
            await interaction.followup.send(
                "Не удалось удалить канал тикета (проверьте права бота).",
                ephemeral=True,
            )


class TicketAcceptChooseView(discord.ui.View):
    """Эфемерный выбор: принять в Storm или VZP (роли из .env)."""

    def __init__(self, ticket_message: discord.Message) -> None:
        super().__init__(timeout=300)
        self.ticket_message = ticket_message

    async def _apply_branch(self, interaction: discord.Interaction, branch: str) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not await _can_moderate_tickets(interaction):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        gid = interaction.guild.id
        rid = (
            get_guild_accept_role_rp_id(gid)
            if branch == "storm"
            else get_guild_accept_role_vzp_id(gid)
        )
        if rid is None:
            hint = (
                "`/роли принятия_рп` или `ACCEPT_ROLE_STORM_ID` в .env"
                if branch == "storm"
                else "`/роли принятия_взп` или `ACCEPT_ROLE_VZP_ID` в .env"
            )
            await interaction.response.send_message(
                f"Роль не настроена: задай через {hint}.",
                ephemeral=True,
            )
            return
        ch = self.ticket_message.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Неверный канал тикета.", ephemeral=True)
            return
        uid = _applicant_id_from_ticket_embed(self.ticket_message)
        if uid is None:
            await interaction.response.send_message(
                "Не найден ID заявителя в анкете.", ephemeral=True
            )
            return
        role = interaction.guild.get_role(rid)
        if role is None:
            await interaction.response.send_message("Роль не найдена на сервере.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        member = interaction.guild.get_member(uid)
        if member is None:
            try:
                member = await interaction.guild.fetch_member(uid)
            except discord.NotFound:
                await interaction.followup.send(
                    "Участник не на сервере — роль не выдана.", ephemeral=True
                )
                return
            except discord.HTTPException:
                await interaction.followup.send(
                    "Не удалось получить участника.", ephemeral=True
                )
                return
        try:
            await member.add_roles(
                role,
                reason=f"Заявка принята ({branch}) — {interaction.user}",
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "Нет прав выдать роль (поставьте роль бота выше выдаваемой).",
                ephemeral=True,
            )
            return

        label = "Storm" if branch == "storm" else "VZP"
        _close_ticket_in_db(
            ch.id,
            close_kind="accept_storm" if branch == "storm" else "accept_vzp",
            close_note=f"роль {role.id}",
        )
        await interaction.followup.send(
            f"Выдана роль {role.mention} (**{label}**). Закрываю тикет…",
            ephemeral=True,
        )
        try:
            await ch.delete(reason=f"Заявка принята в {label} — {interaction.user}")
        except (discord.HTTPException, discord.Forbidden):
            log.warning("Не удалось удалить тикет после принятия", exc_info=True)
            await interaction.followup.send(
                "Не удалось удалить канал тикета (проверьте права бота).",
                ephemeral=True,
            )

    @discord.ui.button(label="Принять в Storm", style=discord.ButtonStyle.success, row=0)
    async def btn_storm(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._apply_branch(interaction, "storm")

    @discord.ui.button(label="Принять в VZP", style=discord.ButtonStyle.success, row=0)
    async def btn_vzp(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._apply_branch(interaction, "vzp")


class TicketModerationView(discord.ui.View):
    """Кнопки под анкетой: принять (ветка Storm/VZP) / отказать (модалка с причиной)."""

    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Принять",
        style=discord.ButtonStyle.success,
        custom_id="storm:ticket_accept",
    )
    async def accept_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not await _can_moderate_tickets(interaction):
            await interaction.response.send_message(
                "Нужны права: роль доступа к тикетам или модерации заявок.",
                ephemeral=True,
            )
            return
        if not interaction.message:
            await interaction.response.send_message("Сообщение анкеты не найдено.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Куда принять заявку? Будет выдана соответствующая роль.",
            view=TicketAcceptChooseView(interaction.message),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Отказать",
        style=discord.ButtonStyle.danger,
        custom_id="storm:ticket_reject",
    )
    async def reject_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            return
        if not await _can_moderate_tickets(interaction):
            await interaction.response.send_message(
                "Нужны права: роль доступа к тикетам или модерации заявок.",
                ephemeral=True,
            )
            return
        if not interaction.message:
            await interaction.response.send_message("Сообщение анкеты не найдено.", ephemeral=True)
            return
        await interaction.response.send_modal(RejectReasonModal(interaction.message))


def build_moderation_embed(guild_id: int) -> discord.Embed:
    rp, vzp = get_guild_app_settings(guild_id)
    embed = discord.Embed(
        title="Приём заявок",
        description="Включите или выключите нужные направления. Кнопки ниже обновляют это сообщение.",
        color=discord.Color.dark_theme(),
    )
    embed.add_field(name="РП", value="**Вкл**" if rp else "**Выкл**", inline=True)
    embed.add_field(name="VZP (ВЗП)", value="**Вкл**" if vzp else "**Выкл**", inline=True)
    return embed


class ModerationView(discord.ui.View):
    def __init__(self, guild_id: int) -> None:
        super().__init__(timeout=300)
        self.guild_id = guild_id
        rp, vzp = get_guild_app_settings(guild_id)

        rp_on = discord.ui.Button(
            label="РП · Вкл",
            style=discord.ButtonStyle.success,
            row=0,
            disabled=rp,
        )
        rp_off = discord.ui.Button(
            label="РП · Выкл",
            style=discord.ButtonStyle.danger,
            row=0,
            disabled=not rp,
        )
        vzp_on = discord.ui.Button(
            label="VZP · Вкл",
            style=discord.ButtonStyle.success,
            row=1,
            disabled=vzp,
        )
        vzp_off = discord.ui.Button(
            label="VZP · Выкл",
            style=discord.ButtonStyle.danger,
            row=1,
            disabled=not vzp,
        )

        async def on_rp_on(interaction: discord.Interaction) -> None:
            await self._apply(interaction, lambda: set_guild_rp_enabled(self.guild_id, True))

        async def on_rp_off(interaction: discord.Interaction) -> None:
            await self._apply(interaction, lambda: set_guild_rp_enabled(self.guild_id, False))

        async def on_vzp_on(interaction: discord.Interaction) -> None:
            await self._apply(interaction, lambda: set_guild_vzp_enabled(self.guild_id, True))

        async def on_vzp_off(interaction: discord.Interaction) -> None:
            await self._apply(interaction, lambda: set_guild_vzp_enabled(self.guild_id, False))

        rp_on.callback = on_rp_on
        rp_off.callback = on_rp_off
        vzp_on.callback = on_vzp_on
        vzp_off.callback = on_vzp_off

        self.add_item(rp_on)
        self.add_item(rp_off)
        self.add_item(vzp_on)
        self.add_item(vzp_off)

    async def _apply(self, interaction: discord.Interaction, fn: Callable[[], None]) -> None:
        if not interaction.guild or interaction.guild.id != self.guild_id:
            await interaction.response.send_message("Неверный сервер.", ephemeral=True)
            return
        if not await _has_guild_administrator(interaction):
            await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
            return
        fn()
        await interaction.response.edit_message(
            embed=build_moderation_embed(self.guild_id),
            view=ModerationView(self.guild_id),
        )


class ApplicationTypeSelect(discord.ui.Select):
    """Выпадающий список заявок (как на референс-скрине: плейсхолдер + пункты с описанием)."""

    def __init__(self) -> None:
        options = [
            discord.SelectOption(
                label="Заявка-РП",
                description="Возраст, онлайн, семьи, откуда узнали, откат DM",
                emoji="\U0001f320",
                value="rp",
            ),
            discord.SelectOption(
                label="Заявка-VZP",
                description="Возраст, онлайн, семьи, откат с ВЗП/DM",
                emoji="\U0001fa90",
                value="vzp",
            ),
        ]
        super().__init__(
            custom_id="storm:application_select",
            placeholder="РП или VZP",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        panel_msg = interaction.message
        if not interaction.guild:
            await interaction.response.send_message("Только на сервере.", ephemeral=True)
            await _try_refresh_application_panel(panel_msg)
            return
        val = self.values[0]
        rp_on, vzp_on = get_guild_app_settings(interaction.guild.id)
        if val == "rp":
            if not rp_on:
                await interaction.response.send_message(
                    "Приём заявок **РП** сейчас **выключен** модерацией.",
                    ephemeral=True,
                )
                await _try_refresh_application_panel(panel_msg)
                return
            try:
                await interaction.response.send_modal(RPApplicationModal())
            except discord.NotFound:
                log.warning(
                    "Модалка РП: взаимодействие устарело (10062). "
                    "Повторите выбор или проверьте нагрузку ПК/сети."
                )
                return
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 40060:
                    log.debug(
                        "Модалка РП: interaction уже подтвержден (40060), "
                        "дубликат callback проигнорирован."
                    )
                    return
                raise
            _schedule_application_panel_refresh(panel_msg)
            return
        if val == "vzp":
            if not vzp_on:
                await interaction.response.send_message(
                    "Приём заявок **VZP** сейчас **выключен** модерацией.",
                    ephemeral=True,
                )
                await _try_refresh_application_panel(panel_msg)
                return
            try:
                await interaction.response.send_modal(VZPApplicationModal())
            except discord.NotFound:
                log.warning(
                    "Модалка VZP: взаимодействие устарело (10062). "
                    "Повторите выбор или проверьте нагрузку ПК/сети."
                )
                return
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 40060:
                    log.debug(
                        "Модалка VZP: interaction уже подтвержден (40060), "
                        "дубликат callback проигнорирован."
                    )
                    return
                raise
            _schedule_application_panel_refresh(panel_msg)


class ApplicationPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        self.add_item(ApplicationTypeSelect())


def build_panel_embed() -> discord.Embed:
    """Миниатюра: локальный PNG (attachment://) рядом с bot.py или EMBED_THUMBNAIL_URL в .env."""
    embed = discord.Embed(
        title="Перед подачей заявки ознакомьтесь с условиями:",
        color=discord.Color.dark_theme(),
    )
    embed.description = (
        "> Для вступления в семью нужен 5 уровень персонажа.\n"
        "> Возможность поменять фамилию на Storm.\n\n"
        "**Подать заявку:** выберите тип в меню ниже."
    )
    author_url = os.getenv("EMBED_AUTHOR_ICON_URL", "").strip() or None
    thumb_url = _embed_thumbnail_url()
    local = _panel_thumbnail_path()
    if local.is_file():
        att = f"attachment://{local.name}"
        embed.set_thumbnail(url=att)
        embed.set_author(name=BRAND, icon_url=author_url or att)
    else:
        embed.set_author(name=BRAND, icon_url=author_url)
        if thumb_url:
            embed.set_thumbnail(url=thumb_url)
    return embed


def _panel_thumbnail_file() -> discord.File | None:
    """Файл для первой отправки панели; имя совпадает с attachment:// в embed."""
    path = _panel_thumbnail_path()
    if not path.is_file():
        return None
    return discord.File(path, filename=path.name)


def _sanitize_voice_channel_name(name: str) -> str:
    cleaned = "".join(c for c in name if c not in '\\/:*?"<>|`')[:100]
    return cleaned.strip() or "Голосовая комната"


def _build_private_voice_overwrites(
    guild: discord.Guild,
    category: discord.CategoryChannel | None,
    owner: discord.Member,
) -> dict[Any, discord.PermissionOverwrite]:
    """База из прав категории + скрытый для @everyone приватный войс владельца."""
    o: dict[Any, discord.PermissionOverwrite] = {}
    if category is not None:
        o.update(dict(category.overwrites))
    o[guild.default_role] = discord.PermissionOverwrite(
        view_channel=False, connect=False, speak=False
    )
    o[guild.me] = discord.PermissionOverwrite(
        view_channel=True,
        connect=True,
        manage_channels=True,
        move_members=True,
    )
    o[owner] = discord.PermissionOverwrite(
        view_channel=True,
        connect=True,
        speak=True,
        manage_channels=True,
        move_members=True,
        mute_members=True,
        deafen_members=True,
        priority_speaker=True,
    )
    return o


def _voice_cleanup_state(channel_id: int) -> None:
    owner = _voice_channel_owner.pop(channel_id, None)
    if owner is not None:
        _voice_owner_channel.pop(owner, None)
    _voice_friends.pop(channel_id, None)
    _voice_bans.pop(channel_id, None)
    _voice_hallway.pop(channel_id, None)
    _voice_panel_message.pop(channel_id, None)
    t = _voice_cleanup_tasks.pop(channel_id, None)
    if t and not t.done():
        t.cancel()


def _member_or_snowflake(
    guild: discord.Guild, user_id: int
) -> discord.Member | discord.Object:
    """Без Members Intent кэш часто пуст — тогда используем Object(id) для overwrites."""
    m = guild.get_member(user_id)
    return m if m is not None else discord.Object(id=user_id)


async def _apply_voice_overwrites(
    channel: discord.VoiceChannel,
    *,
    owner_id: int,
    friends: set[int],
    bans: set[int],
    hallway: bool,
) -> None:
    guild = channel.guild
    overwrites: dict[Any, discord.PermissionOverwrite] = {}
    if channel.category is not None:
        overwrites.update(dict(channel.category.overwrites))
    overwrites[guild.default_role] = discord.PermissionOverwrite(
        view_channel=False, connect=False, speak=False
    )
    overwrites[guild.me] = discord.PermissionOverwrite(
        view_channel=True,
        connect=True,
        manage_channels=True,
        move_members=True,
    )
    owner_key = _member_or_snowflake(guild, owner_id)
    overwrites[owner_key] = discord.PermissionOverwrite(
        view_channel=True,
        connect=True,
        speak=True,
        manage_channels=True,
        move_members=True,
        mute_members=True,
        deafen_members=True,
        priority_speaker=True,
    )
    if not hallway:
        for uid in friends:
            if uid in bans or uid == owner_id:
                continue
            key = _member_or_snowflake(guild, uid)
            overwrites[key] = discord.PermissionOverwrite(
                view_channel=True,
                connect=True,
                speak=True,
            )
    for uid in bans:
        key = _member_or_snowflake(guild, uid)
        overwrites[key] = discord.PermissionOverwrite(
            view_channel=True,
            connect=False,
            speak=False,
        )
    await channel.edit(overwrites=overwrites, reason="Storm: настройки приватного войса")


def _default_voice_channel_name(member: discord.Member) -> str:
    return _sanitize_voice_channel_name(f"{member.display_name}")


def build_voice_control_embed() -> discord.Embed:
    return discord.Embed(
        title="Панель управления",
        description=(
            "Кнопки ниже — настройки **твоей** голосовой комнаты (чат справа у этого войса). "
            "**Название, Лимит, Регион** — сразу меняют войс; **Кикнуть** — из списка; "
            "**Прихожая** — закрыть вход для всех, кроме тебя; **Забрать** — передать владельца из списка; "
            "**Друзья / Баны** — выбор из списка."
        ),
        color=discord.Color.dark_teal(),
    )


async def _get_tracked_voice(
    interaction: discord.Interaction, channel_id: int
) -> discord.VoiceChannel | None:
    ch = interaction.client.get_channel(channel_id)
    return ch if isinstance(ch, discord.VoiceChannel) else None


def _cancel_voice_cleanup(channel_id: int) -> None:
    t = _voice_cleanup_tasks.pop(channel_id, None)
    if t and not t.done():
        t.cancel()


async def _schedule_empty_voice_cleanup(guild: discord.Guild, channel: discord.VoiceChannel) -> None:
    if channel.id not in _voice_channel_owner:
        return
    _cancel_voice_cleanup(channel.id)
    cid = channel.id

    async def _work() -> None:
        try:
            await asyncio.sleep(3)
            ch = guild.get_channel(cid)
            if not ch or not isinstance(ch, discord.VoiceChannel):
                _voice_cleanup_tasks.pop(cid, None)
                return
            humans = [m for m in ch.members if not m.bot]
            if humans:
                _voice_cleanup_tasks.pop(cid, None)
                return
            try:
                await ch.delete(reason="Storm: комната пустая")
            except (discord.HTTPException, discord.Forbidden):
                log.warning("Не удалось удалить пустой войс cid=%s", cid, exc_info=True)
        except Exception:
            log.exception("Ошибка фоновой очистки пустого войса cid=%s", cid)
        finally:
            _voice_cleanup_state(cid)
            _voice_cleanup_tasks.pop(cid, None)

    _voice_cleanup_tasks[cid] = asyncio.create_task(_work())


async def _handle_voice_hub_join(bot: discord.Client, member: discord.Member) -> None:
    hub_id = _voice_hub_channel_id()
    if not hub_id or not member.guild:
        return

    existing_id = _voice_owner_channel.get(member.id)
    if existing_id:
        existing = member.guild.get_channel(existing_id)
        if isinstance(existing, discord.VoiceChannel):
            _cancel_voice_cleanup(existing.id)
            try:
                await member.move_to(existing)
            except (discord.HTTPException, discord.Forbidden):
                log.warning("move_to существующий войс не удался", exc_info=True)
            return

    hub = member.guild.get_channel(hub_id)
    if not isinstance(hub, discord.VoiceChannel):
        log.warning("VOICE_HUB_CHANNEL_ID не указывает на голосовой канал")
        return

    cat_id = _voice_create_category_id()
    category: discord.CategoryChannel | None = None
    if cat_id:
        c = member.guild.get_channel(cat_id)
        if isinstance(c, discord.CategoryChannel):
            category = c
    if category is None and hub.category:
        category = hub.category

    name = _default_voice_channel_name(member)
    overwrites = _build_private_voice_overwrites(member.guild, category, member)
    try:
        channel = await member.guild.create_voice_channel(
            name=name,
            category=category,
            overwrites=overwrites,
            reason=f"Storm join-to-create — {member}",
        )
    except (discord.HTTPException, discord.Forbidden):
        log.exception("Не удалось создать голосовой канал")
        return

    _voice_owner_channel[member.id] = channel.id
    _voice_channel_owner[channel.id] = member.id
    _voice_friends[channel.id] = set()
    _voice_bans[channel.id] = set()
    _voice_hallway[channel.id] = False

    try:
        await member.move_to(channel)
    except (discord.HTTPException, discord.Forbidden):
        log.exception("move_to в новый войс не удался — удаляем канал")
        try:
            await channel.delete(reason="Storm: не удалось перенести участника")
        except (discord.HTTPException, discord.Forbidden):
            pass
        _voice_cleanup_state(channel.id)
        return

    embed = build_voice_control_embed()
    view = VoiceControlPanelView(owner_id=member.id, channel_id=channel.id)
    try:
        msg = await channel.send(embed=embed, view=view)
        _voice_panel_message[channel.id] = msg.id
    except (discord.HTTPException, discord.Forbidden):
        log.warning("Не удалось отправить панель в войс", exc_info=True)


async def _on_voice_state_for_jtc(
    bot: discord.Client, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
) -> None:
    try:
        if member.bot:
            return
        guild = member.guild
        # Только уход из канала (не mute/deafen в том же войсе)
        if before.channel is not None and after.channel != before.channel:
            await _schedule_empty_voice_cleanup(guild, before.channel)

        hub_id = _voice_hub_channel_id()
        if not hub_id:
            return
        if after.channel and after.channel.id == hub_id:
            if before.channel is not None and before.channel.id == hub_id:
                return
            await _handle_voice_hub_join(bot, member)
    except Exception:
        log.exception("Ошибка join-to-create / voice_state (member=%s)", getattr(member, "id", "?"))


class VoiceRenameModal(discord.ui.Modal, title="Название комнаты"):
    name_input = discord.ui.TextInput(
        label="Новое название",
        placeholder="Отображается в списке каналов",
        max_length=100,
        required=True,
    )

    def __init__(self, channel_id: int) -> None:
        super().__init__()
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        ch = await _get_tracked_voice(interaction, self.channel_id)
        if not ch:
            await interaction.response.send_message("Канал не найден.", ephemeral=True)
            return
        oid = _voice_channel_owner.get(ch.id)
        if interaction.user.id != oid:
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        new_name = _sanitize_voice_channel_name(str(self.name_input.value))
        try:
            await ch.edit(name=new_name, reason=f"Storm: переименование — {interaction.user}")
        except (discord.HTTPException, discord.Forbidden):
            await interaction.response.send_message("Не удалось сменить название.", ephemeral=True)
            return
        await interaction.response.send_message("Готово.", ephemeral=True)


class VoiceLimitModal(discord.ui.Modal, title="Лимит пользователей"):
    limit_input = discord.ui.TextInput(
        label="Слоты (0 — без лимита, 1–99)",
        placeholder="Например: 5",
        max_length=2,
        required=True,
    )

    def __init__(self, channel_id: int) -> None:
        super().__init__()
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        ch = await _get_tracked_voice(interaction, self.channel_id)
        if not ch:
            await interaction.response.send_message("Канал не найден.", ephemeral=True)
            return
        oid = _voice_channel_owner.get(ch.id)
        if interaction.user.id != oid:
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        raw = str(self.limit_input.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("Введите число от 0 до 99.", ephemeral=True)
            return
        n = int(raw)
        if n > 99:
            await interaction.response.send_message("Максимум 99.", ephemeral=True)
            return
        try:
            await ch.edit(user_limit=n, reason=f"Storm: лимит — {interaction.user}")
        except (discord.HTTPException, discord.Forbidden):
            await interaction.response.send_message("Не удалось установить лимит.", ephemeral=True)
            return
        await interaction.response.send_message("Готово.", ephemeral=True)


class VoiceRegionModal(discord.ui.Modal, title="Регион RTC"):
    region_input = discord.ui.TextInput(
        label="Регион или пусто = авто",
        placeholder="europe, russia, us-west … оставь пусто для авто",
        max_length=50,
        required=False,
        style=discord.TextStyle.short,
    )

    def __init__(self, channel_id: int) -> None:
        super().__init__()
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        ch = await _get_tracked_voice(interaction, self.channel_id)
        if not ch:
            await interaction.response.send_message("Канал не найден.", ephemeral=True)
            return
        oid = _voice_channel_owner.get(ch.id)
        if interaction.user.id != oid:
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        raw = str(self.region_input.value).strip()
        region: str | None = raw if raw else None
        try:
            await ch.edit(rtc_region=region, reason=f"Storm: регион — {interaction.user}")
        except (discord.HTTPException, discord.Forbidden):
            await interaction.response.send_message(
                "Не удалось сменить регион. Проверь код (например `europe`) или оставь пустым.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message("Готово.", ephemeral=True)


class KickUserSelect(discord.ui.UserSelect):
    def __init__(self, channel_id: int, owner_id: int) -> None:
        super().__init__(
            placeholder="Кого выгнать из войса",
            min_values=1,
            max_values=1,
        )
        self.channel_id = channel_id
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        ch = await _get_tracked_voice(interaction, self.channel_id)
        if not ch:
            await interaction.response.send_message("Канал не найден.", ephemeral=True)
            return
        target = self.values[0]
        if target.id == self.owner_id:
            await interaction.response.send_message("Нельзя кикнуть себя так.", ephemeral=True)
            return
        member = discord.utils.get(ch.members, id=target.id)
        if not member or member.voice is None or member.voice.channel != ch:
            await interaction.response.send_message("Пользователь не в этом войсе.", ephemeral=True)
            return
        try:
            await member.move_to(None, reason=f"Storm: кик — {interaction.user}")
        except (discord.HTTPException, discord.Forbidden):
            await interaction.response.send_message("Не удалось кикнуть.", ephemeral=True)
            return
        await interaction.response.send_message("Готово.", ephemeral=True)


class TransferUserSelect(discord.ui.UserSelect):
    def __init__(self, channel_id: int, owner_id: int) -> None:
        super().__init__(
            placeholder="Кому передать комнату",
            min_values=1,
            max_values=1,
        )
        self.channel_id = channel_id
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        ch = await _get_tracked_voice(interaction, self.channel_id)
        if not ch or not interaction.guild:
            await interaction.response.send_message("Канал не найден.", ephemeral=True)
            return
        new_owner_u = self.values[0]
        if new_owner_u.id == self.owner_id:
            await interaction.response.send_message("Выбери другого участника.", ephemeral=True)
            return
        new_m = discord.utils.get(ch.members, id=new_owner_u.id)
        if not new_m or new_m.voice is None or new_m.voice.channel != ch:
            await interaction.response.send_message(
                "Пользователь должен сидеть в этом войсе.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        friends = _voice_friends.setdefault(ch.id, set())
        bans = _voice_bans.setdefault(ch.id, set())
        hallway = _voice_hallway.get(ch.id, False)

        friends.discard(new_owner_u.id)
        if self.owner_id not in bans:
            friends.add(self.owner_id)

        _voice_channel_owner[ch.id] = new_owner_u.id
        _voice_owner_channel.pop(self.owner_id, None)
        _voice_owner_channel[new_owner_u.id] = ch.id

        await _apply_voice_overwrites(
            ch, owner_id=new_owner_u.id, friends=friends, bans=bans, hallway=hallway
        )

        mid = _voice_panel_message.get(ch.id)
        if mid:
            try:
                msg = await ch.fetch_message(mid)
                await msg.edit(
                    embed=build_voice_control_embed(),
                    view=VoiceControlPanelView(owner_id=new_owner_u.id, channel_id=ch.id),
                )
            except (discord.HTTPException, discord.NotFound, discord.Forbidden):
                log.debug("Не удалось обновить панель после передачи", exc_info=True)

        await interaction.followup.send(
            f"Владелец: {new_m.display_name}. Панель обновлена.",
            ephemeral=True,
        )


class FriendUserSelect(discord.ui.UserSelect):
    def __init__(self, channel_id: int, owner_id: int) -> None:
        super().__init__(
            placeholder="Кого добавить в друзья",
            min_values=1,
            max_values=1,
        )
        self.channel_id = channel_id
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        ch = await _get_tracked_voice(interaction, self.channel_id)
        if not ch:
            await interaction.response.send_message("Канал не найден.", ephemeral=True)
            return
        uid = self.values[0].id
        if uid == self.owner_id:
            await interaction.response.send_message("Это ты.", ephemeral=True)
            return
        friends = _voice_friends.setdefault(ch.id, set())
        bans = _voice_bans.setdefault(ch.id, set())
        friends.add(uid)
        bans.discard(uid)
        hallway = _voice_hallway.get(ch.id, False)
        await _apply_voice_overwrites(
            ch,
            owner_id=self.owner_id,
            friends=friends,
            bans=bans,
            hallway=hallway,
        )
        await interaction.response.send_message("Друг добавлен (может заходить в войс).", ephemeral=True)


class BanUserSelect(discord.ui.UserSelect):
    def __init__(self, channel_id: int, owner_id: int) -> None:
        super().__init__(
            placeholder="Кого забанить в комнате",
            min_values=1,
            max_values=1,
        )
        self.channel_id = channel_id
        self.owner_id = owner_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        ch = await _get_tracked_voice(interaction, self.channel_id)
        if not ch:
            await interaction.response.send_message("Канал не найден.", ephemeral=True)
            return
        uid = self.values[0].id
        if uid == self.owner_id:
            await interaction.response.send_message("Нельзя забанить себя.", ephemeral=True)
            return
        friends = _voice_friends.setdefault(ch.id, set())
        bans = _voice_bans.setdefault(ch.id, set())
        friends.discard(uid)
        bans.add(uid)
        hallway = _voice_hallway.get(ch.id, False)
        await _apply_voice_overwrites(
            ch,
            owner_id=self.owner_id,
            friends=friends,
            bans=bans,
            hallway=hallway,
        )
        member = discord.utils.get(ch.members, id=uid)
        if member and member.voice and member.voice.channel == ch:
            try:
                await member.move_to(None, reason=f"Storm: бан в комнате — {interaction.user}")
            except (discord.HTTPException, discord.Forbidden):
                pass
        await interaction.response.send_message("Пользователь забанен в этой комнате.", ephemeral=True)


class VoiceControlPanelView(discord.ui.View):
    """Панель в чате войса (join-to-create)."""

    def __init__(self, *, owner_id: int, channel_id: int) -> None:
        super().__init__(timeout=None)
        self.owner_id = owner_id
        self.channel_id = channel_id

    def _is_owner(self, user_id: int) -> bool:
        return user_id == self.owner_id

    @discord.ui.button(
        label="Название",
        style=discord.ButtonStyle.secondary,
        emoji="🔤",
        row=0,
    )
    async def btn_name(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        await interaction.response.send_modal(VoiceRenameModal(self.channel_id))

    @discord.ui.button(
        label="Лимит",
        style=discord.ButtonStyle.secondary,
        emoji="👥",
        row=0,
    )
    async def btn_limit(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        await interaction.response.send_modal(VoiceLimitModal(self.channel_id))

    @discord.ui.button(
        label="Регион",
        style=discord.ButtonStyle.secondary,
        emoji="🌐",
        row=0,
    )
    async def btn_region(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        await interaction.response.send_modal(VoiceRegionModal(self.channel_id))

    @discord.ui.button(
        label="Кикнуть",
        style=discord.ButtonStyle.secondary,
        emoji="📞",
        row=1,
    )
    async def btn_kick(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        view = discord.ui.View(timeout=180)
        view.add_item(KickUserSelect(self.channel_id, self.owner_id))
        await interaction.response.send_message(
            "Выбери участника для кика:",
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Гайд",
        style=discord.ButtonStyle.primary,
        emoji="ℹ️",
        row=1,
    )
    async def btn_guide(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        text = (
            "**Название / Лимит / Регион** — меняют параметры канала.\n"
            "**Кикнуть** — отключить от голоса выбранного участника.\n"
            "**Прихожая** — только ты (и бот) можешь заходить; друзья временно не пускаются.\n"
            "**Забрать** — передать права владельца тому, кто уже в этом войсе.\n"
            "**Друзья** — разрешить заход конкретным людям.\n"
            "**Баны** — запретить заход и выкинуть, если уже внутри."
        )
        await interaction.response.send_message(text, ephemeral=True)

    @discord.ui.button(
        label="Прихожая",
        style=discord.ButtonStyle.secondary,
        emoji="🕐",
        row=2,
    )
    async def btn_hallway(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        ch = await _get_tracked_voice(interaction, self.channel_id)
        if not ch:
            await interaction.response.send_message("Канал не найден.", ephemeral=True)
            return
        cur = _voice_hallway.get(ch.id, False)
        new = not cur
        _voice_hallway[ch.id] = new
        friends = _voice_friends.setdefault(ch.id, set())
        bans = _voice_bans.setdefault(ch.id, set())
        await _apply_voice_overwrites(
            ch,
            owner_id=self.owner_id,
            friends=friends,
            bans=bans,
            hallway=new,
        )
        await interaction.response.send_message(
            "Прихожая **включена**: только ты можешь заходить."
            if new
            else "Прихожая **выключена**: друзья снова могут заходить.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="Забрать",
        style=discord.ButtonStyle.secondary,
        emoji="⭐",
        row=2,
    )
    async def btn_transfer(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        view = discord.ui.View(timeout=180)
        view.add_item(TransferUserSelect(self.channel_id, self.owner_id))
        await interaction.response.send_message(
            "Кому передать владение?",
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Друзья",
        style=discord.ButtonStyle.success,
        emoji="👤",
        row=3,
    )
    async def btn_friends(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        view = discord.ui.View(timeout=180)
        view.add_item(FriendUserSelect(self.channel_id, self.owner_id))
        await interaction.response.send_message(
            "Кого добавить в друзья?",
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Баны",
        style=discord.ButtonStyle.danger,
        emoji="⚖️",
        row=3,
    )
    async def btn_bans(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._is_owner(interaction.user.id):
            await interaction.response.send_message("Это не твоя комната.", ephemeral=True)
            return
        view = discord.ui.View(timeout=180)
        view.add_item(BanUserSelect(self.channel_id, self.owner_id))
        await interaction.response.send_message(
            "Кого забанить?",
            view=view,
            ephemeral=True,
        )


intents = discord.Intents.default()
# Members Intent не включаем — бот запускается без привилегий в портале; войс/оверрайды через Object(id).
intents.voice_states = True

moderation_group = app_commands.Group(
    name="модерация",
    description="Команды модерации сервера",
)

roles_group = app_commands.Group(
    name="роли",
    description="Кто видит тикеты и кто может пользоваться модерацией заявок",
)


@moderation_group.command(
    name="заявок",
    description="Включить или выключить приём заявок РП и VZP",
)
async def moderation_zayavok(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Команда только на сервере.", ephemeral=True)
        return
    if not await _has_guild_administrator(interaction):
        await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
        return
    await interaction.response.send_message(
        embed=build_moderation_embed(interaction.guild.id),
        view=ModerationView(interaction.guild.id),
        ephemeral=True,
    )


def _format_role_mentions(guild: discord.Guild, role_ids: list[int]) -> str:
    if not role_ids:
        return "—"
    parts: list[str] = []
    for rid in role_ids:
        role = guild.get_role(rid)
        parts.append(role.mention if role else f"`{rid}`")
    return ", ".join(parts)


@roles_group.command(
    name="тикет_добавить",
    description="Добавить роль: видит тикеты и кнопки Принять/Отказать по заявкам",
)
@app_commands.describe(role="Роль")
@app_commands.default_permissions(administrator=True)
async def roles_ticket_add(interaction: discord.Interaction, role: discord.Role) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    if not await _has_guild_administrator(interaction):
        await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
        return
    if role.guild.id != interaction.guild.id:
        await interaction.response.send_message("Роль с этого сервера.", ephemeral=True)
        return
    add_ticket_view_role(interaction.guild.id, role.id)
    await interaction.response.send_message(
        f"Роль {role.mention} добавлена к **доступу к тикетам**.",
        ephemeral=True,
    )


@roles_group.command(
    name="тикет_удалить",
    description="Убрать роль из доступа к тикетам",
)
@app_commands.describe(role="Роль")
@app_commands.default_permissions(administrator=True)
async def roles_ticket_remove(interaction: discord.Interaction, role: discord.Role) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    if not await _has_guild_administrator(interaction):
        await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
        return
    if role.guild.id != interaction.guild.id:
        await interaction.response.send_message("Роль с этого сервера.", ephemeral=True)
        return
    remove_ticket_view_role(interaction.guild.id, role.id)
    await interaction.response.send_message(
        f"Роль {role.mention} убрана из **доступа к тикетам**.",
        ephemeral=True,
    )


@roles_group.command(
    name="модерация_добавить",
    description="Добавить роль: команда /модерация заявок и кнопки Принять/Отказать",
)
@app_commands.describe(role="Роль")
@app_commands.default_permissions(administrator=True)
async def roles_mod_add(interaction: discord.Interaction, role: discord.Role) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    if not await _has_guild_administrator(interaction):
        await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
        return
    if role.guild.id != interaction.guild.id:
        await interaction.response.send_message("Роль с этого сервера.", ephemeral=True)
        return
    add_moderation_role(interaction.guild.id, role.id)
    await interaction.response.send_message(
        f"Роль {role.mention} добавлена к **модерации заявок**.",
        ephemeral=True,
    )


@roles_group.command(
    name="модерация_удалить",
    description="Убрать роль из модерации заявок",
)
@app_commands.describe(role="Роль")
@app_commands.default_permissions(administrator=True)
async def roles_mod_remove(interaction: discord.Interaction, role: discord.Role) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    if not await _has_guild_administrator(interaction):
        await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
        return
    if role.guild.id != interaction.guild.id:
        await interaction.response.send_message("Роль с этого сервера.", ephemeral=True)
        return
    remove_moderation_role(interaction.guild.id, role.id)
    await interaction.response.send_message(
        f"Роль {role.mention} убрана из **модерации заявок**.",
        ephemeral=True,
    )


def _format_accept_role_line(guild: discord.Guild, role_id: int | None) -> str:
    if role_id is None:
        return "не задана"
    role = guild.get_role(role_id)
    return role.mention if role else f"`{role_id}`"


@roles_group.command(
    name="принятия_рп",
    description="Роль при принятии заявки в Storm (кнопка «Принять в Storm»)",
)
@app_commands.describe(
    role="Роль для выдачи; не указывай — покажу текущую",
    reset="Сбросить сохранённую роль (снова подхватится .env при наличии)",
)
@app_commands.default_permissions(administrator=True)
async def roles_accept_rp(
    interaction: discord.Interaction,
    role: discord.Role | None = None,
    reset: bool = False,
) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    if not await _has_guild_administrator(interaction):
        await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
        return
    gid = interaction.guild.id
    if reset:
        set_guild_accept_role_rp_id(gid, None)
        await interaction.response.send_message(
            "Сохранённая роль сброшена. Если в `.env` задан `ACCEPT_ROLE_STORM_ID`, он снова действует.",
            ephemeral=True,
        )
        return
    if role is not None:
        if role.guild.id != interaction.guild.id:
            await interaction.response.send_message("Роль с этого сервера.", ephemeral=True)
            return
        set_guild_accept_role_rp_id(gid, role.id)
        await interaction.response.send_message(
            f"При **Принять в Storm** будет выдаваться роль {role.mention}.",
            ephemeral=True,
        )
        return
    cur = get_guild_accept_role_rp_id(gid)
    data = _load_settings_file()
    g = data.get(str(gid))
    src = (
        "(из команды)"
        if isinstance(g, dict) and "accept_role_rp_id" in g
        else "(из .env `ACCEPT_ROLE_STORM_ID`, если задан)"
    )
    await interaction.response.send_message(
        f"**Роль принятия Storm/РП** {src}: {_format_accept_role_line(interaction.guild, cur)}\n"
        "Укажи параметр **роль** чтобы задать, **reset** True — сбросить.",
        ephemeral=True,
    )


@roles_group.command(
    name="принятия_взп",
    description="Роль при принятии заявки в VZP (кнопка «Принять в VZP»)",
)
@app_commands.describe(
    role="Роль для выдачи; не указывай — покажу текущую",
    reset="Сбросить сохранённую роль (снова подхватится .env при наличии)",
)
@app_commands.default_permissions(administrator=True)
async def roles_accept_vzp(
    interaction: discord.Interaction,
    role: discord.Role | None = None,
    reset: bool = False,
) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    if not await _has_guild_administrator(interaction):
        await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
        return
    gid = interaction.guild.id
    if reset:
        set_guild_accept_role_vzp_id(gid, None)
        await interaction.response.send_message(
            "Сохранённая роль сброшена. Если в `.env` задан `ACCEPT_ROLE_VZP_ID`, он снова действует.",
            ephemeral=True,
        )
        return
    if role is not None:
        if role.guild.id != interaction.guild.id:
            await interaction.response.send_message("Роль с этого сервера.", ephemeral=True)
            return
        set_guild_accept_role_vzp_id(gid, role.id)
        await interaction.response.send_message(
            f"При **Принять в VZP** будет выдаваться роль {role.mention}.",
            ephemeral=True,
        )
        return
    cur = get_guild_accept_role_vzp_id(gid)
    data = _load_settings_file()
    g = data.get(str(gid))
    src = (
        "(из команды)"
        if isinstance(g, dict) and "accept_role_vzp_id" in g
        else "(из .env `ACCEPT_ROLE_VZP_ID`, если задан)"
    )
    await interaction.response.send_message(
        f"**Роль принятия VZP** {src}: {_format_accept_role_line(interaction.guild, cur)}\n"
        "Укажи параметр **роль** чтобы задать, **reset** True — сбросить.",
        ephemeral=True,
    )


@roles_group.command(
    name="список",
    description="Показать роли для тикетов и для модерации",
)
@app_commands.default_permissions(administrator=True)
async def roles_list(interaction: discord.Interaction) -> None:
    if not interaction.guild:
        await interaction.response.send_message("Только на сервере.", ephemeral=True)
        return
    if not await _has_guild_administrator(interaction):
        await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
        return
    gid = interaction.guild.id
    t = get_ticket_view_role_ids(gid)
    m = get_moderation_role_ids(gid)
    data = _load_settings_file()
    g = data.get(str(gid))
    t_src = "из `.env` (пока не настроено через команды)" if (
        isinstance(g, dict) and "ticket_role_ids" not in g
    ) else "сохранено"
    m_src = "из `.env` (пока не настроено через команды)" if (
        isinstance(g, dict) and "moderation_role_ids" not in g
    ) else "сохранено"
    rp_acc = get_guild_accept_role_rp_id(gid)
    vzp_acc = get_guild_accept_role_vzp_id(gid)
    await interaction.response.send_message(
        "**Тикеты** "
        f"({t_src}): {_format_role_mentions(interaction.guild, t)}\n"
        "**Модерация** "
        f"({m_src}): {_format_role_mentions(interaction.guild, m)}\n"
        "**Принятие Storm/РП:** "
        f"{_format_accept_role_line(interaction.guild, rp_acc)}\n"
        "**Принятие VZP:** "
        f"{_format_accept_role_line(interaction.guild, vzp_acc)}",
        ephemeral=True,
    )


async def _safe_interaction_error_message(
    interaction: discord.Interaction, text: str
) -> None:
    """Эфемерное сообщение об ошибке; не бросает наружу."""
    try:
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)
    except (discord.HTTPException, discord.NotFound):
        log.debug("Не удалось отправить ephemeral об ошибке", exc_info=True)


class StormBot(commands.Bot):
    def __init__(self) -> None:
        super().__init__(command_prefix="!", intents=intents)
        self.tree.add_command(self._setup_panel_command())
        self.tree.add_command(moderation_group)
        self.tree.add_command(roles_group)
        self.tree.error(self._on_app_command_error)

    async def _on_app_command_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ) -> None:
        if isinstance(error, app_commands.CommandInvokeError):
            orig = error.original
            log.exception(
                "Слэш-команда %s",
                error.command.qualified_name if error.command else "?",
                exc_info=orig,
            )
            if not isinstance(
                orig, (discord.HTTPException, discord.NotFound, discord.Forbidden)
            ):
                await _safe_interaction_error_message(
                    interaction,
                    "Произошла ошибка при выполнении команды. Попробуйте позже.",
                )
            return
        log.warning("Ошибка слэш-команды: %s", error)

    async def on_error(self, event_method: str, *args: Any, **kwargs: Any) -> None:
        # format_exc до await — контекст исключения ещё действителен
        log.error(
            "Исключение в обработчике события %s:\n%s",
            event_method,
            traceback.format_exc(),
        )

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        await _on_voice_state_for_jtc(self, member, before, after)

    async def setup_hook(self) -> None:
        # Панель заявок отправляется с view в сообщении. Глобальная регистрация этого
        # же persistent-view может приводить к дублю callback (40060).
        self.add_view(TicketModerationView())
        guild_id = _guild_id()
        try:
            if guild_id:
                g = discord.Object(id=guild_id)
                self.tree.copy_global_to(guild=g)
                await self.tree.sync(guild=g)
                log.info("Слэш-команды синхронизированы для гильдии %s", guild_id)
            else:
                await self.tree.sync()
                log.info("Глобальная синхронизация слэш-команд (до ~1 часа)")
        except Exception:
            log.exception(
                "Синхронизация слэш-команд не удалась — бот продолжит работу без обновления команд"
            )

    def _setup_panel_command(self) -> app_commands.Command:
        @app_commands.default_permissions(administrator=True)
        @app_commands.command(name="панель", description="Отправить панель заявок в этот канал")
        async def panel_command(interaction: discord.Interaction) -> None:
            if not interaction.channel or not isinstance(
                interaction.channel, discord.TextChannel
            ):
                await interaction.response.send_message(
                    "Команда работает только в текстовом канале.", ephemeral=True
                )
                return
            if not await _has_guild_administrator(interaction):
                await interaction.response.send_message(_MSG_ADMIN_ONLY, ephemeral=True)
                return
            deferred_by_us = False
            try:
                await interaction.response.defer(ephemeral=True, thinking=True)
                deferred_by_us = True
            except discord.HTTPException as e:
                if getattr(e, "code", None) != 40060:
                    raise
                # Если interaction уже подтверждён (дубль/гонка), не падаем:
                # продолжим отправку панели в канал.
                log.warning(
                    "Команда /панель: interaction уже подтверждён (40060), "
                    "продолжаю отправку панели в канал."
                )
            embed = build_panel_embed()
            extra: dict[str, Any] = {}
            f = _panel_thumbnail_file()
            if f is not None:
                extra["file"] = f
            await interaction.channel.send(embed=embed, view=ApplicationPanelView(), **extra)
            if deferred_by_us:
                try:
                    await interaction.delete_original_response()
                except (discord.HTTPException, discord.NotFound):
                    pass

        return panel_command


def main() -> None:
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Задайте DISCORD_BOT_TOKEN в .env")

    bot = StormBot()

    @bot.event
    async def on_ready() -> None:
        log.info("В сети как %s (%s)", bot.user, bot.user.id if bot.user else "?")

    bot.run(token)


if __name__ == "__main__":
    main()
