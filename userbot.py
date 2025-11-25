# userbot.py
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon import functions, types, utils

@dataclass
class Settings:
    API_ID: int
    API_HASH: str
    USERBOT_SESSION: str
    MANAGED_BOT_USERNAME: str  

class ChatFactory:
    """
    Делает 3 действия:
    1) Создаёт мегагруппу (supergroup)
    2) Приглашает в неё PTB-бота
    3) Выдаёт боту админку (invite_users=True, change_info=True)
    """
    def __init__(self, client: TelegramClient):
        self._client = client

    async def aclose(self):
        await self._client.disconnect()

    async def create_chat(self, *, title: str, bot_username: str) -> int:
        if bot_username.startswith("@"):
            bot_username = bot_username[1:]

        # 1) Cоздаём мегагруппу
        create = await self._client(
            functions.channels.CreateChannelRequest(
                title=title,
                about="",
                megagroup=True,
                broadcast=False,
                forum=False,
            )
        )
        if not create.chats:
            raise RuntimeError("CreateChannel: пустой ответ chats")
        channel = create.chats[0]

        # 2) Cущности
        _full = await self._client(functions.channels.GetFullChannelRequest(channel))
        bot = await self._client.get_entity(bot_username)

        # 3) Приглашаем бота
        await self._client(functions.channels.InviteToChannelRequest(
            channel=channel,
            users=[bot]
        ))

        # 4) Повышаем бота до админа (даём права для инвайтов и смены title)
        rights = types.ChatAdminRights(
            change_info=True,     # Нужно для set_chat_title в regular_bot.py
            invite_users=True,    # Нужно для create_chat_invite_link
            post_messages=True,
            edit_messages=True,
            delete_messages=True,
            ban_users=True,
            pin_messages=True,
            add_admins=True,
            anonymous=True,
            manage_call=True,
            other=True,
        )
        await self._client(functions.channels.EditAdminRequest(
            channel=channel,
            user_id=bot,
            admin_rights=rights,
            rank=""
        ))

        # Bot API chat_id
        chat_id = utils.get_peer_id(channel)
        return chat_id


async def build_chat_factory(settings: Settings) -> ChatFactory:
    if not settings.USERBOT_SESSION:
        raise RuntimeError("USERBOT_SESSION пуст. Сгенерируйте через скрипт и положите в .env")

    client = TelegramClient(
        StringSession(settings.USERBOT_SESSION),
        settings.API_ID,
        settings.API_HASH,
    )
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Userbot session не авторизована. Пересоздайте USERBOT_SESSION.")

    return ChatFactory(client)
