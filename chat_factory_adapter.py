# chat_factory_adapter.py
class ChatFactoryAdapter:
    """
    Thin wrapper around userbot.ChatFactory.
    Exposes both create_chat(...) and legacy create_group_with_bot(...) helpers.
    """

    def __init__(self, telethon_factory, bot_username: str):
        self._factory = telethon_factory
        self._bot_username = bot_username

    async def create_chat(self, title: str) -> int:
        return await self._factory.create_chat(title=title, bot_username=self._bot_username)

    async def create_group_with_bot(self, title: str) -> int:
        return await self.create_chat(title=title)
