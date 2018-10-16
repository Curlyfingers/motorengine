from motorengine.base.database import BaseDatabase


class Database(BaseDatabase):
    async def ping(self):
        return await self.connection.admin.command('ping')
