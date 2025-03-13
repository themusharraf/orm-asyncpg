import asyncpg
import asyncio
from config import DATABASE_URL


class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=5)
        print("Database connection pool created!")

    async def create_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE NOT NULL,
                    full_name TEXT NOT NULL
                )
            ''')
            print("Table 'users' created successfully!")

    async def add(self, user_id: int, full_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (user_id, full_name) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING",
                user_id, full_name
            )

    async def all(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            users = await conn.fetch("SELECT user_id, full_name FROM users")
            return [dict(user) for user in users]

    async def is_exists(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            user = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)", user_id)
            return user

    async def update(self, user_id: int, new_name: str) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE users SET full_name = $1 WHERE user_id = $2", new_name, user_id
            )
            return result

    async def delete(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)
            return result

    async def close(self):
        if self.pool:
            await self.pool.close()
            print("Database connection pool closed!")


async def main():
    db = Database(DATABASE_URL)
    await db.connect()
    await db.create_table()

    await db.add(12345, "John Doe")

    users = await db.all()
    print("Users:", users)

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
