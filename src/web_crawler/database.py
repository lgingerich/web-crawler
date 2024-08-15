import asyncpg
import json
from utils import logger

class DatabaseManager:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.conn_params = {
            "database": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port,
        }
        self.pool = None

    async def async_create_pool(self):
        self.pool = await asyncpg.create_pool(**self.conn_params)
        logger.info("Database connection pool established")

    async def async_close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database connection pool closed")

    async def async_create_table(self):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS url_data (
                        id SERIAL PRIMARY KEY,
                        url TEXT UNIQUE NOT NULL,
                        data JSONB NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_url_data_url ON url_data (url);
                    CREATE INDEX IF NOT EXISTS idx_url_data_data ON url_data USING GIN (data);
                """)
            logger.info("url_data table and indexes created or already exist")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    async def async_insert_record(self, url, data):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO url_data (url, data) VALUES ($1, $2) ON CONFLICT (url) DO UPDATE SET data = $2",
                    url, json.dumps(data)
                )
            logger.info(f"Record inserted or updated for URL: {url}")
        except Exception as e:
            logger.error(f"Failed to insert/update record for URL {url}: {e}")
            raise

    async def async_update_record(self, url, record, value):
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(
                    "UPDATE url_data SET data = jsonb_set(data, $1, $2::jsonb) WHERE url = $3 RETURNING id",
                    f"{{{record}}}", json.dumps(value), url
                )
                if result is None:
                    logger.warning(f"No record found for URL: {url}")
                    return False
            logger.info(f"Record updated for URL: {url}, field: {record}")
            return True
        except Exception as e:
            logger.error(f"Failed to update record for URL {url}, field {record}: {e}")
            raise

    async def async_get_record(self, url):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM url_data WHERE url = $1", url)

    async def async_upsert_record(self, url, data):
        try:
            async with self.pool.acquire() as conn:
                existing_data = await self.async_get_record(url)

                if existing_data:
                    # Update existing record
                    data["first_crawl_time"] = existing_data["data"]["first_crawl_time"]
                    data["crawl_count"] = existing_data["data"].get("crawl_count", 0) + 1
                else:
                    # New record
                    data["first_crawl_time"] = data["last_crawl_time"]
                    data["crawl_count"] = 1

                result = await conn.fetchrow("""
                    INSERT INTO url_data (url, data)
                    VALUES ($1, $2)
                    ON CONFLICT (url) DO UPDATE SET data = $2
                    RETURNING (data->>'crawl_count')::int as crawl_count
                """, url, json.dumps(data))

                logger.info(f"Record upserted for URL: {url}")
                return result["crawl_count"] if result else 1
        except Exception as e:
            logger.error(f"Failed to upsert record for URL {url}: {e}")
            raise