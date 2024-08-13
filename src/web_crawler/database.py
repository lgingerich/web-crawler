# import psycopg
# from psycopg.rows import dict_row
# import json
# from contextlib import contextmanager
# from utils import logger


import psycopg
from psycopg.rows import dict_row
import json
from contextlib import contextmanager
from utils import logger
from psycopg.conninfo import make_conninfo

class DatabaseManager:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.conn_params = (
            f"dbname={dbname} user={user} password={password} host={host} port={port}"
        )
        self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed")

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.conn_params)
            logger.info("Database connection established")

    @contextmanager
    def get_cursor(self, row_factory=dict_row):
        """
        Context manager for database cursor operations.
        """
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.conn_params)
            logger.info("Database connection established")

        try:
            with self.conn.cursor(row_factory=row_factory) as cur:
                yield cur
                self.conn.commit()
        except psycopg.Error as e:
            self.conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Unexpected error: {e}")
            raise

    def create_table(self):
        try:
            with self.get_cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS url_data (
                        id SERIAL PRIMARY KEY,
                        url TEXT UNIQUE NOT NULL,
                        data JSONB NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_url_data_url ON url_data (url);
                    CREATE INDEX IF NOT EXISTS idx_url_data_data ON url_data USING GIN (data);
                """
                )
            self.logger.info("url_data table and indexes created or already exist")
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            raise

    def insert_record(self, url, data):
        try:
            with self.get_cursor() as cur:
                cur.execute(
                    "INSERT INTO url_data (url, data) VALUES (%s, %s) ON CONFLICT (url) DO UPDATE SET data = %s",
                    (url, json.dumps(data), json.dumps(data)),
                )
            self.logger.info(f"Record inserted or updated for URL: {url}")
        except Exception as e:
            self.logger.error(f"Failed to insert/update record for URL {url}: {e}")
            raise

    def update_record(self, url, record, value):
        try:
            with self.get_cursor() as cur:
                cur.execute(
                    "UPDATE url_data SET data = jsonb_set(data, %s, %s::jsonb) WHERE url = %s RETURNING id",
                    ("{" + record + "}", json.dumps(value), url),
                )
                if cur.rowcount == 0:
                    self.logger.warning(f"No record found for URL: {url}")
                    return False
            self.logger.info(f"Record updated for URL: {url}, field: {record}")
            return True
        except Exception as e:
            self.logger.error(
                f"Failed to update record for URL {url}, field {record}: {e}"
            )
            raise

    def get_record(self, url):
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM url_data WHERE url = %s", (url,))
            return cur.fetchone()

    def upsert_record(self, url, data):
        try:
            with self.get_cursor() as cur:
                existing_data = self.get_record(url)

                if existing_data:
                    # Update existing record
                    data["first_crawl_time"] = existing_data["data"]["first_crawl_time"]
                    data["crawl_count"] = (
                        existing_data["data"].get("crawl_count", 0) + 1
                    )
                else:
                    # New record
                    data["first_crawl_time"] = data["last_crawl_time"]
                    data["crawl_count"] = 1

                cur.execute(
                    """
                    INSERT INTO url_data (url, data)
                    VALUES (%s, %s)
                    ON CONFLICT (url) DO UPDATE SET data = %s
                    RETURNING (data->>'crawl_count')::int as crawl_count
                """,
                    (url, json.dumps(data), json.dumps(data)),
                )

                result = cur.fetchone()
                self.logger.info(f"Record upserted for URL: {url}")
                return result["crawl_count"] if result else 1
        except Exception as e:
            self.logger.error(f"Failed to upsert record for URL {url}: {e}")
            raise
