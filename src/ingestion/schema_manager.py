"""
Schema Manager
Manages Snowflake DDL: tables, views, schemas, clustering keys.
"""
import logging
from typing import List
import snowflake.connector

logger = logging.getLogger(__name__)

RAW_TABLES = {
    "EVENTS": """
        CREATE TABLE IF NOT EXISTS RAW.EVENTS (
            event_id        VARCHAR(64)       NOT NULL,
            user_id         VARCHAR(32),
            event_type      VARCHAR(64),
            session_id      VARCHAR(64),
            properties      VARIANT,
            received_at     TIMESTAMP_NTZ,
            server_time     TIMESTAMP_NTZ,
            _ingested_at    TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP(),
            _source_file    VARCHAR(512),
            _row_number     NUMBER
        )
        CLUSTER BY (DATE(received_at), event_type)
        COMMENT = 'Raw event data auto-ingested from S3'
    """,
    "TRANSACTIONS": """
        CREATE TABLE IF NOT EXISTS RAW.TRANSACTIONS (
            txn_id          VARCHAR(64)       NOT NULL,
            user_id         VARCHAR(32),
            amount          NUMBER(18,4),
            currency        VARCHAR(3)        DEFAULT 'USD',
            status          VARCHAR(32),
            type            VARCHAR(32),
            metadata        VARIANT,
            created_at      TIMESTAMP_NTZ,
            updated_at      TIMESTAMP_NTZ,
            _ingested_at    TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP()
        )
        CLUSTER BY (DATE(created_at))
    """,
    "USERS": """
        CREATE TABLE IF NOT EXISTS RAW.USERS (
            user_id         VARCHAR(32)       NOT NULL,
            email           VARCHAR(256),
            properties      VARIANT,
            created_at      TIMESTAMP_NTZ,
            updated_at      TIMESTAMP_NTZ,
            _ingested_at    TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP()
        )
    """,
}

class SchemaManager:
    def __init__(self, sf_config: dict):
        self.conn = snowflake.connector.connect(**sf_config)

    def create_schemas(self):
        for schema in ["RAW", "STAGING", "MARTS", "AUDIT"]:
            self.conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        logger.info("Schemas created: RAW, STAGING, MARTS, AUDIT")

    def create_raw_tables(self):
        for name, ddl in RAW_TABLES.items():
            self.conn.cursor().execute(ddl)
            logger.info(f"Created table RAW.{name}")

    def get_table_stats(self) -> List[dict]:
        sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT,
               ROUND(BYTES/1e9, 3) AS SIZE_GB, LAST_ALTERED
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN ('RAW', 'STAGING', 'MARTS')
        ORDER BY TABLE_SCHEMA, SIZE_GB DESC
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def close(self):
        self.conn.close()
