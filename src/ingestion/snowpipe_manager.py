"""
Snowpipe Manager
Manages S3 → Snowflake auto-ingest via Snowpipe.
Handles pipe creation, refresh triggers, and ingest monitoring.
"""
import boto3, json, logging
from datetime import datetime, timedelta
from typing import List, Dict
import snowflake.connector

logger = logging.getLogger(__name__)

class SnowpipeManager:
    """Manages Snowpipe auto-ingest from S3 to Snowflake RAW schema."""

    def __init__(self, sf_config: dict, s3_bucket: str):
        self.conn = snowflake.connector.connect(**sf_config)
        self.s3 = boto3.client("s3")
        self.bucket = s3_bucket

    def create_stage(self, stage_name: str, s3_prefix: str, aws_role: str):
        sql = f"""
        CREATE STAGE IF NOT EXISTS {stage_name}
          URL = 's3://{self.bucket}/{s3_prefix}/'
          CREDENTIALS = (AWS_ROLE = '{aws_role}')
          FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
        """
        self.conn.cursor().execute(sql)
        logger.info(f"Stage {stage_name} created")

    def create_pipe(self, pipe_name: str, stage: str, target_table: str):
        sql = f"""
        CREATE PIPE IF NOT EXISTS {pipe_name}
          AUTO_INGEST = TRUE
          COMMENT = 'Auto-ingest from S3'
        AS
        COPY INTO {target_table}
        FROM @{stage}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
        self.conn.cursor().execute(sql)
        result = self.conn.cursor().execute(f"SHOW PIPES LIKE '{pipe_name}'").fetchone()
        sqs_arn = result[11] if result else "N/A"
        logger.info(f"Pipe {pipe_name} created. SQS ARN: {sqs_arn}")
        return sqs_arn

    def get_pipe_status(self, pipe_name: str) -> Dict:
        result = self.conn.cursor().execute(f"SELECT SYSTEM$PIPE_STATUS('{pipe_name}')").fetchone()
        return json.loads(result[0]) if result else {}

    def list_new_s3_files(self, prefix: str, since_hours: int = 1) -> List[str]:
        cutoff = datetime.utcnow() - timedelta(hours=since_hours)
        paginator = self.s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["LastModified"].replace(tzinfo=None) > cutoff:
                    files.append(obj["Key"])
        logger.info(f"Found {len(files)} new files since {cutoff.isoformat()}")
        return files

    def refresh_pipe(self, pipe_name: str, files: List[str]):
        if not files:
            return
        file_list = ", ".join([f"'{f}'" for f in files[:100]])
        self.conn.cursor().execute(f"ALTER PIPE {pipe_name} REFRESH FILES = ({file_list})")
        logger.info(f"Refreshed pipe {pipe_name} with {len(files)} files")

    def monitor_ingest_history(self, pipe_name: str, hours: int = 1) -> List[Dict]:
        sql = f"""
        SELECT FILE_NAME, STATUS, ROW_COUNT, FIRST_ERROR_MESSAGE, LAST_LOAD_TIME
        FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'RAW.EVENTS',
            START_TIME => DATEADD('hour', -{hours}, CURRENT_TIMESTAMP())
        ))
        ORDER BY LAST_LOAD_TIME DESC
        LIMIT 100
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def close(self):
        self.conn.close()
