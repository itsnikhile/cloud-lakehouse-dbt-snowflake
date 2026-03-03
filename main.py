"""
Cloud Lakehouse Pipeline Entry Point
Usage:
  python main.py setup           # Create Snowflake schemas + tables + pipes
  python main.py incremental     # Run incremental dbt refresh
  python main.py full-refresh    # Run full dbt refresh
  python main.py status          # Show pipeline status
"""
import sys, logging, yaml, subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def load_config():
    import os
    with open("config/config.yaml") as f:
        content = f.read()
    for key, val in os.environ.items():
        content = content.replace(f"${{{key}}}", val)
    import yaml
    return yaml.safe_load(content)

def run_dbt(select=None, command="run"):
    cmd = ["dbt", command, "--target", "prod", "--project-dir", "dbt_project"]
    if select:
        cmd += ["--select", select]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt command failed")
    logger.info(result.stdout[-300:])

def setup(config):
    from src.ingestion.schema_manager import SchemaManager
    from src.ingestion.snowpipe_manager import SnowpipeManager
    sf = SchemaManager(config["snowflake"])
    sf.create_schemas()
    sf.create_raw_tables()
    sf.close()
    logger.info("Snowflake setup complete")

def incremental():
    logger.info("Running incremental refresh...")
    run_dbt(select="staging", command="run")
    run_dbt(select="staging", command="test")
    logger.info("Incremental refresh complete")

def full_refresh():
    logger.info("Running full refresh...")
    run_dbt(command="run")
    run_dbt(command="test")
    run_dbt(command="docs generate")
    logger.info("Full refresh complete")

if __name__ == "__main__":
    config = load_config()
    mode = sys.argv[1] if len(sys.argv) > 1 else "status"
    {"setup": setup, "incremental": incremental, "full-refresh": full_refresh}.get(
        mode, lambda c: logger.info("Usage: setup | incremental | full-refresh")
    )(config)
