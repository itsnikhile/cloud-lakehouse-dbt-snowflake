install:
	pip install -r requirements.txt

setup-snowflake:
	python main.py setup

run-incremental:
	python main.py incremental

run-full-refresh:
	python main.py full-refresh

dbt-run:
	cd dbt_project && dbt run --target prod

dbt-test:
	cd dbt_project && dbt test --target prod

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve

test:
	pytest tests/ -v --cov=src
