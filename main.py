import requests
import pandas as pd
import sqlparse
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
import time
from pathlib import Path
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

data_url = 'https://drive.google.com/uc?export=download&id=1vbHELLhLzr5VaDiaeesDQ6BRsEoFd9lK'

psql_user = os.environ['PSQL_USER']
psql_pass = os.environ['PSQL_PASS']
psql_host = os.environ['PSQL_HOST']
psql_port = os.environ['PSQL_PORT']
psql_db = os.environ['PSQL_DB']
truncate = os.environ['TRUNCATE']

def check_db(engine: object):
    with engine.connect() as conn:
        res = conn.execute(
            text(
                "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
        )
        correct = False
        for row in res.fetchall():
            if row[1] in {'raw_data','publishers', 'trackers', 'clicks', 'devices'}:
                correct = True
            else:
                return False

        return correct

def init_tables(engine: object):
    init_table_sql = BASE_DIR / 'ddl.sql'
    sql = init_table_sql.open('r', encoding='utf-8').read()
    sqls = sqlparse.format(sql, reindent=True, strip_comments=True)
    with engine.connect() as conn:
        for sql_command in sqlparse.split(sqls):
            conn.execute(text(sql_command))

    print('Tables created')


def main():
    start = time.time()

    engine = create_engine(f'postgresql://{psql_user}:{psql_pass}@{psql_host}:{psql_port}/{psql_db}', )

    if not check_db(engine):
        init_tables(engine)
    else:
        print("All required tables already exist.")
    end = time.time()
    print('Operation completed in', f'{(end - start):.2f}', 'seconds')


if __name__ == '__main__':
    main()