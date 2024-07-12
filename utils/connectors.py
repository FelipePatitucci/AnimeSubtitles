import os

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def sqlite_connector(db_name: str) -> Engine:
    if db_name[-3:] != ".db":
        db_name += ".db"
    db_path = os.path.realpath(f'database/{db_name}')
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    return engine


def postgres_connector(
    user: str,
    password: str,
    host: str,
    database: str,
    port: str,
    autocommit: bool = True
):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        if autocommit:
            # 0 is for autocommit, try psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            connection.set_isolation_level(0)
    except Exception as error:
        print("Error while connecting to PostgreSQL", error)

    return connection
