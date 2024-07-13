import logging
from typing import Any, Literal, Optional

import pandas as pd
import psycopg2.extras

from .constants import FORMAT
from .queries import query_create_table

# setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    format=FORMAT,
    level=logging.INFO,
    handlers=[logging.StreamHandler()])


def write_data(
    df: pd.DataFrame,
    con: Any,
    table_name: str,
    if_exists: Literal["fail", "replace", "append"] = "fail",
    clear_songs: bool = True
) -> Optional[int]:
    # empty df
    if df.empty:
        logger.info("Nothing to be done, empty dataframe.")
        return 0

    if clear_songs:
        # TODO: this could use some work (maybe change to isin (op, opening, etc.))
        songs = df[(df["name"] == "ED") | (df["name"] == "OP")]
        if len(songs) > 0:
            # drop every row with op or ed
            df = df.drop(songs.index)
            # now just concat the unique texts from cleaned ops and eds
            songs = songs.drop_duplicates(subset=["name", "quote"])
            df = pd.concat([df, songs])

    logger.info(f"Preparing to write {len(df)} rows into dataframe...")

    try:
        num_rows = df.to_sql(
            name=table_name, con=con, if_exists=if_exists, index=False
        )
    except Exception as e:
        logger.error(str(e))
        num_rows = 0

    return num_rows


def _create_table(
    con,
    schema: str,
    table_name: str,
) -> None:
    cursor = con.cursor()
    query = query_create_table % (schema, table_name)

    cursor.execute(query)
    con.commit()
    cursor.close()


def _truncate_table(
    con,
    schema: str,
    table_name: str,
) -> None:
    cursor = con.cursor()
    query = "TRUNCATE TABLE %s.%s;" % (schema, table_name)

    cursor.execute(query)
    con.commit()
    cursor.close()


def write_postgres(
    df: pd.DataFrame,
    con: Any,
    schema: str,
    table_name: str,
    if_exists: Literal["replace", "append"] = "replace",
    clear_songs: bool = True
) -> None:
    # empty df
    if df.empty:
        logger.info("Nothing to be done, empty dataframe.")
        return 0

    if clear_songs:
        # TODO: this could use some work (maybe change to isin (op, opening, etc.))
        songs = df[df["name"].isin(
            ["ED", "ed", "Ending", "OP", "op", "Opening"]
        )]
        if len(songs) > 0:
            # drop every row with op or ed
            df = df.drop(songs.index)
            # now just concat the unique texts from cleaned ops and eds
            songs = songs.drop_duplicates(subset=["name", "quote"])
            df = pd.concat([df, songs])

    logger.info(f"Preparing to write {len(df)} rows into dataframe...")

    # need to create table if it not exists
    _create_table(con, schema, table_name)

    # give user option to clear table before insertion
    if if_exists == "replace":
        logger.info(f"Truncating table {schema}.{table_name}...")
        _truncate_table(con, schema, table_name)

    try:
        df_columns = [col.lower() for col in df.columns]
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = f"INSERT INTO {schema}.{table_name} ({columns}) {values}"

        cur = con.cursor()
        # insert in batches
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
        con.commit()
        cur.close()

    except Exception as e:
        logger.error(str(e))

    return


def merge_quotes(
    conn,
    schema: str,
    table_name: str,
    df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Merges consecutive rows with the same NAME and EPISODE into a single row
    and creates a new PostgreSQL table with the merged data.

    Parameters:
    - conn (Postgres conn): Postgres connection.
    - schema (str): Name of the schema to read from.
    - table_name (str): Name of the table in the database to read from.
    - df (pd.DataFrame): If provided, will not query database

    Returns:
    - pd.DataFrame: DataFrame containing the merged data.

    Example:
    merged_df = quote_merge(conn, 'raw_quotes', 'my_anime')
    """

    if df is None:
        query = f'SELECT * FROM {schema}.{table_name};'
        df = pd.read_sql(query, conn)

    new_df = []

    for idx, row in df.iterrows():
        # Initialize variables for a new quote sequence at the start of iteration or when a new quote is encountered
        if idx == 0:
            mal_id = row['mal_id']
            episode = row['episode']
            name = row['name']
            start_time = row['start_time']
            end_time = row['end_time']
            quote = row['quote']

            continue

        current_name = row['name']
        current_episode = row['episode']
        current_start_time = row['start_time']

        # Append to existing quote if the current row belongs to the same quote sequence
        if current_name == name and name != 'Unknown' and current_episode == episode \
                and current_start_time == end_time:
            quote += ' ' + row['quote']
            end_time = row['end_time']
        else:
            new_df.append([mal_id, episode, name, quote, start_time, end_time])

            mal_id = row['mal_id']
            episode = row['episode']
            name = row['name']
            start_time = row['start_time']
            end_time = row['end_time']
            quote = row['quote']

    new_df = pd.DataFrame(
        new_df,
        columns=[
            'mal_id', 'episode', 'name', 'quote', 'start_time', 'end_time'
        ]
    )
    new_df = new_df.astype({'mal_id': 'int32', 'episode': 'int32'})

    return new_df
