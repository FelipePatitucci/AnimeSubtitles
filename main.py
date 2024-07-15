import json
import logging
import time
import os
import warnings
from datetime import datetime
# from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task

from utils.connectors import postgres_connector
from utils.constants import DESIRED_SUBS, FORMAT
from utils.helpers import (
    build_df_from_ass_files,
    generate_ass_files,
)
from utils.parsers import (
    download_subtitles,
)
from utils.routines import build_json_with_links
from utils.writers import merge_quotes, write_postgres

warnings.filterwarnings('ignore')
# setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    format=FORMAT,
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

load_dotenv()
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")
user = os.getenv("USER")
password = os.getenv("PASSWORD")


@task
def export_links_to_db(
    con,
    data: dict[str, Any],
) -> None:
    today = datetime.today()

    normalized_entries = [
        json.dumps({"name": key, "info": value}) for key, value in data.items()
    ]
    json_df = pd.DataFrame(
        data={
            "json_data": normalized_entries,
        }
    )
    json_df["reference_date"] = today

    write_postgres(
        df=json_df,
        con=con,
        schema="raw_quotes",
        table_name="json_reference",
        if_exists="append",
        clear_songs=False,
        cleanup=True
    )


@task
def get_links_from_web(
    page_count: int = 1,
    page_limit: int = 1,
    desired_subs: str = DESIRED_SUBS,
    filter_links: Optional[list[str]] = None,
    save_links_on_db: bool = True
) -> None:
    start = time.time()
    for page in range(1, page_count + 1):
        data = build_json_with_links(
            page=page,
            limit_per_page=page_limit,
            desired_subs=desired_subs,
            filter_links=filter_links,
        )
        with open(f"examples/page_{page}.json", "w+", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        if save_links_on_db:
            con = postgres_connector(
                user=user,
                password=password,
                host=host,
                database=database,
                port=port
            )
            export_links_to_db(
                con=con,
                data=data
            )

    end = time.time()
    logger.info(
        f"Finished getting links for {page_count} pages in {round(end - start)}s."
    )


@task
def get_subtitles_from_web(
    download_amount: int = 1,
    schema: str = "raw_quotes",
) -> None:
    for idx, file in enumerate(os.listdir("examples")):
        if idx == download_amount:
            logger.info(
                f"Download amount of {download_amount} reached."
            )
            break

        file_path = "examples/" + file

        download_subtitles(
            file_path=file_path,
        )

        created = generate_ass_files()
        if not created:
            # probably, we already had built all the files
            with open(file_path, "r+", encoding="utf-8") as f:
                created = json.load(f).keys()

        con = postgres_connector(
            user=user,
            password=password,
            host=host,
            database=database,
            port=port
        )

        try:
            # writing data to db for each anime
            for anime in created:
                logger.info(f"---------- Processing anime: {anime} ----------")
                df = build_df_from_ass_files(
                    file_path=file_path,
                    anime_name=anime
                )

                if df is None:
                    continue

                df = merge_quotes(
                    conn=con,
                    schema=schema,
                    table_name=anime,
                    df=df
                )

                write_postgres(
                    df=df,
                    con=con,
                    schema=schema,
                    table_name=anime,
                    if_exists="replace",
                    cleanup=False
                )
        except Exception as err:
            logger.error(err)
            raise
        finally:
            con.close()


@flow
def flow(
    get_links: bool = True,
    download_limit: int = 1,
    page_count: int = 1,
    page_limit: int = 1,
    filter_links: Optional[list[str]] = None,
    schema: str = "raw_quotes"
) -> None:
    if get_links:
        get_links_from_web(
            page_count=page_count,
            page_limit=page_limit,
            desired_subs=DESIRED_SUBS,
            filter_links=filter_links,
            save_links_on_db=True
        )

    get_subtitles_from_web(
        download_amount=download_limit,
        schema=schema
    )


flow(
    get_links=True,
    download_limit=1,
    page_count=1,
    page_limit=3,
    filter_links=[],
    schema="raw_quotes"
)
