import json
import logging
import time
import os
# from typing import Dict, List

from dotenv import load_dotenv
from utils.parsers import (
    download_subtitles,
)
from utils.helpers import (
    generate_ass_files,
    build_df_from_ass_files,
)
from utils.routines import build_json_with_links
from utils.writers import write_postgres, merge_quotes
from utils.connectors import postgres_connector
from utils.constants import FORMAT, DESIRED_SUBS

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

# Specify parameters
start = time.time()
page_count = 1
page_limit = 6
filter_links = None
desired_subs = DESIRED_SUBS
raw_schema = "raw_quotes"
processed_schema = "processed_quotes"

# getting links for subtitle files
for page in range(1, page_count + 1):
    data = build_json_with_links(
        page=page,
        limit_per_page=page_limit,
        desired_subs=desired_subs,
        filter_links=filter_links,
    )
    with open(f"examples/page_{page}.json", "w+", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

end = time.time()
logger.info(f"Finished getting links in {round(end - start)}s")

# downloading data from website and generating ass files
file_path = "examples/page_1.json"
anime_list = download_subtitles(
    file_path=file_path,
)
created = generate_ass_files()

# writing data to db for each anime
if not created:
    created = os.listdir("data")

con = postgres_connector(
    user=user,
    password=password,
    host=host,
    database=database,
    port=port
)

try:
    for anime in created:
        logger.info(f"---------- Processing anime: {anime} ----------")
        df = build_df_from_ass_files(
            file_path=file_path,
            anime_name=anime
        )

        write_postgres(
            df=df,
            con=con,
            schema=raw_schema,
            table_name=anime
        )

        df = merge_quotes(
            conn=con,
            schema=raw_schema,
            table_name=anime
        )

        write_postgres(
            df=df,
            con=con,
            schema=processed_schema,
            table_name="merged_" + anime
        )

        # write_postgres(
        #     df=df,
        #     con=con,
        #     schema=raw_schema,
        #     table_name=anime
        # )


except Exception as err:
    logger.error(err)
    raise

finally:
    con.close()
