import logging
import pandas as pd
from typing import Literal, Optional
from sqlalchemy.engine.base import Engine
from .constants import FORMAT

# setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    format=FORMAT,
    level=logging.INFO,
    handlers=[logging.StreamHandler()])


def write_data(
    table_name: str, con: Engine, df: pd.DataFrame,
    if_exists: Literal["fail", "replace", "append"] = "fail",
    clear_songs: bool = True
) -> Optional[int]:
    # empty df
    if df.empty:
        logger.info("Nothing to be done, empty dataframe.")
        return 0

    if clear_songs:
        # TODO: this could use some work (maybe change to isin (op, opening, etc.))
        songs = df[(df["NAME"] == "ED") | (df["NAME"] == "OP")]
        if len(songs) > 0:
            # drop every row with op or ed
            df = df.drop(songs.index)
            # now just concat the unique texts from cleaned ops and eds
            songs = songs.drop_duplicates(subset=["NAME", "QUOTE"])
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
