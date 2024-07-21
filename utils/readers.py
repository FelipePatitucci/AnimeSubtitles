from time import sleep
from typing import Any, Callable, Optional

# import logging
import pandas as pd
import requests
from prefect import get_run_logger

from .constants import (
    DEFAULT_ATTEMPTS,
    DEFAULT_TIMEOUT,
    DEFAULT_WAIT_TIME,
    # FORMAT,
)
# logger = logging.getLogger(__name__)
# level = logging.INFO
# logging.basicConfig(
#     format=FORMAT,
#     level=level,
#     handlers=[logging.StreamHandler()])


def read_url(
    url: str,
    max_retries: int = DEFAULT_ATTEMPTS,
    timeout: int = DEFAULT_TIMEOUT,
    wait_time: int = DEFAULT_WAIT_TIME,
    process_fn: Optional[Callable[[requests.Response], Any]] = None
) -> requests.Response | Any:
    logger = get_run_logger()
    attempts = 0
    completed = False
    wait = False

    for attempts in range(max_retries):
        if wait:
            sleep(wait_time)
        try:
            res = requests.get(url=url, timeout=timeout)
            completed = res.ok
            if completed:
                break

            # lets try again but now waiting a little
            wait = True
            attempts += 1
            wait_time *= attempts
            logger.info(
                f"Received '{res.reason}' for link {url}. Trying again after {wait_time}s."
            )

        except TimeoutError:
            logger.error(
                f"Timeout during url {url} request. (attempt: {attempts + 1})"
            )

        except Exception as e:
            logger.debug(str(e))
            logger.warning(
                f"Failed to request subtitle data from link {url}. (attempt: {attempts + 1})"
            )

    if not completed:
        logger.warning(
            f"Failed to get response from url {url} after {max_retries} attempts."
        )
        # res = ""

    elif process_fn is not None:
        res = process_fn(res)

    return res


def read_postgres(
    con,
    query: str,
    cleanup: bool = True
) -> pd.DataFrame:
    logger = get_run_logger()

    try:
        df = pd.read_sql(sql=query, con=con)

    except Exception as err:
        logger.error(f"Error executing query: {query}.\n", err)
        raise

    finally:
        if con and cleanup:
            con.close()

    return df
