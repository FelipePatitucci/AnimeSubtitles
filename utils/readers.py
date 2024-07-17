from typing import Any, Callable, Optional, Tuple

import logging
import pandas as pd
import requests

from .constants import (
    DEFAULT_ATTEMPTS,
    DEFAULT_TIMEOUT,
    DEFAULT_WAIT_TIME,
    FORMAT,
    MAIN_URL,
)
logger = logging.getLogger(__name__)
level = logging.INFO
logging.basicConfig(
    format=FORMAT,
    level=level,
    handlers=[logging.StreamHandler()])


class AnimeTosho:

    def __init__(
        self,
        url: str = MAIN_URL,
        attempts: int = DEFAULT_ATTEMPTS,
        timeout: int = DEFAULT_TIMEOUT,
        wait_time: int = DEFAULT_WAIT_TIME,
        silent: bool = True
    ) -> None:
        self.url = url
        self.attempts = attempts
        self.timeout = timeout
        self.wait_time = wait_time
        self.silent = silent
        pass

    def get(
        self,
        page: int = 0,
        process_fn: Optional[Callable] = None,
        silent: bool = True
    ) -> Tuple[Any, int]:
        data = ""
        code = ""
        silent = silent and self.silent

        if page:
            url = self.url + f"?page={page}"

        try:
            res = requests.get(url=url, timeout=60)
            data = res.text
            code = res.status_code
            logger.info(f"Processed page {page} request.")

        except TimeoutError:
            logger.error(f"Timeout when during page {page} request.")

        except Exception as e:
            logger.error(str(e))

        if process_fn is not None and data:
            data = process_fn(data)

        return data, code


def read_postgres(
    con,
    query: str,
    cleanup: bool = True
) -> pd.DataFrame:
    try:
        df = pd.read_sql(sql=query, con=con)
    except Exception as err:
        logger.error(f"Error executing query: {query}.\n", err)
        raise

    finally:
        if con and cleanup:
            con.close()

    return df
