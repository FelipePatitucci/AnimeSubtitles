# import logging
from typing import Any, Dict

from prefect import get_run_logger

from utils.constants import DESIRED_SUBS, MEMBER_CUT
from utils.helpers import (
    check_for_id,
    extract_titles_and_anime_links,
    filter_links_from_provider,
    remove_special_characters,
    sort_options_by_priority,
)
from utils.parsers import (
    get_all_links_from_provider,
    get_all_subtitles_info,
    get_animes_finished_from_page,
    get_batch_options_and_episode_count,
    get_subtitle_links,
    get_title_name,
)
from utils.readers import read_url

# logger = logging.getLogger(__name__)
# level = logging.INFO
# logging.basicConfig(
#     format=FORMAT,
#     level=level,
#     handlers=[logging.StreamHandler()])


def build_json_with_links(
    page: int = 1,
    limit_per_page: int = 1,
    filter_links: list[str] = None,
    desired_subs: str = DESIRED_SUBS,
    already_collected_animes: dict[str, dict[str, Any]] = dict(),
) -> Dict[str, Any]:
    """
    Constructs a dictionary containing anime titles and corresponding lists
    of dictionaries with subtitle and torrent link information.

    This function fetches a list of animes entries and its subtitles information (
    download link and language) from animethosho website. Supports filtering
    per link. For each title, it attempts to find a subtitle
    provider that matches the desired subtitle language or style (if specified).
    It collects and returns all relevant links to subtitles for each anime title
    processed.

    Parameters:
    - page (int, optional): The page number from which to fetch data. Default is 1.
    - limit_per_page (int, optional): The maximum number of anime titles to process from
        the fetched page. If you want all from the page, set to a high value like 999.
        Default is 1.
    - filter_links (list[str], optional): A specific page title to process.
        If not empty, will only process this link, page parameters are ignored.
        Default is an empty string.
    - desired_subs (str, optional): The desired subtitle language (e.g. "eng").
        Default is "eng".

    Returns:
    - Dict[str, Any]: A dictionary containing data and metadata about the entry.

    The function logs various information and errors throughout the processing,
    including issues with data fetching, provider selection, and subtitle retrieval.
    It continues processing next titles or pages until the specified limit is
    reached or there are no more entries.
    """
    logger = get_run_logger()
    data = {}
    if filter_links is None:
        filter_links = []
    animes = get_animes_finished_from_page(page=page)

    if not animes:
        logger.error(f"Bad response from page {page}. Skipping...")
        return dict()

    titles, links = extract_titles_and_anime_links(
        animes=animes, filter_links=filter_links
    )

    # while this is kinda bad architecture, it's the simplest way without rewriting much code
    if filter_links:
        logger.info(f"Processing only links: {'; '.join(filter_links)}.")
        links = filter_links
        # we have to make a call here to get the anime title
        # this will be used for the table name
        titles = [read_url(url=link, process_fn=get_title_name) for link in links]

    if not titles or not links:
        logger.info(f"Nothing to process on page {page}.")
        return dict()

    if limit_per_page < len(links):
        logger.info(
            f"Will only process {limit_per_page} of {len(links)} entries from page {page}."
        )

    for title, link in zip(titles[:limit_per_page], links[:limit_per_page]):
        logger.info(f"Processing link: {link}")
        logger.info(f"Processing anime: {title}")

        providers_info, episode_count, mal_id = get_batch_options_and_episode_count(
            title=title, link=link
        )
        logger.debug(f"Batch Providers: {providers_info}")

        # check if we already have this full entry
        if already_collected_animes.get(mal_id, {}).get("completed", False):
            logger.info(
                f"Anime [{title}] with id of {mal_id} already completed in database. Skipping..."
            )
            continue

        if episode_count == 0 or mal_id == 0:
            # we will not be able to sort our data appropriatelly
            logger.info("Could not find either episode count or MAL ID. Skipping...")
            continue

        if len(providers_info) == 0:
            # nothing we can do
            logger.warning(f"No available provider for anime {title}. Skipping...")
            continue

        # TODO: we need to rebuild this map to include recent animes
        is_relevant = check_for_id(mal_id=mal_id, members_cut=MEMBER_CUT)
        if not is_relevant:
            logger.info(
                f"Anime {title} has less than {MEMBER_CUT} members. Ignoring..."
            )
            continue

        provider_selected = ""
        # sort provider_names by priority (preference, then amount of links)
        providers_info = sort_options_by_priority(providers_info)

        # search for a functional provider
        for provider_name, info in providers_info.items():
            # link to test provider
            trial_link = info["trial_link"]
            sub_info, sub_link = get_subtitle_links(trial_link, desired_subs)

            if sub_info and sub_link:
                provider_selected = provider_name
                logger.info(f"Selected {provider_selected} provider for anime {title}.")
                break

        if not provider_selected:
            # nothing to be done
            logger.warning(
                f"No available provider with subtitles for anime {title}. Skipping..."
            )
            continue

        # if we get here, we may have good data for this entry, let's process it
        processing = True
        page = 1
        title_key = remove_special_characters(title).replace(" ", "_").lower()

        if title_key[0].isdigit():
            # table names cannot start with digits
            title_key = "_" + title_key

        data[title_key] = {
            "data": [],
            "metadata": {
                "episode_count": episode_count,
                "mal_id": mal_id,
                "original_name": title,
            },
        }

        while processing:
            logger.info(f"Parsing page {page}")
            page_links, has_entries = get_all_links_from_provider(
                provider_selected, page, link
            )
            data[title_key]["data"] += page_links
            processing = has_entries
            page += 1

        data[title_key]["data"] = filter_links_from_provider(
            data[title_key]["data"], provider_selected, episode_count
        )

    for anime_title, anime_info in data.items():
        all_subs_info = get_all_subtitles_info(
            anime_title, anime_info, provider_selected, desired_subs
        )
        data[anime_title]["data"] = all_subs_info

        # if this is a new entry, we will write it regardless
        # however, if this is duplicate, we only want to write back to db if it has more eps
        current_id = data[anime_title]["metadata"]["mal_id"]
        eps_in_db = already_collected_animes.get(current_id, {}).get("ep_amount", 0)

        if eps_in_db >= len(all_subs_info):
            logger.info(
                f"This anime curretly has {eps_in_db} eps in db. This iteration "
                f"would provide {len(all_subs_info)} eps, so it will not be inserted."
            )
            data[anime_title]["data"] = []

    return data
