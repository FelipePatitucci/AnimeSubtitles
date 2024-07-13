# import json
import logging
import os
# import re
import requests
from time import sleep
from typing import Any, Dict, List, Optional, Tuple, Union
from bs4 import BeautifulSoup
from bs4.element import Tag
from .constants import (
    MAIN_URL,
    REMOVE_REPACK,
    FORMAT,
    DESIRED_SUBS,
    DEFAULT_ATTEMPTS,
    DEFAULT_WAIT_TIME,
)
from .helpers import (
    get_provider,
    convert_title_to_size,
    filter_subs,
    create_folders_for_anime,
    get_mal_id,
    process_data_input,
    create_data_folder,
    # format_title_for_filter,
    find_episode_number,
    # find_season,
    remove_special_characters,
)

# setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    format=FORMAT,
    level=logging.INFO,
    handlers=[logging.StreamHandler()])


def get_animes_finished_from_page(page: int = 1) -> List[Optional[Tag]]:
    url = MAIN_URL + f"?page={page}"
    finished_entries = []
    try:
        response = requests.get(url=url, timeout=60)
        data = response.text
        soup = BeautifulSoup(data, 'html.parser')
        for div in soup.find_all('div', class_='home_list_entry'):
            if '(finished)' in div.text or '(movie)' in div.text:
                finished_entries.append(div)
        logger.info(f"Processed page {page} request.")

    except TimeoutError:
        logger.error(f"Timeout when during page {page} request.")

    except Exception as e:
        logger.error(str(e))

    return finished_entries


def get_batch_options_and_episode_count(title: str, link: str) \
        -> Tuple[Dict[str, str], List[str], int, int]:
    batch_options = {}
    url = link + REMOVE_REPACK
    res = requests.get(url, timeout=60)
    soup = BeautifulSoup(res.text, 'html.parser')

    # get all candidates
    parent_divs = soup.find_all("div", class_="home_list_entry")
    # the entry may not have any options, so we have to skip it
    current_choice = 0
    fonts_set = set()
    for candidate in parent_divs:
        # skip all batches
        is_batch = candidate.find("div", class_="links").find("em")
        if is_batch:
            continue

        # skip options > 16 GB
        size_str = candidate.find("div", class_="size").get("title")
        size_in_gb = convert_title_to_size(size_str)
        if size_in_gb > 16.0:
            continue

        # TODO: Maybe get the amount of links from provider on the page
        batch_obj = candidate.find("div", class_="link").find("a")
        provider = get_provider(batch_obj.text)
        if not provider or provider in fonts_set:
            # just one example per provider is enough
            continue

        batch_options[provider] = batch_obj.get("href")
        fonts_set.add(provider)
        current_choice += 1

    # not a single option below 16gb for this anime
    if not current_choice:
        logger.warning(
            f"No available file below 16GB for anime {title}."
        )

    # get episode count
    episode_div = soup.select_one("table > tbody > tr > td > div > div")
    try:
        episode_count = episode_div.text.split(
            " episode(s)")[0].split(", ")[-1]
        logger.info(f"Got episode count of {episode_count}.")
    except Exception:
        episode_count = 0

    # get MAL id
    infos_div = soup.select("table > tbody > tr > td > div", limit=2)
    mal_id = get_mal_id(div=infos_div, title=title)

    return batch_options, list(fonts_set), int(episode_count), mal_id


def get_subtitle_links(link: str, desired_subs: str = DESIRED_SUBS) -> Tuple[str, str]:
    sub_info, sub_link = "", ""
    wait, wait_time = False, DEFAULT_WAIT_TIME
    max_attempts = DEFAULT_ATTEMPTS
    completed = False
    attempts = 0

    if not link:
        return "", ""

    # TODO: move this whole request logic to a helper function
    while not completed and attempts < max_attempts:
        if wait:
            sleep(wait_time)
        try:
            res = requests.get(url=link, timeout=60)
            completed = res.ok
            if completed:
                break
            # lets try again but now waiting a little
            wait = True
            attempts += 1
            wait_time *= attempts
            logger.info(f"Received '{res.reason}' for link {link}. "
                        f"Trying again after {wait_time}s.")

        except Exception as e:
            logging.debug(str(e))
            logging.warning(
                f"Failed to request subtitle data from link {link}.")
            return "", ""

    soup = BeautifulSoup(res.text, 'html.parser')
    content = soup.find("div", id="content")
    if not content or (attempts == max_attempts):
        # no divs implies no subtitles
        logger.warning(
            f"Could not get subtitles for link {link} after {max_attempts} "
            "attempts.")
        return "", ""

    tables = content.find_all("table", recursive=False)
    if not tables:
        # no tables implies no subtitles
        logger.warning(f"No table found on link {link}.")
        return "", ""

    for table in tables:
        # if last row has Subtitles as header, then page may have download links
        last_row = table.find_all("tr", recursive=False)[-1]

        if last_row.find("th").text == "Subtitles":
            # now, we may or may not have eng subs
            target_row = last_row
            sub_options = parse_subtitles(target_row.find("td"))

            # check for en subs and see if has .ass downloadable file
            sub_info, sub_link = filter_subs(
                link=link, subs=sub_options, target_lang=desired_subs
            )
            break

    return sub_info, sub_link


def parse_subtitles(subs: Optional[Tag]) -> Dict[str, str]:
    if not subs:
        return dict()

    subs_links = dict()
    attachments = subs.find_all("a")
    for sub in attachments:
        subs_links[sub.text] = sub.get("href")
    return subs_links


def get_all_links_from_provider(provider: str, page: str, link: str) \
        -> Tuple[List[Dict[str, str]], bool]:
    episode_links = []
    url = link + REMOVE_REPACK + f"&page={page}"
    res = requests.get(url, timeout=60)
    soup = BeautifulSoup(res.text, 'html.parser')
    has_entries = False

    parent_divs = soup.find_all("div", class_="home_list_entry")
    # if no parent_divs, then there is nothing on the page
    if parent_divs:
        has_entries = True

        for candidate in parent_divs:
            # skip all batches
            is_batch = candidate.find("div", class_="links").find("em")
            if is_batch:
                continue

            # skip options > 16 GB
            size_str = candidate.find("div", class_="size").get("title")
            size_in_gb = convert_title_to_size(size_str)
            if size_in_gb > 16.0:
                continue

            curr_link = candidate.find("div", class_="link").find("a")
            if provider in curr_link.text:
                episode_links.append({
                    "link_title": curr_link.text,
                    "link_url": curr_link.get("href")
                })
    return episode_links, has_entries


def get_all_subtitles_info(
        title: str,
        anime_info: dict[str, Any],
        provider_name: str,
        desired_subs: str = DESIRED_SUBS
) -> List[Dict[str, str]]:
    final_object = []
    already_obtained_links = set()
    already_obtained_episodes = set()
    episode_count = anime_info["metadata"]["episode_count"]
    total_to_gather = len(anime_info["data"])

    if total_to_gather == 0:
        logger.info(f"Anime {title} does not have subtitles available.")
        return []

    logger.info(f"Gathering subtitle links for anime {title}...")

    for idx, item in enumerate(anime_info["data"]):

        if ((idx+1) % 10) == 0 or (idx + 1) == total_to_gather:
            logger.info(f"[Progress|Total]: [{idx+1}|{total_to_gather}]")

        if len(final_object) == episode_count:
            # we are done, maybe the rest are from other seasons (let's hope)
            logger.info(
                f"Already got {episode_count} episode links. Skipping the remaining ones..."
            )
            break

        link_url = item.get("link_url", "")
        link_title = item.get("link_title", "")

        sub_info, sub_link = get_subtitle_links(
            link_url, desired_subs=desired_subs)

        # skip repeated episodes and episodes without subs
        if sub_link in already_obtained_links or not sub_link:
            continue

        episode_number = find_episode_number(link_title)
        # season = find_season(link_title, provider_name)

        if episode_number in already_obtained_episodes:
            # maybe duplicate link
            continue

        if not episode_number:
            if episode_count == 1:
                # assuming it is a movie, so set ep to "1"
                episode_number = 1
            else:
                # not worth it (may be .5 episodes or some alien format)
                logger.info(
                    f"Skipped episode {link_title} due to not finding ep number."
                )
                continue

        item["sub_link"] = sub_link
        item["sub_info"] = sub_info
        item["episode_number"] = episode_number
        # item["season"] = season
        final_object.append(item)
        already_obtained_links.add(sub_link)
        already_obtained_episodes.add(episode_number)

    return final_object


def get_subtitle_file(link: str) -> Optional[requests.Response]:
    try:
        response = requests.get(link, timeout=10)
    # TODO: make this better (specify problem)
    except Exception as e:
        logger.warning(
            f"Error when downloading file from link: {link}"
        )
        logger.debug(e)
        response = None

    return response


def save_subtitle_file(
    response: Optional[requests.Response],
    file_path: str
) -> bool:
    completed = False
    # bad request, did not got file
    if not response:
        return completed

    # if request is successful proceed
    if response.status_code == 200:
        filename = file_path.split("/")[-1]
        with open(file_path, 'wb') as file:
            # write response object to file
            file.write(response.content)
            logger.debug(f'{filename} downloaded successfully.')
            completed = True
    else:
        logger.error(
            f'Failed to download {filename}. Status code:', response.status_code)

    return completed


def download_subtitles(
    file_path: Union[str, Dict[str, List[Dict[str, str]]]],
    filter_anime: str = "",
) -> None:
    # verify data
    data = process_data_input(file_path)

    # nothing to be done
    if not data:
        return

    # create general data folder to store every anime folder
    try:
        create_data_folder()
    except Exception as e:
        raise e

    filter_anime = remove_special_characters(
        filter_anime).replace(" ", "_").lower()
    # iterate over every anime on .json file
    for anime, anime_info in data.items():
        # target just entry/entries from filter
        if filter_anime and filter_anime not in anime:
            continue

        logger.info(f"---------- Processing anime {anime} ----------")
        entries = anime_info["data"]
        if not entries:
            # nothing to be done
            logger.info("No links available for this anime. Skipping...")
            continue

        error_count = 0
        result = create_folders_for_anime(anime_name=anime)

        if not result:
            logger.warning(
                f"Failed creating folders for anime {anime}. Skipping...")
            continue

        folder_path = f'data/{anime}/raw'

        logger.info("Downloading subtitles...")
        for idx, entry in enumerate(entries):

            if ((idx+1) % 10) == 0 or (idx+1) == len(entries):
                logger.info(f"[Progress|Total]: [{idx+1}|{len(entries)}]")

            episode = entry.get("episode_number", "")
            if not episode:
                # we dont even know the ep number, no reason to save this
                logger.debug(
                    f"No episode number for entry {entry['link_title']}.")
                continue

            filename = f'ep_{episode}.xz'
            sub_link = entry.get("sub_link", "")

            if not sub_link:
                # not sub link available
                logger.debug(
                    f"Subtitle file for episode {episode} does not exists.")
                continue

            # check if file is already downloaded
            if filename not in os.listdir(folder_path):
                # path like data/anime_name/ep_number.xz
                file_path = os.path.join(folder_path, filename)
                sub_file = get_subtitle_file(link=sub_link)
                completed = save_subtitle_file(
                    response=sub_file, file_path=file_path
                )
                if not completed:
                    error_count += 1

            else:
                logger.debug(
                    f'{folder_path + "/" + filename} is already downloaded')
                continue

        logger.info(f"Finished downloading files for anime {anime}.")
        if error_count > 0:
            logger.info(
                f"Failed {error_count} from a total of {len(entries)} files.")

    return
