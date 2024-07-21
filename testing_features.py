# import lzma
import json
# from bs4 import BeautifulSoup
# import requests
from datetime import datetime
import os
import pandas as pd
import ass
import requests
from bs4 import BeautifulSoup
# from sqlalchemy import create_engine
# from sqlite3 import connect
# from utils.helpers import clean_ass_text

# url = "https://animetosho.org/series/mashle.17478"
# res = requests.get(url=url, timeout=60)
# soup = BeautifulSoup(res.text, 'html.parser')
# mal_div = soup.select("table > tbody > tr > td > div", limit=2)[-1]
# mal_id = int(mal_div.find("a", string="MAL").get("href").split("/")[-1])
# print(mal_id)
# data = ""
# with open("csvjson.json", "r+", encoding="utf-8") as f:
#     data = json.load(f)

# data = {int(item["id"]): item["members"] for item in data}
# with open("mal_id_member_count.json", "w+", encoding="utf-8") as f:
#     json.dump(data, f, indent=4)
import re
BRACKETS_REGEX = r'\{[^{}]*\}|\([^\[\]]*\)'

# def find_numbers(input_string):
#     # Regex para encontrar números conforme as condições especificadas
#     regex = r'[0-9]{2,4}(?=\s)'
#     matches = re.finditer(regex, input_string)
#     results = [(match.group(), match.start()) for match in matches]
#     return results


# s_teste = "[Erai-raws] Spy x Family Season 2 - 02 [1080p][Multiple Subtitle] \
# [ENG][POR-BR][SPA-LA][SPA][ARA][FRE][GER][ITA][RUS]"
# res = list(map(lambda x: x.lower(), s_teste.split(" ")))
# if "season" in res:
#     print(res[res.index("season") + 1])
def remove_special_characters(input_string: str) -> str:
    pattern = r'[\\\"\',.;:?-]'
    clean_text = re.sub(pattern, '', input_string)
    print(clean_text)
    return clean_text


def prepare_text_for_insertion(input_string: str) -> str:
    """
    This is used before writing dataframe to database. This is the last processing step.
    """
    regex = BRACKETS_REGEX
    clean_text = re.sub(regex, '', input_string)
    clean_text = clean_text.replace("\\N", " ").replace("  ", " ")
    return clean_text


st = "\"Oshi no- Ko-\""
# remove_special_characters(st)
# with open("data/ore_dake_level_up_na_ken/processed/ep_01.ass", encoding='utf_8_sig') as f:
#     doc = ass.parse(f)
#     # get every dialogue line
#     events = doc.events
#     for i, event in enumerate(events):
#         print(prepare_text_for_insertion(event.text))
#         if i > 5:
#             break
# with open("examples/page_1.json", "r+", encoding="utf-8") as f:
#     teste = json.load(f)
#     print(list(teste.keys()))
