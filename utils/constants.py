# URL API Configs
MAIN_URL = "https://animetosho.org/animes"
REMOVE_REPACK = "?filter%5B0%5D%5Bt%5D=nyaa_class&filter%5B0%5D%5Bv%5D=remake&order=date-a"
DEFAULT_TIMEOUT = 15
DEFAULT_ATTEMPTS = 3
DEFAULT_WAIT_TIME = 15.0

# Logger config
FORMAT = "[%(filename)s | %(funcName)s : %(lineno)s] %(levelname)s: %(message)s"

# REGEX configs
# removes text inside parenthesis and brackets
REMOVE_DELIMITERS_REGEX = r'\[.*?\]|\(.*?\)'
QUALITY_REGEX = r'(\d{3,4}p)'  # matches quality, e.g. 720p
SEQUENCE_REGEX = r'\[([A-Z0-9]{8})\]'  # matches torrent sequence
# mathces sequence of 2-4 numbers followed by space
EPISODE_REGEX = r'[0-9]{2,4}(?=\s)'
SEASON_REGEX = r'S([0-9]{2})E'
BRACKETS_REGEX = r'\{[^{}]*\}|\([^\[\]]*\)'

# PARSER configs
PREFERENCE_RAWS = ["[Erai-raws]", "[SubsPlease]"]
DESIRED_SUBS = "eng"
MEMBER_CUT = 20000
PATH_ID_MEMBER_MAP = "misc/mal_id_member_count.json"
# this is for episode_number logic
NOT_ALLOWED_CHARACTERS = ["x", "."]
# reserved characters remap (for table names)
RESERVED_CHARACTERS_REMAP = {
    ":": "_" * 2,
    "!": "_" * 3
}
MAX_LINES_PER_EPISODE = 600
