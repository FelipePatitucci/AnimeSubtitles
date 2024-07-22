query_create_table = """
create table if not exists %s.%s (
	MAL_ID INTEGER, 
	EPISODE INTEGER,
	NAME VARCHAR(200), 
	QUOTE TEXT, 
	START_TIME TIME(3),
	END_TIME TIME(3)
);
"""

query_json_data = """
SELECT
	mal_id,
    completed
FROM raw_quotes.v_json_info
ORDER BY completed ASC, ep_amount ASC;
"""
