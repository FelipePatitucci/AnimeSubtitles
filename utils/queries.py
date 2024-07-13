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
