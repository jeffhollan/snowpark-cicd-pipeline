create TABLE ARTICLES if not exists (
	AUTHOR VARCHAR(16777216),
	CLAPS VARCHAR(16777216),
	READING_TIME NUMBER(38,0),
	LINK VARCHAR(16777216),
	TITLE VARCHAR(16777216),
	TEXT VARCHAR(16777216)
) AS select AUTHOR, CLAPS, READING_TIME, LINK, TITLE, TEXT  from JEFFHOLLAN_DEMO.PUBLIC.ARTICLES sample (100 rows);

create or replace procedure most_popular_article()
    returns string
    language python
    runtime_version = '3.8'
    PACKAGES = ('snowflake-snowpark-python')
    handler = 'most_popular_article.process_data.notebook'
    imports = ('@deploy/app.zip');