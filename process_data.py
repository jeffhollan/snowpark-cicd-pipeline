#!/usr/bin/env python
# coding: utf-8

# In[2]:


from snowflake.snowpark import Session, udf
from snowflake.snowpark.functions import *
import os
from snowflake_connection import SnowflakeConnection

# if running in snowflake
if SnowflakeConnection().connection:
    session = SnowflakeConnection().connection
# if running locally with a config file
elif os.path.exists('config.py'):
    from config import snowpark_config
    session = Session.builder.configs(snowpark_config).create()
else:
    connection_parameters = {
        "account": os.environ["snowflake_account"],
        "user": os.environ["snowflake_user"],
        "password": os.environ["snowflake_password"],
        "role": os.environ["snowflake_user_role"],
        "warehouse": os.environ["snowflake_warehouse"],
        "database": os.environ["snowflake_database"],
        "schema": os.environ["snowflake_schema"]
    }
    session = Session.builder.configs(connection_parameters).create()


# In[ ]:


articles_df = session.table('articles')


# In[ ]:


articles_df = articles_df.select(col('author'), col('claps'), col('title'), col('reading_time'))


# In[ ]:


# making this a method so I can unit test the logic
def convert_claps_to_int(input_df):
    # standardize claps
    replacedClaps = when(col('claps').like('%K'), regexp_replace(col('claps'), 'K', '').cast('DOUBLE')*1000).otherwise(col('claps').cast('DOUBLE'))
    return input_df.select('author', replacedClaps.cast('INTEGER').alias('claps'), 'title', 'reading_time')

df = convert_claps_to_int(articles_df)


# In[ ]:


# making this a method so I can unit test the logic
def get_each_author_most_popular_article(input_df):
    # get the article with the most claps for each author
    max_df = input_df.groupBy('author').agg(max('claps').alias('claps'))
    # join back in the title and reading time, and remove duplicates
    return max_df.join(df, ['author', 'claps']).distinct()

output = get_each_author_most_popular_article(df)


# In[ ]:


# update table
output.write.save_as_table('top_articles_by_author', mode='overwrite')

