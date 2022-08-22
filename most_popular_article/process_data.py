#!/usr/bin/env python
# coding: utf-8

from snowflake.snowpark import Session, udf, DataFrame
from snowflake.snowpark.functions import *
from shared import get_session

def notebook(session):
    articles_df = session.table('articles')
    articles_df = articles_df.select(col('author'), col('claps'), col('title'), col('reading_time'))
    df = convert_claps_to_int(articles_df)
    df = minmax_time(df)
    output = get_each_author_most_popular_article(df)
    output.write.save_as_table('top_articles_by_author', mode='overwrite')
    return output

# making this a method so I can unit test the logic
def convert_claps_to_int(input_df):
    # standardize claps
    replacedClaps = when(col('claps').like('%K'), regexp_replace(col('claps'), 'K', '').cast('DOUBLE')*1000).otherwise(col('claps').cast('DOUBLE'))
    return input_df.select('author', replacedClaps.cast('INTEGER').alias('claps'), 'title', 'reading_time')


# making this a method so I can unit test the logic
def get_each_author_most_popular_article(input_df):
    # get the article with the most claps for each author
    max_df = input_df.groupBy('author').agg(max('claps').alias('claps'))
    # join back in the title and reading time, and remove duplicates
    return max_df.join(input_df, ['author', 'claps']).distinct()

@udf
def minmax(maxtime: int, mintime: int, value: int) -> float:
    return (value - mintime) / (maxtime - mintime)

def minmax_time(input_df: DataFrame) -> DataFrame:
    maxtime = input_df.agg(max(col('reading_time'))).collect()[0][0]
    mintime = input_df.agg(min(col('reading_time'))).collect()[0][0]
    return input_df.select('author', 'claps', 'title', minmax(lit(maxtime), lit(mintime), col('reading_time')).alias('reading_time'))