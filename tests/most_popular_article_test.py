import pytest
from snowflake.snowpark import Session
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DoubleType
from shared import get_session
get_session.session()
from most_popular_article import process_data

@pytest.fixture
def session() -> Session:
    return get_session.session()


def test_claps_conversion(session: Session):
    source_data = [
        ("Jon Doe", "1.2K", "My Title", 11),
        ("Jane Doe", "200", "This title", 100),
        ("Author X", "11.4K", "Title Y", 1)
    ]
    source_df = session.create_dataframe(
        source_data,
        ["author", "claps", "title", "reading_time"]
    )
    actual_df = process_data.convert_claps_to_int(source_df)
    expected_data = [
        ("Jon Doe", 1200, "My Title", 11),
        ("Jane Doe", 200, "This title", 100),
        ("Author X", 11400, "Title Y", 1)
    ]
    exepcted_df = session.create_dataframe(
        expected_data,
        ["author", "claps", "title", "reading_time"]
    )

    assert (actual_df.collect() == exepcted_df.collect())

def test_minmax_conversion(session: Session):
    

    source_data = [
        ("Jon Doe", 1200, "My Title", 50),
        ("Jane Doe", 200, "This title", 100),
        ("Author X", 11400, "Title Y", 1)
    ]
    source_df = session.create_dataframe(
        source_data,
        ["author", "claps", "title", "reading_time"]
    )
    actual_df = process_data.minmax_time(source_df)
    expected_data = [
        ("Jon Doe", 1200, "My Title", (50 - 1) / (100 - 1)),
        ("Jane Doe", 200, "This title", 1),
        ("Author X", 11400, "Title Y", 0)
    ]
    exepcted_df = session.create_dataframe(
        expected_data,
        StructType([
            StructField("author", StringType()), 
            StructField("claps", IntegerType()), 
            StructField("title", StringType()), 
            StructField("reading_time", DoubleType())])
    )

    assert (actual_df.collect() == exepcted_df.collect())