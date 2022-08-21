import pytest
from most_popular_article import process_data
from snowflake.snowpark import Session
from shared import get_session


@pytest.fixture
def session():
    return get_session.session()


def test_claps_converstion():
    assert 1 == 1
    process_data.convert_claps_to_int()
