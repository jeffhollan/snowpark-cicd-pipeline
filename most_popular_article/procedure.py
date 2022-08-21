from snowflake.snowpark import Session
from snowflake_connection import SnowflakeConnection


def invoke(session: Session) -> str:
    SnowflakeConnection().connection = session
    import process_data
    return process_data.output._show_string()
