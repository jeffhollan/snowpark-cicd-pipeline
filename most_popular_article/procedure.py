from snowflake.snowpark import Session
from shared.snowflake_connection import SnowflakeConnection
import process_data


def invoke(session: Session) -> str:
    SnowflakeConnection().connection = session
    return process_data.notebook(session)._show_string()