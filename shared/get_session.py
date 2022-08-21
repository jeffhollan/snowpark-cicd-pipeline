from shared.snowflake_connection import SnowflakeConnection
from snowflake.snowpark import Session
import os


def session() -> Session:

    # if running in snowflake
    if SnowflakeConnection().connection:
        session = SnowflakeConnection().connection
    # if running locally with a config file
    elif os.path.exists('../config.py') or os.path.exists('config.py'):
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
    return session
