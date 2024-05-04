import psycopg2
from sqlalchemy import create_engine


def connect_db(dbname, user, password, host='localhost', port='5432'):
    """
    Connect to the PostgreSQL database.
    :param dbname: str, name of the database
    :param user: str, username
    :param password: str, password
    :param host: str, host address
    :param port: str, port number
    :return: connection object
    """
    try:
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        connection = create_engine(conn_str)
        print("Connection established successfully.")
        return connection
    except Exception as e:
        print(f"Connection could not be established. Error: {e}")
        return None
