# standard libraries
import logging, json, os

# third-party libraries
from sqlalchemy import create_engine


# Initializing settings and global variables

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

DB_CACHE_PATH_ELEMS = ENV['DB_CACHE_PATH']
DB_CACHE_PATH_STR = '/'.join(DB_CACHE_PATH_ELEMS)
ENGINE = create_engine(f'sqlite:///{DB_CACHE_PATH_STR}')


# Functions

def init_db() -> None:
    try:
        conn = ENGINE.connect()
        conn.close()
        logger.info(f'Created or connected to {DB_CACHE_PATH_STR} database')
    except:
        logger.error(f'Unable to create or connect to {DB_CACHE_PATH_STR} database')


def create_table(table_name: str, create_statement: str) -> None:
    conn = ENGINE.connect()
    drop_statement = f'''DROP TABLE IF EXISTS '{table_name}';'''
    conn.execute(drop_statement)
    conn.execute(create_statement)
    logger.info(f'Created table {table_name} in {DB_CACHE_PATH_STR}')
    conn.close()


def set_up_database() -> None:
    init_db()

    request_create_statement = '''
        CREATE TABLE 'request' (
            'request_id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            'request_url' TEXT NOT NULL UNIQUE,
            'response' BLOB NOT NULL,
            'timestamp' TEXT NOT NULL
        );
    '''
    create_table('request', request_create_statement)


# Main Program

if __name__ == '__main__':
    set_up_database()