import configparser
import logging
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s: %(name)s: %(funcName)s: %(lineno)d: %(levelname)s: %(message)s')
file_handler = logging.FileHandler('setting_warehouse.log')
file_handler.setFormatter(formatter)
# file_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)


def create_dwh():
    config = configparser.ConfigParser()
    config.read('config.ini')

    dwh_host = config.get('dwhhost', 'host')
    user = config.get('dwhhost', 'user')
    userkey = config.get('dwhhost', 'userkey')
    db = config.get('dwhhost', 'db')
    olap_db = config.get('dwhhost', 'olap_db')

    try:
        cnx = psycopg2.connect(host=dwh_host, dbname=db, user=user, password=userkey, port=5439)
        cnx.autocommit = True
        cursor = cnx.cursor()
        logger.info('Database connection successful')

        # create database
        cursor.execute("DROP DATABASE %s" % olap_db)
        cursor.execute("CREATE DATABASE %s;" % olap_db)
        logging.info('Database %s has been created', olap_db)
        # close connection to db instance
        cnx.close()

    except psycopg2.OperationalError as err:
        logger.error("Failed setting connection- %s", err)

    # connect to the created database
    cnx = psycopg2.connect(host=dwh_host, dbname=olap_db, user=user, password=userkey, port=5439)
    cursor = cnx.cursor()
    return (cnx, cursor)


def drop_dwh_tables(conn, cursor):

    for query in drop_table_queries:
        try:
            cursor.execute(query)
            conn.commit()

        except psycopg2.OperationalError as err:
            logger.error('Err %d: Failed dropping table %s', err, query)


def create_dwh_tables(conn, cursor):

    for query in create_table_queries:
        try:
            cursor.execute(query)
            conn.commit()
            logger.info('Ran query: % s', query)

        except psycopg2.OperationalError as err:
            logger.error('Err %d: Failed creating tables %s', err, query)


def main():

    (conn, cur) = create_dwh()

    drop_dwh_tables(conn, cur)
    create_dwh_tables(conn, cur)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
