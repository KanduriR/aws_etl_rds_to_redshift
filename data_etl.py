import configparser
import logging
import mysql.connector as sqlconnector
import psycopg2
from sql_queries import dimension_etl_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s: %(name)s: %(funcName)s: %(lineno)d: %(levelname)s: %(message)s')
filehandler = logging.FileHandler('setting_warehouse.log')
filehandler.setFormatter(formatter)

logger.addHandler(filehandler)


def connect_to_db(host, db, user, key):
    try:
        cnx = sqlconnector.connect(user=user, password=key, host=host, db=db)
        cur = cnx.cursor()
        logger.info('Successfully connected to %s', db)

    except sqlconnector.Error as err:
        if (err.errno == sqlconnector.errorcode.ER_ACCESS_DENIED_ERROR):
            logger.error("Wrong user name or password of Database %s", db)
        else:
            logger.error('Unkown error %s connecting to Database %s', err, db)

    return cnx, cur


def connect_to_dwh(host, db, port, user, key):
    try:
        conn = psycopg2.connect(host=host, dbname=db, port=port, user=user, password=key)
        conn.autocommit = True
        cursor = conn.cursor()
        logger.info('Successfully connected to %s', db)

    except psycopg2.OperationalError as err:
        logger.error("Failed setting connection- %s", err)

    return conn, cursor

def load_schema_data(oltp_cur, olap_cur, olap_cnx):

    for query in dimension_etl_query:
        oltp_cur.execute(query[0])
        items = oltp_cur.fetchall()
        try:
            olap_cur.executemany(query[1], items)
            olap_cnx.commit()
            logger.info("Inserted data with: %s", query[1])
        except sqlconnector.Error as err:
            logger.error('Error %s Couldnt run query %s', err, query[1])

def main():

    config = configparser.ConfigParser()
    config.read('config.ini')

    db_host = config.get('sqlhost', 'host')
    user = config.get('sqlhost', 'user')
    userkey = config.get('sqlhost', 'userkey')
    oltpdb = config.get('sqlhost', 'oltp_db')

    # connect to OLTP database
    oltp_cnx, oltp_cur = connect_to_db(db_host, oltpdb, user, userkey)

    host = config.get('dwhhost', 'host')
    port = config.get('dwhhost', 'port')
    olap_db = config.get('dwhhost', 'olap_db')
    user = config.get('dwhhost', 'user')
    key = config.get('dwhhost', 'userkey')

    # connect to OLAP database
    olap_cnx, olap_cur = connect_to_dwh(host, olap_db, port, user, key)

    # run insert function copying data from one to another
    load_schema_data(oltp_cur, olap_cur, olap_cnx)

    oltp_cur.close()
    oltp_cnx.close()
    olap_cur.close()
    olap_cnx.close()


if __name__ == '__main__':
    main()
