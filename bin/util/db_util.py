import psycopg2 as pg
import logging

def get_db_cursor(host, user, password=None, port=None, dbname=None):
    logger = logging.getLogger('root')
    dsn = "host=%s user=%s" % (host, user)

    dbParams = {
        'password': password,
        'port': port,
        'dbname': dbname
    }

    for param in dbParams.keys():
        if dbParams[param] is not None:
            dsn += " %s=%s " % (param, dbParams[param])

    try:
        conn = pg.connect("%s" % dsn)
    except:
        logger.error("Failed to connect to db with dsn: %s\nERROR: %s" % (dsn, str(e)))


    try:
        return conn.cursor()
    except:
        logger.error("Failed to get cursor: %s" % (str(e)))
        raise


def select_sql(curs, sql, params=None, fetchone=False):
    logger = logging.getLogger('root')
    try:
        curs.execute(sql, params)
    except:
        logger.error("Failed selectSql: %s" % (curs.mogrify(sql, params)))
        # connection should be closed on failure
        curs.connection.close()
        raise

    if fetchone:
        return curs.fetchone()
    else:
        return curs.fetchall()


def run_sql(curs, sql, params=None, debug=False):
    logger = logging.getLogger('root')

    if debug:
        logger.debug("params = " + params)
        logger.debug("\nDEBUG: PRINTING SQL: %s" % (curs.mogrify(sql, params),))
    else:
        try:
            # print "sql = " + sql
            curs.execute(sql, params)
        except:
            logger.error("Failed to excecute sql %s" % curs.mogrify(sql, params))
            curs.connection.close()
            raise
        
        curs.connection.commit()


def make_db_conn(host=None, user=None, password=None, port=None, dbname=None, dsnString=None):
    logger = logging.getLogger('root')
    if dsnString is not None:
        dsn = dsnString
    else:
        dsn = "host=%s user=%s" % (host, user)

        dbParams = {
            'password': password,
            'port': port,
            'dbname': dbname
        }

        for param in dbParams.keys():
            if dbParams[param] is None:
                continue
            else:
                dsn = dsn + " %s=%s" % (param, dbParams[param])

    try:
        conn = pg.connect("%s" % (dsn))
        return conn
    except:
        logger.error("Failed to connect to db with dsn: %s" % dsn)
        raise
