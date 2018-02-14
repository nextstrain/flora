from __future__ import print_function, division
import rethinkdb as r
import os, sys
import logging
from rethink_io import rethink_io

""" https://www.rethinkdb.com/docs/guide/python/ """

def old_connect(database):
    logger = logging.getLogger(__name__)
    try:
        r.connect("localhost", 28015).repl()
        if database:
            rdb = r.db(database)
        else:
            rdb = r
    except:
        logger.fatal("Error connecting to rethinkDB")
        os.exit(2)
    return rdb

def connect(database, rethink_host, auth_key):
    logger = logging.getLogger(__name__)
    try:
        my_rethink_io = rethink_io()
        rethink_host, auth_key = my_rethink_io.assign_rethink(rethink_host, auth_key, local=False)
        if database:
            my_rethink_io.connect_rethink(database, rethink_host, auth_key)
            my_rethink_io.check_table_exists(database, "dbinfo")
            # my_rethink_io.check_table_exists(database, sequences_table)
        else:
            print('THISTHISTHIS')
            print(rethink_host)
            print(database)
            my_rethink_io = r.connect(host=rethink_host, port=28015, db=database).repl()
            my_rethink_io = r
            print('madeithere')
    except:
        logger.fatal("Error connecting to rethinkDB")
        os.exit(2)
    return my_rethink_io
