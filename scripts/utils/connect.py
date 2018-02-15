from __future__ import print_function, division
import rethinkdb as r
import os, sys
import logging
from rethink_io import rethink_io

""" https://www.rethinkdb.com/docs/guide/python/ """

def connect(database, local):
    logger = logging.getLogger(__name__)

    try:
        if local:
            r.connect("localhost", 28015).repl()
        else:
            host, auth_key = get_host_and_key()
            print(host, auth_key)
            r.connect(host, 28015, auth_key=auth_key).repl()
    except:
        logger.fatal("Error connecting to rethinkDB: 1")

    try:
        if database:
            rdb = r.db(database)
        else:
            rdb = r
    except:
        logger.fatal("Error connecting to rethinkDB: 2")
        os.exit(2)
    return rdb

def get_host_and_key():
    '''
    Assign rethink_host, auth_key, return as tuple
    '''
    try:
        rethink_host = os.environ['RETHINK_HOST']
    except:
        logger.critical("Missing rethink host - is $RETHINK_HOST set?")
    auth_key = None
    try:
        auth_key = os.environ['RETHINK_AUTH_KEY']
    except:
        logger.critical("Missing rethink auth_key")
    return rethink_host, auth_key

# def connect(database, rethink_host, auth_key, local):
#     logger = logging.getLogger(__name__)
#     try:
#         my_rethink_io = rethink_io()
#         rethink_host, auth_key = my_rethink_io.assign_rethink(rethink_host, auth_key, local)
#         if database:
#             my_rethink_io.connect_rethink(database, rethink_host, auth_key)
#             # my_rethink_io.check_table_exists(database, "dbinfo")
#             # my_rethink_io.check_table_exists(database, sequences_table)
#         else:
#             print('THISTHISTHIS')
#             print(rethink_host)
#             print(database)
#             my_rethink_io = r.connect(host=rethink_host, port=28015, db=database).repl()
#             my_rethink_io = r
#             print('madeithere')
#     except:
#         logger.fatal("Error connecting to rethinkDB")
#         os.exit(2)
#     return my_rethink_io
