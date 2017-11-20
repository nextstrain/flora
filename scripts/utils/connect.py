from __future__ import print_function, division
import rethinkdb as r
import os, sys
import logging

""" https://www.rethinkdb.com/docs/guide/python/ """

def connect(database):
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
