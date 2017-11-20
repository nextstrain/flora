from __future__ import print_function, division
import rethinkdb as r
import os, sys
from utils.tables_primary_keys import tables_primary_keys
import logging
from utils.connect import connect

logger = logging.getLogger(__name__)

def createDatabase(database, **kwargs):
    r = connect(None)
    if database in r.db_list().run():
        logger.critical("Cannot create database \"{}\" - it already exists".format(database))
        sys.exit(2)
    r.db_create(database).run()
    logger.info("Created database \"{}\"".format(database))

def createDropTables(database, tables, cmd, **kwargs):
    rdb = connect(database)
    if cmd == "createTables":
        if tables == None:
            tables = tables_primary_keys
        else:
            logger.error("To Do")
            sys.exit(2)
        for table_name, primary_key in tables_primary_keys.iteritems():
            try:
                rdb.table_create(table_name, primary_key=primary_key).run()
            except r.errors.ReqlOpFailedError as e:
                if "already exists" in e.message:
                    logger.info("Failed to create table {} (it already exists)".format(table_name))
                else:
                    logger.warn("Failed to create table {}".format(table_name))
                    logger.debug(e)
            else:
                logger.info("Created table {}".format(table_name))

    if cmd == "clearTables":
        if tables == None:
            tables = rdb.table_list().run()
        else:
            logger.error("To Do")
            sys.exit(2)
        for table_name in tables:
            try:
                rdb.table(table_name).delete().run()
            except r.errors.ReqlOpFailedError as e:
                logger.warn("Failed to clear table {}".format(table_name))
                logger.debug(e)
            else:
                logger.info("Table {} now empty".format(table_name))

    if cmd == "dropTables":
        logger.error("To Do")
        sys.exit(2)
        # rdb.table_drop(table_name).run()
