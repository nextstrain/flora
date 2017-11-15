from __future__ import print_function, division
import rethinkdb as r
import os, sys
from utils.tables_primary_keys import tables_primary_keys
import logging

# https://www.rethinkdb.com/docs/guide/python/
r.connect("localhost", 28015).repl()
dbname = "test"
# we assume that the "test" database is present (it is by default)
# in reality, this will be the pathogen name, e.g. zika / cholera

logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')
logger = logging.getLogger(__name__)

# create the tables
for table_name, primary_key in tables_primary_keys.iteritems():
    r.db(dbname).table_create(table_name, primary_key=primary_key).run()

logger.info("Created {} tables in the database \"{}\"".format(len(tables_primary_keys.keys()), dbname))

delete = False
if delete:
    for table_name, primary_key in tables_primary_keys.iteritems():
        r.db(dbname).table_drop(table_name).run()
