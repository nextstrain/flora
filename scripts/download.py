from __future__ import division, print_function
import os, json, sys
import rethinkdb as r
import argparse
from pdb import set_trace
from collections import defaultdict
# import hashlib
import logging

parser = argparse.ArgumentParser()
# parser.add_argument('--filename', '--file', '-f', required=True, help="file to write")
# parser.add_argument('--change_db', default=False, action="store_true", help="perform changes to the DB (default: False)")
parser.add_argument('--everything', default=False, action="store_true", help="Download everything (output will be JSON)")
parser.add_argument('--filename', '--file', '-f', required=True, help="Output filename")
# parser.add_argument("--verbose", "-v", action="store_const", dest="loglevel", const=logging.INFO, help="Enable verbose logging")
parser.add_argument("--debug", "-d", action="store_const", dest="loglevel", const=logging.DEBUG, help="Enable debugging logging")

def write_json(data, fname):
    logger = logging.getLogger(__name__)
    with open(fname, 'w') as f:
        json.dump(data, f, indent=2)
        logger.info("Successfully saved all data to {}".format(fname))


def download_all_tables(db):
    logger = logging.getLogger(__name__)
    tableNames = r.db(dbname).table_list().run()
    tables = {}
    for tableName in tableNames:
        tables[tableName] = list(db.table(tableName).run())
    logger.info("Successfully downloaded all tables")
    return tables

if __name__=="__main__":
    args = parser.parse_args()
    if not args.loglevel: args.loglevel = logging.INFO
    logging.basicConfig(level=args.loglevel, format='%(asctime)-15s %(message)s')
    logger = logging.getLogger(__name__)

    conn = r.connect("localhost", 28015).repl()
    dbname = "test"

    ## DOWNLOAD ALL ##
    if args.everything:
        assert(args.filename.endswith(".json"))
        tables = download_all_tables(r.db(dbname))
        write_json(tables, args.filename)


    # set_trace()


    ## the download is effectively dumps of the tables
    ## but there needs to be querying / consistency
    ## commonly used
