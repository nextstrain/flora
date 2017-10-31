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
parser.add_argument('--everything', default=False, action="store_true", help="Download everything (output can only JSON)")
parser.add_argument('--sequences', default=False, action="store_true", help="Download sequence data only")
parser.add_argument('--filename', '--file', '-f', required=True, help="Output filename (type is inferred from suffix)")
parser.add_argument('--locus', default=None, help="Sequence locus to download (only used with --sequences)")
parser.add_argument('--lineage', default=None, help="Sequence lineage to download (only used with --sequences)")
# parser.add_argument('--resolve', default="random", help="How to resolve duplicates (default: 'random')")
# parser.add_argument("--verbose", "-v", action="store_const", dest="loglevel", const=logging.INFO, help="Enable verbose logging")
parser.add_argument("--debug", "-d", action="store_const", dest="loglevel", const=logging.DEBUG, help="Enable debugging logging")

def write_json(data, fname):
    logger = logging.getLogger(__name__)
    with open(fname, 'w') as f:
        json.dump(data, f, indent=2)
        logger.info("Successfully saved all data to {}".format(fname))

def infer_ftype(fname):
    logger = logging.getLogger(__name__)
    if (args.filename.endswith(".fasta")):
        return "fasta"
    elif (args.filename.endswith(".json")):
        return "json"
    logger.error("Unknown output filetype. Fatal.")
    sys.exit(2)

def download_all_tables(db):
    logger = logging.getLogger(__name__)
    tableNames = r.db(dbname).table_list().run()
    tables = {}
    for tableName in tableNames:
        tables[tableName] = list(db.table(tableName).run())
    logger.info("Successfully downloaded all tables")
    return tables

def add_filter_to_query(query, filterName, filterValue):
    logger = logging.getLogger(__name__)
    if type(filterValue) is str:
        logger.info("Filtering to {} {}".format(filterName, filterValue))
        query.filter(lambda r: r[filterName] == filterValue)
    elif type(filterValue) is list:
        logger.info("Filtering to {} {}".format(filterName, filterValue))
        query.filter(lambda r: r[filterName] in filterValue)
    else:
        logger.info("Skpping filtering of {} - incorrect type".format(filterName))



def download_tables_via_query(r, db, tableNames, locus, lineage, **kwargs):
    logger = logging.getLogger(__name__)
    # acc_strain_map = {pair[0]:pair[1] for row in db.table("pathogens").pluck('strain', 'accessions').run() for pair in [[acc, row['strain']] for acc in row['accessions']]}

    ### join the sequences and isolates tables on "accession" - will ignore rows without matches
    query = db.table('sequences').inner_join(db.table('pathogens'), lambda srow, prow: prow['accessions'].contains(srow['accession'])).zip()
    if locus:
        add_filter_to_query(query, 'locus', locus)
    if lineage:
        add_filter_to_query(query, 'lineage', lineage)

    return list(query.run())


if __name__=="__main__":
    args = parser.parse_args()
    if not args.loglevel: args.loglevel = logging.INFO
    logging.basicConfig(level=args.loglevel, format='%(asctime)-15s %(message)s')
    logger = logging.getLogger(__name__)

    conn = r.connect("localhost", 28015).repl()
    dbname = "test"

    ## DOWNLOAD ALL ##
    if args.everything:
        assert(infer_ftype(args.filename) == "json")
        tables = download_all_tables(r.db(dbname))
        write_json(tables, args.filename)

    ## DOWNLOAD SEQUENCES ##
    if args.sequences:
        data = download_data_via_query(r=r, db=r.db(dbname), tableNames=["sequences", "pathogens"], **vars(args))
        if infer_ftype(args.filename) == "fasta":
            write_fasta(data, args.filename)
        elif infer_ftype(args.filename) == "json":
            write_json(data, args.filename)
