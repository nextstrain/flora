from __future__ import division, print_function
import sys, os
import argparse
import logging
import time
from createDropTables import createDropTables, createDatabase
from upload import upload
from download import download
from utils.colorLogging import ColorizingStreamHandler

if __name__=="__main__":

    parser = argparse.ArgumentParser(description="Interface (API) for the nextstrain databases.")

    # parser.add_argument("--verbose", action="store_const", dest="loglevel", const=logging.INFO, help="Enable verbose logging")
    parser.add_argument("--debug", action="store_const", dest="loglevel", const=logging.DEBUG, help="Enable debugging logging")
    # parser.add_argument("--dryrun", action="store_true", help="Perform a dryrun without uploading or downloading any files")
    parser.add_argument('--rethink_host', default=None, help="rethink host url")
    parser.add_argument('--auth_key', default=None, help="auth_key for rethink database")
    parser.add_argument("--database", "--db", type=str, default="test", help="Name of the database (the pathogen name) [default: test]")

    subparsers = parser.add_subparsers(dest="cmd")

    parser_createDatabase = subparsers.add_parser("createDatabase", help="Create a new database")
    parser_createDatabase.set_defaults(func=createDatabase)

    parser_createTables = subparsers.add_parser("createTables", help="Create tables in an (already existing) database")
    parser_createTables.add_argument("--tables", default=None, help="tables to be created. Default: the normal set. TO DO: how to specify syntax here.")
    parser_createTables.set_defaults(func=createDropTables)

    parser_clearTables = subparsers.add_parser("clearTables", help="Clear tables in an (already existing) database")
    parser_clearTables.add_argument("--tables", default=None, help="tables to be cleared. Default: all in the DB. TO DO: specify cmd line syntax")
    parser_clearTables.set_defaults(func=createDropTables)

    parser_upload = subparsers.add_parser("upload", help="Upload data. NOTE: currently this must be an already cleaned JSON. TO DO.")
    parser_upload.add_argument("--filename", "-f", required=True, help="sacra cleaned JSON file")
    parser_upload.add_argument("--preview", action="store_true", help="preview changes (don't modify DB)")
    parser_upload.set_defaults(func=upload)

    parser_download = subparsers.add_parser("download", help="Download data")
    parser_download.add_argument('--dbdump', default=False, action="store_true", help="Download the entire DB -> JSON. Without this, there will be a 1-1 mapping between strains, samples and sequences.")
    parser_download.add_argument("--outformat", type=str, choices=['json', 'fasta'], help="output format")
    parser_download.add_argument("--filename", "-f", required=True, help="file to write")
    parser_download.add_argument('--resolve_method', default="random", help="How to resolve duplicates (default: 'random')")
    parser_download.add_argument('--subtype', default=None, help="Restrict to this subtype (works with dbdump)")
    parser_download.add_argument('--locus', default=None, help="Restrict to this locus / segment (works with dbdump)")
    parser_download.set_defaults(func=download)

    args = parser.parse_args()

    ## L O G G I N G
    # https://docs.python.org/2/howto/logging-cookbook.html#multiple-handlers-and-formatters
    root_logger = logging.getLogger('')
    root_logger.setLevel(args.loglevel if args.loglevel else logging.INFO)
    root_logger.addHandler(ColorizingStreamHandler())
    args.func(**vars(args))
