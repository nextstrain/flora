from __future__ import division, print_function
import argparse
import logging
from createDropTables import createDropTables
from upload import upload

if __name__=="__main__":

    parser = argparse.ArgumentParser(description="Interface (API) for the nextstrain databases.")

    # parser.add_argument("--verbose", action="store_const", dest="loglevel", const=logging.INFO, help="Enable verbose logging")
    parser.add_argument("--debug", action="store_const", dest="loglevel", const=logging.DEBUG, help="Enable debugging logging")
    # parser.add_argument("--dryrun", action="store_true", help="Perform a dryrun without uploading or downloading any files")
    parser.add_argument("--database", "--db", type=str, default="test", help="Name of the database (the pathogen name) [default: test]")

    subparsers = parser.add_subparsers(dest="cmd")

    parser_createTables = subparsers.add_parser("createTables", help="Create tables in an (already existing) database")
    parser_createTables.add_argument("--tables", default=None, help="tables to be created. Default: the normal set. TO DO: how to specify syntax here.")
    parser_createTables.set_defaults(func=createDropTables)

    parser_createTables = subparsers.add_parser("clearTables", help="Create tables in an (already existing) database")
    parser_createTables.add_argument("--tables", default=None, help="tables to be cleared. Default: all in the DB. TO DO: specify cmd line syntax")
    parser_createTables.set_defaults(func=createDropTables)

    parser_createTables = subparsers.add_parser("upload", help="Upload data. NOTE: currently this must be an already cleaned JSON. TO DO.")
    parser_createTables.add_argument("--filename", "-f", required=True, help="sacra cleaned JSON file")
    parser_createTables.add_argument("--preview", action="store_true", help="preview changes (don't modify DB)")
    parser_createTables.set_defaults(func=upload)

    args = parser.parse_args()

    if not args.loglevel:
        args.loglevel = logging.INFO

    logging.basicConfig(level=args.loglevel, format='%(asctime)-15s %(levelname)s %(message)s')

    args.func(**vars(args))
