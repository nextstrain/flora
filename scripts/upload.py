from __future__ import division, print_function
import os, json, sys
import rethinkdb as r
import argparse
from pdb import set_trace

parser = argparse.ArgumentParser()
parser.add_argument('--path', default='input_data/', help="Path to directory containing upload file, default is input_data/")
# parser.add_argument('--rethink_host', default=None, help="rethink host url")
# parser.add_argument('--auth_key', default=None, help="auth_key for rethink database")
# parser.add_argument('--local', default=False, action="store_true",  help ="connect to local instance of rethinkdb database")
parser.add_argument('--filename', '--file', '-f', required=True, help="file to upload to rethink database")


def parse_sacra_json(filename):
    try:
        with open(filename, 'r') as f:
            dataset = json.load(f)
        # verify the contents are consistent?
    except IOError:
        print("File not found")
        sys.exit(2)
    except KeyError:
        print("Sacra JSON not correctly formatted")
        sys.exit(2)
    return dataset

if __name__=="__main__":
    args = parser.parse_args()
    sacra = parse_sacra_json(os.path.join(args.path, args.filename))

    r.connect("localhost", 28015).repl()
    dbname = "test"


    if "sequences" in sacra:
        primary_keys = [x["accession"] for x in sacra["sequences"]]
        primary_keys_in_db = r.db(dbname).table("sequences").get_all(primary_keys).run()

        # set_trace()
        # ensure all unique

        print("uploading {} sequences".format(len(sacra["sequences"])))
        # I believe inserting is only for new rows? what happens if they have key colissions?
        # Do we need to check whats new (insert) and what exists (append)

        r.db(dbname).table("sequences").insert(sacra["sequences"]).run()

    if "viruses" in sacra:
        print("uploading {} pathogens".format(len(sacra["viruses"])))
        r.db(dbname).table("pathogens").insert(sacra["viruses"]).run()
