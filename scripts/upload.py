from __future__ import division, print_function
import os, json, sys
import rethinkdb as r
import argparse
from pdb import set_trace
from collections import defaultdict
import hashlib

parser = argparse.ArgumentParser()
# parser.add_argument('--path', default='input_data/', help="Path to directory containing upload file, default is input_data/")
# parser.add_argument('--rethink_host', default=None, help="rethink host url")
# parser.add_argument('--auth_key', default=None, help="auth_key for rethink database")
# parser.add_argument('--local', default=False, action="store_true",  help ="connect to local instance of rethinkdb database")
parser.add_argument('--filename', '--file', '-f', required=True, help="file to upload to rethink database (full path)")
parser.add_argument('--change_db', default=False, action="store_true", help="perform changes to the DB (default: False)")


def ensure_sacra_file_seen(filename, change_db):
    hasher = hashlib.md5()
    with open(filename, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    md5 = hasher.hexdigest();

    print("Sacra file had hash {}".format(md5))

    with open("./.sacra_hashes", 'rU') as f:
        for line in f:
            print(line)
            if line.strip() == md5:
                print("This (sacra) file has been seen before. Proceeding.")
                return # irrelevant if we want to change the DB - we've seen this before
    ## if we're here, then the file hasn't been seen before
    if change_db:
        print("Cannot continue (with the possibility of modifying the DB) as this sacra file has not been seen before.")
        sys.exit(2)
    else:
        print("First time seeing this sarce file.")
        with open("./.sacra_hashes", 'a+') as f:
            f.write(md5 + "\n")

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

def checkEqual(iterator):
    iterator = iter(iterator)
    try:
        first = next(iterator)
    except StopIteration:
        return True
    return all(first == rest for rest in iterator)

def change_db_via_json(table, sacra, pkey, do):
    ## 1: what fields are in the json_in (and is it consistent?)\
    sacra_consistent = checkEqual([set(x.keys()) for x in sacra])
    print("Is sacra consistent? {}".format(sacra_consistent))

    ## 2 not 2: what fields will be added to the table?

    ## 2: seperate json_in into udate and upload portions
    primary_keys_sacra = [x[pkey] for x in sacra]
    primary_keys_db = set(table.get_all(*primary_keys_sacra).get_field(pkey).run())
    updates = filter(lambda x: x[pkey] in primary_keys_db, sacra)
    inserts = filter(lambda x: x[pkey] not in primary_keys_db, sacra)

    ## 3: perform inserts
    print("Inserting (creating) {} rows".format(len(inserts)))
    for i in inserts:
        print("\t{}".format(i[pkey]))
    if do:
        table.insert(inserts).run()

    ## 4: for updates:
    updates_db = table.get_all(*[x[pkey] for x in updates]).coerce_to('array').run()
    updates_db_dict = {x[pkey]:x for x in updates_db}
    updates_sacra_dict = {x[pkey]:x for x in updates}

    ## 4a: are rows unchanged (then no-op)
    identicals = filter(lambda x: updates_db_dict[x] == updates_sacra_dict[x], updates_sacra_dict.keys())
    print("Skipping {} rows as they are identical in the input & the DB".format(len(identicals)))
    for i in identicals:
        print("\t{}: {}".format(pkey, i))
    for i in identicals:
        updates_sacra_dict.pop(i, None)
        updates_db_dict.pop(i, None)

    ## 4b: for other rows, what fields are changing (op)
    updates_to_do = defaultdict(dict)
    print("Updating (modifying) {} rows".format(len(updates_sacra_dict.keys())))
    for row_key in updates_sacra_dict.keys():
        print("\t{}: {}".format(pkey, row_key))
        for (k, v) in updates_sacra_dict[row_key].iteritems():
            if v == updates_db_dict[row_key][k]: continue
            if k in updates_db_dict[row_key]:
                print("\t\t{}: {} -> {}".format(k, updates_db_dict[row_key][k], v))
            else:
                print("\t\t{}: (not present) -> {}".format(k, v))
            updates_to_do[row_key][k] = v

    if do:
        for row_key in updates_to_do:
            table.get(row_key).update(updates_to_do[row_key]).run()



if __name__=="__main__":
    args = parser.parse_args()
    sacra_filename = args.filename
    sacra = parse_sacra_json(sacra_filename)
    ensure_sacra_file_seen(sacra_filename, args.change_db)

    conn = r.connect("localhost", 28015).repl()
    dbname = "test"


    if "sequences" in sacra:
        print("TABLE: sequences")
        change_db_via_json(r.db(dbname).table("sequences"), sacra["sequences"], "accession", args.change_db)
    if "pathogens" in sacra:
        print("TABLE: pathogens")
        change_db_via_json(r.db(dbname).table("pathogens"), sacra["pathogens"], "strain", args.change_db)

    ## the other tables (geo etc) should be updated elsewhere. Authors is different.
