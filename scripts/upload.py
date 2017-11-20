from __future__ import division, print_function
import os, json, sys
import rethinkdb as r
from pdb import set_trace
from collections import defaultdict
from utils.checkFileSeen import ensure_sacra_file_seen
from utils.tables_primary_keys import tables_primary_keys
from utils.connect import connect
from utils.parsers import parse_sacra_json
from utils.validate import check_json_for_orphans
import logging

logger = logging.getLogger(__name__)

def upload(database, filename, preview, **kwargs):
    rdb = connect(database)

    ## check a file has been seen before - if novel, then cannot modify the DB
    seen = ensure_sacra_file_seen(filename, preview)
    if not seen and not preview:
        preview = True

    sacra_json = parse_sacra_json(filename)

    check_json_for_orphans(sacra_json)
    logger.warn("TO DO: check dbinfo[0][pathogen] == database name")
    logger.warn("TO DO: modifications -> markdown function")

    for table_name, rows in sacra_json.iteritems():
        if table_name in tables_primary_keys and table_name != "dbinfo":
            modify_db(rdb, table_name, rows, tables_primary_keys[table_name], preview, **kwargs)

def modify_db(rdb, table_name, rows, pkey, preview, **kwargs):
    if preview:
        logger.info("Previewing modifications to table {}".format(table_name))
    else:
        logger.info("Modifying table \"{}\"".format(table_name))

    ## 1: what fields will be added to the table?

    ## 2: seperate rows (the information to update the DB with) into update and insert portions
    primary_keys_sacra = [x[pkey] for x in rows]
    primary_keys_db = set(rdb.table(table_name).get_all(*primary_keys_sacra).get_field(pkey).run())
    updates = filter(lambda x: x[pkey] in primary_keys_db, rows)
    inserts = filter(lambda x: x[pkey] not in primary_keys_db, rows)

    ## 3: perform inserts
    logger.debug("Inserting (creating) {} new rows (table: {})".format(len(inserts), table_name))
    for i in inserts:
        print("\t{}".format(i[pkey]))
    if not preview:
        rdb.table(table_name).insert(inserts).run()

    ## 4: for updates:
    updates_db_dict = {x[pkey]:x for x in rdb.table(table_name).get_all(*[x[pkey] for x in updates]).coerce_to('array').run()}
    updates_sacra_dict = {x[pkey]:x for x in updates}
    ## NB these dictionaries have the same keys

    ## 4a: are rows unchanged (then no-op)
    identicals = filter(lambda x: updates_db_dict[x] == updates_sacra_dict[x], updates_db_dict.keys())
    logger.debug("Skipping {} rows as they are identical in the input & the DB".format(len(identicals)))
    for i in identicals:
        print("\t{}: {}".format(pkey, i))
    for i in identicals:
        updates_sacra_dict.pop(i, None)
        updates_db_dict.pop(i, None)

    ## 4b: for other rows, what fields are changing (op)
    updates_to_do = defaultdict(dict)
    logger.debug("Updating (modifying) {} rows".format(len(updates_sacra_dict.keys())))
    for row_key in updates_sacra_dict.keys():
        print("\t{}: {}".format(pkey, row_key))
        for (k, v) in updates_sacra_dict[row_key].iteritems():
            if k in updates_db_dict[row_key] and v == updates_db_dict[row_key][k]: continue
            if k in updates_db_dict[row_key]:
                print("\t\t{}: {} -> {}".format(k, updates_db_dict[row_key][k], v))
            else:
                print("\t\t{}: (not present) -> {}".format(k, v))
            updates_to_do[row_key][k] = v

    if not preview:
        for row_key in updates_to_do:
            rdb.table(table_name).get(row_key).update(updates_to_do[row_key]).run()
