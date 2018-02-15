import logging
from tables_primary_keys import tables_primary_keys

def _check_linkage(tables, key, a, b):
    linked, unlinked = [], []
    ids = [x[key] for x in tables[b]]
    for row in tables[a]:
        if row[key] not in ids:
            unlinked.append(row[key])
        else:
            linked.append(row[key])
    return (linked, unlinked)

def check_json_for_orphans(tables):
    """Check that all entries in the supplied JSON are linked
    i.e. that any sequence has a corresponding sample and, in turn, a strain (or vice versa).
    Similarly for attributions.
    Note that if there are orphans this is not neccessarily an error, but the user should be warned.
    """
    logger = logging.getLogger(__name__)
    ## TO DO: this fn assumes that strains, samples & sequences are all present. This may not always be the case!

    for parent_table_name, child_table_name in [ ["strains", "samples"], ["samples", "sequences"] ]:
        if parent_table_name not in tables or child_table_name not in tables:
            logger.warn("orphan detection: skipping {} -> {} (one/both are missing from JSON)".format(parent_table_name, child_table_name))
        else:
            ## 1. check (e.g.) strain -> sample linkage (table A - B)
            ids_linked, ids_missing = _check_linkage(tables, tables_primary_keys[parent_table_name], parent_table_name, child_table_name)
            if len(ids_missing) == 0:
                logger.info("Complete (forward) linkage between tables {} & {}".format(parent_table_name, child_table_name))
            else:
                logger.warn("Incomplete (forward) linkage between tables {} & {}. These {} were unlinked: {}".format(parent_table_name, child_table_name, tables_primary_keys[parent_table_name], ids_missing))

            ## 2. check the reverse (e.g. are the sample_id's in strains all present in samples?
            ## this is made possible by the child table including the parent's primary key
            ids_linked, ids_missing = _check_linkage(tables, tables_primary_keys[parent_table_name], child_table_name, parent_table_name)
            if len(ids_missing) == 0:
                logger.info("Complete (backwards) linkage between tables {} & {}".format(child_table_name, parent_table_name))
            else:
                logger.warn("Incomplete (backwards) linkage between tables {} & {}. These {} value were unlinked: {}".format(child_table_name, parent_table_name, tables_primary_keys[parent_table_name], ids_missing))
