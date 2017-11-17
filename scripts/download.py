from __future__ import division, print_function
import os, json, sys
import rethinkdb as r
from pdb import set_trace
from collections import defaultdict
import logging
import random
from utils.connect import connect


#     ## DOWNLOAD ALL ##
#     if args.everything:
#         assert(infer_ftype(args.filename) == "json")
#         tables = download_all_tables(r.db(dbname))
#         write_json(tables, args.filename)
#
#     ## DOWNLOAD SEQUENCES ##
#     if args.sequences:
#         tableNames = ["sequences", "pathogens"]
#         data = download_data_via_query(r=r, db=r.db(dbname), tableNames=tableNames, **vars(args))
#         if infer_ftype(args.filename) == "fasta":
#             # store this somewhere else... command line arg?
#             fasta_headers = ['strain', 'accession', 'date', 'country', 'division', 'authors', 'url', 'title', 'journal', 'paper_url']
#             write_fasta(data=data, headers=fasta_headers, filename=args.filename)
#         elif infer_ftype(args.filename) == "json":
#             tables = data_to_tables(data=data, db=r.db(dbname), tableNames=tableNames)
#             write_json(tables, args.filename)

def download(database, dbdump, outformat, filename, resolve_method, subtype, locus, **kwargs):
    rdb = connect(database)
    logger = logging.getLogger(__name__)

    if not outformat:
        outformat = infer_ftype(filename)

    ## some basic checks
    if dbdump:
        if outformat != "json":
            logger.error("Filtype must be json to save entire DB")
            sys.exit(2)

    data = download_from_db(rdb, dbdump, locus, subtype, resolve_method, **kwargs)
    write_data(outformat, filename, data)

def write_data(outformat, filename, data):
    logger = logging.getLogger(__name__)
    f = open(filename, 'w')
    if outformat is "json":
        json.dump(data, f, indent=2)
        logger.info("Successfully saved data to {} ({} format)".format(filename, outformat))

    f.close()




def write_json(data, fname):
    logger = logging.getLogger(__name__)
    with open(fname, 'w') as f:
        json.dump(data, f, indent=2)
        logger.info("Successfully saved all data to {}".format(fname))

def infer_ftype(fname):
    logger = logging.getLogger(__name__)
    if (fname.endswith(".fasta")):
        return "fasta"
    elif (fname.endswith(".json")):
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
        return query.filter(lambda r: r[filterName] == filterValue)
    elif type(filterValue) is list:
        logger.info("Filtering to {} {}".format(filterName, filterValue))
        return query.filter(lambda r: r[filterName] in filterValue)
    else:
        logger.info("Skpping filtering of {} - incorrect type".format(filterName))
        return query

def resolve_duplicates(data, resolve_method):
    '''
    Finds strains with multiple sequences
    Removes all but one of these (from data)
    '''
    logger.info("to do: when resolving need to collapse acessions field in pathogens table")

    duplicated_strains = defaultdict(list)
    idxs_to_drop = []
    for idx, row in enumerate(data):
        duplicated_strains[row['strain']].append(idx)
    duplicated_strains = {k: v for k, v in duplicated_strains.iteritems() if len(v) > 1}

    if duplicated_strains == {}:
        logger.info("No duplicated isolates in data")
        return data

    if resolve_method == "random":
        for k, idxs in duplicated_strains.iteritems():
            keep_idx = random.choice(idxs)
            idxs_to_drop.extend([x for x in idxs if x != keep_idx])
    else:
        logger.info("Skpping removal of duplicates - unknown resolve_method")
        return data

    count = 0
    for i in sorted(idxs_to_drop, reverse=True):
        del data[i]
        count += 1
    logger.info("Removed {} duplicated sequences using method \"{}\"".format(count, resolve_method))
    return data


def download_from_db(rdb, dbdump, locus, subtype, resolve_method, **kwargs):
    """ download data and keep in the tables format (i.e. don't merge)
    and then either write each row to FASTA or separate into tables and write to JSON.
    Why join then separate? It ensures removal of unwanted rows (e.g. isolates without sequences, references without strains...)
    Note that rows *may* be duplicated if, for example, we don't resolve duplicates

    to explore: filter the sequences before the inner join
    """
    logger = logging.getLogger(__name__)
    table_names = r.table_list().run()
    expected_table_names = ["dbinfo", "strains", "samples", "sequences", "attributions"]
    assert(len(set(expected_table_names) - set(table_names)) == 0)
    if set(table_names) - set(expected_table_names):
        logger.info("The DB contains these (unexpected) tables: {}".format([x for x in table_names if x not in expected_table_names]))

    ## 1. strains table (filter on subtype)
    q = {}
    q["strains"] = rdb.table("strains")
    if subtype is not None:
        logger.debug("Filtering strains table to subtype {}".format(subtype))
        q["strains"] = q["strains"].filter({'subtype': subtype})

    q["samples"] = rdb.table("samples")

    q["sequences"] = rdb.table("sequences")
    if locus is not None:
        logger.debug("Filtering sequences table to locus {}".format(locus))
        q["sequences"] = q["sequences"].filter({'sequence_locus': locus})

    q["attributions"] = rdb.table("attributions")
    q["dbinfo"] = rdb.table("dbinfo")

    j = {}
    for name, query in q.iteritems():
        j[name] = query.coerce_to('array').run()
    return j

    #
    # ## Strains
    # rdb.
    #
    #
    # ### join the sequences and isolates tables on "accession" - will ignore rows without matches
    # query = db.table('sequences').inner_join(db.table('pathogens'), lambda srow, prow: prow['accessions'].contains(srow['accession'])).zip()
    # if locus:
    #     query = add_filter_to_query(query, 'locus', locus)
    # if lineage:
    #     query = add_filter_to_query(query, 'lineage', lineage)
    #
    # logger.info("to do: merge in publication data")
    #
    # data = list(query.run())
    # data = resolve_duplicates(data, resolve_method)
    #
    # return data

# def download_from_db(rdb, locus, subtype, resolve_method, **kwargs):
#     """
#     The approach taken here is to merge / join into a single table
#     and then either write each row to FASTA or separate into tables and write to JSON.
#     Why join then separate? It ensures removal of unwanted rows (e.g. isolates without sequences, references without strains...)
#     Note that rows *may* be duplicated if, for example, we don't resolve duplicates
#
#     to explore: filter the sequences before the inner join
#     """
#     logger = logging.getLogger(__name__)
#     # acc_strain_map = {pair[0]:pair[1] for row in db.table("pathogens").pluck('strain', 'accessions').run() for pair in [[acc, row['strain']] for acc in row['accessions']]}
#
#     ### join the sequences and isolates tables on "accession" - will ignore rows without matches
#     query = db.table('sequences').inner_join(db.table('pathogens'), lambda srow, prow: prow['accessions'].contains(srow['accession'])).zip()
#     if locus:
#         query = add_filter_to_query(query, 'locus', locus)
#     if lineage:
#         query = add_filter_to_query(query, 'lineage', lineage)
#
#     logger.info("to do: merge in publication data")
#
#     data = list(query.run())
#     data = resolve_duplicates(data, resolve_method)
#
#     return data

def data_to_tables(data, db, tableNames):
    """
    note that rows don't have to be consistent in tables format - undefined / null values should be skipped
    e.g. a row may have "age" and the subsequent row may not
    """
    tables = defaultdict(list)
    for tableName in tableNames:
        fieldNames = db.table(tableName)[0].run().keys()
        for row in data:
            tables[tableName].append({fieldName: row[fieldName] for fieldName in fieldNames if fieldName in row and row[fieldName] is not None})
    return tables

def write_fasta(data, headers, filename):
    """
    the data already arrives in rows - our task is to simply write these as FASTA entries...
    """
    missingCounts = [0 for x in headers]
    def getEntry(row, idx, name):
        if name in row and row[name] is not None:
            return row[name]
        missingCounts[idx] += 1
        return ''
    logger = logging.getLogger(__name__)
    with open(filename, 'w') as f:
        for row in data:
            f.write(">"+'|'.join([getEntry(row, i, x) for i, x in enumerate(headers)])+"\n")
            f.write(row['sequence'])
            f.write("\n")
    logger.info("FASTA file written")
    for i, n in enumerate(headers):
        if missingCounts[i] != 0:
            logger.debug("{}/{} samples were missing {}".format(missingCounts[i], len(data), n))
