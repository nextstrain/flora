from __future__ import division, print_function
import os, json, sys
import rethinkdb as r
from pdb import set_trace
from collections import defaultdict
import logging
import random
from utils.connect import connect
from utils.fasta import headers

# local globals
expected_table_names = ["dbinfo", "strains", "samples", "sequences", "attributions"]
logger = logging.getLogger(__name__)


def download(database, dbdump, outformat, filename, resolve_method, subtype, segment, locus, local, **kwargs):
    rdb = connect(database, local)
    if not outformat: outformat = infer_ftype(filename)
    if dbdump:
        if outformat != "json":
            logger.error("Filtype must be json to save entire DB")
            sys.exit(2)
        data = download_db(rdb, locus, subtype, segment, **kwargs)
        write_json(data, filename)
    else:
        data = download_join(rdb, locus, subtype, segment)
        data = resolve_duplicates(data, resolve_method) # Perhaps this can be done in the DB logic??
        if outformat == "json":
            write_json(recreate_tables(rdb=rdb, data=data), filename)
        elif outformat == "fasta":
            try:
                logger.debug("using headers from {}".format(database))
                headers_to_use = headers[database]
            except KeyError:
                logger.info("using default fasta headers as no entry for {}".format(database))
                headers_to_use = headers["default"]
            write_fasta(data=data, headers=headers_to_use, filename=filename)

def write_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    logger.info("Successfully saved data to {}".format(filename))

def infer_ftype(fname):
    if (fname.endswith(".fasta")):
        return "fasta"
    elif (fname.endswith(".json")):
        return "json"
    logger.error("Unknown output filetype. Fatal.")
    sys.exit(2)

def add_filter_to_query(query, filterName, filterValue):
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
    ''' Finds strains (strain_id) with multiple samples / sequences
    Removes all but one of these.
    TO DO: this method must be able to filter based on samples and sequences.
    '''
    # create duplicated_strains structure: all entries with multiple "strain_id"s
    duplicated_strains = defaultdict(list)
    for idx, row in enumerate(data):
        if 'segment' in row:
            duplicated_strains[row['strain_id']+row['segment']].append(idx)
        else:
            duplicated_strains[row['strain_id']].append(idx)
    duplicated_strains = {k: v for k, v in duplicated_strains.iteritems() if len(v) > 1}
    if duplicated_strains == {}:
        logger.info("No duplicated isolates in data :)")
        return data

    ## different methods all populate the idxs_to_drop array.
    idxs_to_drop = []
    if resolve_method == "random":
        for k, idxs in duplicated_strains.iteritems():
            keep_idx = random.choice(idxs)
            idxs_to_drop.extend([x for x in idxs if x != keep_idx])
    else:
        logger.warn("Skipping removal of duplicates - {} is an unknown resolve_method".format(resolve_method))
        return data

    ## drop indexes from idxs_to_drop
    for i in sorted(idxs_to_drop, reverse=True):
        del data[i]
    logger.info("Removed {} duplicated sequences using method \"{}\"".format(len(idxs_to_drop), resolve_method))
    return data

def _check_table_names(rdb):
    table_names = rdb.table_list().run()
    assert(len(set(expected_table_names) - set(table_names)) == 0)
    if set(table_names) - set(expected_table_names):
        logger.info("The DB contains these (unexpected) tables: {}. They will be ignored!".format([x for x in table_names if x not in expected_table_names]))

def download_db(rdb, locus, subtype, segment, **kwargs):
    """ download data and keep in the tables format (i.e. don't merge)
    locus and subtype filtering are applied
    """
    _check_table_names(rdb)
    q = {x: rdb.table(x) for x in expected_table_names}
    if subtype is not None:  q["strains"] = add_filter_to_query(q["strains"], 'subtype', subtype)
    if locus   is not None:  q["sequences"] = add_filter_to_query(q["strains"], 'sequence_locus', locus)
    if segment is not None:  q["sequences"] = add_filter_to_query(q["sequences"], 'segment', segment)
    return {name: query.coerce_to('array').run() for name, query in q.iteritems()}

def download_join(rdb, locus, subtype, segment):
    query = rdb.table("sequences")
    if locus is not None:
        query = add_filter_to_query(query, 'sequence_locus', locus)
    query = query.eq_join("sample_id", rdb.table("samples")).zip()
    query = query.eq_join("strain_id", rdb.table("strains")).zip()
    if subtype is not None:
        query = add_filter_to_query(query, 'subtype', subtype)
    if segment is not None:
        query = add_filter_to_query(query, 'segment', segment)
    query = query.coerce_to("array")
    data = query.run()
    ## we must do attributions seperately as the table may be empty (cannot do map_concat etc), then merge in python
    attributions = {x["attribution_id"]: x for x in rdb.table("attributions").coerce_to("array").run()}
    for i in data:
        if "attribution_id" in i.keys() and i["attribution_id"] in attributions:
            for k, v in attributions[i["attribution_id"]].iteritems():
                if k not in i:
                    i[k] = v
    logger.info("DB query returned {} entries".format(len(data)))
    return data

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

def write_fasta(data, headers, filename, join_char = '|'):
    """
    the data already arrives in rows - our task is to simply write these as FASTA entries...
    """
    missingCounts = [0 for x in headers]
    def _getEntry(row, idx, name):
        if name in row and row[name] is not None:
            return row[name].replace(join_char, '//') ## not sure about this
        missingCounts[idx] += 1
        return '?'
    with open(filename, 'w+') as f:
        for row in data:
            f.write(">"+'|'.join([_getEntry(row, i, x) for i, x in enumerate(headers)])+"\n")
            f.write(row['sequence'])
            f.write("\n")
    logger.info("FASTA file written")
    for i, n in enumerate(headers):
        if missingCounts[i] != 0:
            logger.debug("{}/{} samples were missing {}".format(missingCounts[i], len(data), n))

def _extract_fields(data, fields):
    return [{field: (line[field] if field in line else None) for field in fields} for line in data]

def recreate_tables(rdb, data):
    tables_fields = {name: rdb.table(name).map(lambda doc: doc.keys()).reduce(lambda uniq, doc: uniq.set_union(doc)).distinct().run() for name in expected_table_names if name is not "dbinfo"}
    tables = {name: _extract_fields(data, fields) for name, fields in tables_fields.iteritems()}
    tables["dbinfo"] = rdb.table("dbinfo").coerce_to("array").run()
    return tables
