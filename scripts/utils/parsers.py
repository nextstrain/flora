import os, json, sys
import logging

def parse_sacra_json(filename):
    logger = logging.getLogger(__name__)
    try:
        with open(filename, 'r') as f:
            dataset = json.load(f)
        # verify the contents are consistent?
    except IOError:
        logger.error("{} not found".format(filename))
        sys.exit(2)
    # except KeyError:
    #     logger.error("{} not correctly formatted. Fatal.".format(filename))
    #     sys.exit(2)
    logger.debug("Parsed {} OK".format(filename))
    return dataset
