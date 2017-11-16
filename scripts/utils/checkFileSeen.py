from __future__ import division, print_function
import os, sys
import hashlib
import logging

def ensure_sacra_file_seen(filename, preview):
    logger = logging.getLogger(__name__)
    hasher = hashlib.md5()

    with open(filename, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    md5 = hasher.hexdigest();

    logger.debug("{} hash: {}".format(filename, md5))

    with open("./.sacra_hashes", 'rU') as f:
        for line in f:
            if line.strip() == md5:
                logger.info("{} has been seen before. Proceeding.".format(filename))
                return True

    ## if we're here, then the file hasn't been seen before
    if preview:
        logger.info("First time seeing {}".format(filename))
    else:
        logger.warn("Cannot mofify database as {} has not been seen before. Continuing in \"preview\" mode".format(filename))

    with open("./.sacra_hashes", 'a+') as f:
        f.write(md5 + "\n")
    return False
