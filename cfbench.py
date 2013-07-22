#/usr/bin/env python
# -*- coding: utf-8 -*-

import pyrax
import time
import random
import argparse
import logging


def connection_test(cf, container):
    '''
    Test connection by getting metadata and creating a test
    container.
    '''
    try:
        cf.create_container(container)
    except:
        logging.error("Container creation failed.")
    try:
        cf.delete_container(container)
    except:
        logging.error("Container deletion failed.")


def upload_random_obj(cf, container, length):
    '''
    Generate n random objects and upload to container.
    '''
    cf.create_container(container)
    text = pyrax.utils.random_name(length=length)
    name = pyrax.utils.random_name(ascii_only=True)
    chksum = pyrax.utils.get_checksum(text)
    logging.debug("Uploading {0}".format(name))
    obj = cf.store_object(container, name, text, etag=chksum)
    if chksum != obj.etag:
        logging.error("Checksum Mismatch!")


def fetch_random_obj(cf, container):
    '''
    Fetch a random object.
    '''
    cont = cf.get_container(container)
    objects = cont.get_objects()
    num = random.randrange(0, len(objects))
    obj = cont.get_object(objects[num].name)
    return obj


def upload_benchmark(cf, container, length, n):
    '''
    Upload n number of objects to a container.
    '''
    start = time.time()
    for i in range(0, n):
        upload_random_obj(cf, container, length)
    end = time.time()
    total_obj = n
    seconds = end - start
    logging.info("Uploaded {0} objects in {1} seconds.".format(
        total_obj, seconds))
    logging.info("{0} objects per second.".format(total_obj / seconds))


def fetch_benchmark(cf, container, n, chunk_size=8192):
    '''
    Fetch n number of objects to a temp directory.
    '''
    start = time.time()
    count = 0
    mismatch = 0
    while count < n:
        obj = fetch_random_obj(cf, container)
        obj_gen = obj.fetch(chunk_size=chunk_size)
        output = "".join(obj_gen)
        chksum = pyrax.utils.get_checksum(output)
        if chksum == obj.etag:
            match = "Matched"
        else:
            match = "Mismatch!"
            mismatch += 1
        logging.debug("Fetched {0} Chksum {1}".format(obj.name, match))
        count += 1
    end = time.time()
    total_obj = n
    seconds = end - start
    logging.info("Fetched {0} objects in {1} seconds.".format(
        total_obj, seconds))
    logging.info("{0} objects per second.".format(total_obj / seconds))
    logging.info("{0} mismatched checksums.".format(mismatch))


def cleanup(cf, container):
    '''
    Cleanup all the testing data and containers.
    '''
    cont = cf.create_container(container)
    total_obj = len(cont.get_objects())
    start = time.time()
    cont.delete_all_objects()
    while cont.get_objects() != []:
        print(cont.get_objects())
    cont.delete()
    end = time.time()
    seconds = end - start
    logging.info("Deleted {0} objects in {1} seconds".format(
        total_obj, seconds))
    logging.info("{0} objects per second.".format(total_obj / seconds))

if __name__ == "__main__":

    # Command line arguments
    description = "Benchmark Cloud Files."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('test', choices=['fetch', 'upload', 'clean'])
    parser.add_argument('container', type=str,
                        help='Container to use for tests.')
    parser.add_argument('-n', '--count', type=int, default=10,
                        help='Number of tests to perform.')
    parser.add_argument('-c', '--chunk', type=int, default=8192,
                        help='Chunk size for objects.')
    parser.add_argument('-m', '--multi', type=int, default=100,
                        help='Chunk * Multi = Total Object Size.')
    parser.add_argument('-r', '--region', type=str, default='ORD',
                        help='Cloud Files region for the tests.')
    parser.add_argument('-l', '--log', type=str, default="/tmp/cfbench.log",
                        help='Location of the log file.')
    parser.add_argument('-D', '--debug', type=bool,
                        help='Set for debug logging output.')
    args = parser.parse_args()

    # Logging
    if args.debug is True:
        level = logging.DEBUG
    else:
        level = logging.INFO
    formatting = '%(asctime)s:%(levelname)s:%(message)s'
    logging.basicConfig(filename=args.log, level=level,
                        format=formatting)

    # Setup pyrax connection handler
    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_setting("region", args.region)
    pyrax.set_credential_file(".pyrax_creds")
    cf = pyrax.cloudfiles

    # Testing variables
    container = args.container
    count = args.count
    length = args.chunk * args.multi

    # Test connection
    connection_test(cf, pyrax.utils.random_name(ascii_only=True))

    if args.test == 'upload':
        # Generate some random objects
        logging.info("Uploading {0} objects sized {1} to {2}.".format(
            count, length, container))
        upload_benchmark(cf, container, length, count)
        logging.info("Upload complete to {0}.".format(container))
    elif args.test == 'fetch':
        # Fetch random objects
        logging.info("Fetching {0} objects from {1}.".format(count, container))
        fetch_benchmark(cf, container, count)
    elif args.test == 'clean':
        # Cleanup our mess
        logging.info("Attempting cleanup of {0}.".format(container))
        cleanup(cf, container)
        logging.info("Cleanup of {0} complete.".format(container))
