"""
Chunk a very large JSON file into smaller 10,000 record files
"""
from time import time
import ijson.backends.yajl2_cffi as ijson
import simplejson as json
import argparse
import smart_open

def fetchConfig(s3Config):
    with smart_open.smart_open(s3Config) as fin:
        configuration = json.loads(fin.read().decode("utf-8"))
    return configuration

def load_json(configuration):
    """
    Stream filename into new files in large chunks.
    """
    current_row = 0
    files = 1
    records = []
    with open(configuration["input"], 'rt') as __fd:
        objects = ijson.items(__fd, 'item')
        people = (o for o in objects)
        chunk_size = 15000
        start_time = time()
        for person in people:
            current_row = current_row + 1

            records.append(person)
            if current_row == chunk_size:
                end_time = time()
                time_taken = end_time - start_time
                print "file " + str(files) + " - " + str(time_taken)
                write_output(records, files, configuration)
                start_time = time()
                records = []
                current_row = 0
                files = files + 1
    # Catch any remainder
    if records.count > 0:
        end_time = time()
        time_taken = end_time - start_time
        print "file " + str(files) + " - " + str(time_taken)
        write_output(records, files, configuration)

def write_output(records, files, configuration):
    """
    Write output file to disk
    """
    __f = None
    with open(configuration["output"]+"/sub-" + str(files) + ".json", "w+") as __f:
        for __p in records:
            __f.write(json.dumps(__p) + '\n')


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Split massive file")
    PARSER.add_argument(
        "--config",
        help="where in s3 to load configuration")

    ARGS = PARSER.parse_args()
    GLOBAL_START_TIME = time()

    configuration = fetchConfig(ARGS.config)
    load_json(configuration)

    GLOBAL_END_TIME = time()
    print "total time - " + str(GLOBAL_END_TIME - GLOBAL_START_TIME)
