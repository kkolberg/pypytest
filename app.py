"""
Chunk a very large JSON file into smaller 10,000 record files
"""
from time import time
from random import randint
import ijson.backends.yajl2_cffi as ijson
import simplejson as json


def load_json(filename):
    """
    Stream filename into new files in large chunks.
    """
    current_row = 0
    files = 1
    records = []
    with open(filename, 'rt') as __fd:
        objects = ijson.items(__fd, 'item')
        people = (o for o in objects)
        # Variable chunk size to get files above and below 5MB
        chunk_size = randint(8000, 12000)
        start_time = time()
        for person in people:
            current_row = current_row + 1

            records.append(person)
            if current_row == chunk_size:
                end_time = time()
                time_taken = end_time - start_time
                print "file " + str(files) + " - " + str(time_taken)
                write_output(records, files)
                start_time = time()
                records = []
                current_row = 0
                files = files + 1
                chunk_size = randint(8000, 12000)
    # Catch any remainder
    if records.count > 0:
        end_time = time()
        time_taken = end_time - start_time
        print "file " + str(files) + " - " + str(time_taken)
        write_output(records, files)

def write_output(records, files):
    """
    Write output file to disk
    """
    __f = None
    with open("./temp/sub-" + str(files) + ".json", "w+") as __f:
        for __p in records:
            __f.write(json.dumps(__p) + '\n')


if __name__ == "__main__":
    GLOBAL_START_TIME = time()
    load_json("./data.json")
    GLOBAL_END_TIME = time()
    print "total time - " + str(GLOBAL_END_TIME - GLOBAL_START_TIME)
