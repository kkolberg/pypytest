import ijson.backends.yajl2_cffi as ijson
import simplejson as json
from time import time

def load_json(filename):
     x = 0
     files = 1
     f = None
     buf=8192*10000
     records=[]
     with open(filename, 'rt') as fd:
        parser = ijson.parse(fd)
        objects = ijson.items(fd, 'item')
        people = (o for o in objects)

        start_time = time()
        for person in people:
            x = x + 1
            
            records.append(person)

            if x == 10000:
                end_time = time()
                time_taken = end_time - start_time
                print("file "+str(files) +" - " + str(time_taken))
                start_time = time()
                with open("./temp/sub-"+str(files)+".json", "w+") as f:
                     for p in records:
                         f.write(json.dumps(p)+'\n')
                start_time = time()
                records = []
                x = 0
                files = files + 1


if __name__ == "__main__":   
    start_time = time()
    load_json("./data.json")

    end_time = time()
    time_taken = end_time - start_time
    print("total time - " + str(time_taken))