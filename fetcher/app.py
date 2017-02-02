"""
Fetch data from Salesforce
"""
from time import time
import simplejson as json
import argparse
import smart_open
import requests

def fetchConfig(s3Config):
    with smart_open.smart_open(s3Config) as fin:
        configuration = json.loads(fin.read().decode("utf-8"))
    return configuration

def fetchSalesForceObject(configuration):  
    r = requests.get(configuration["url"], stream=True)
    with open(configuration["output"], 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="Object file fetcher")
    PARSER.add_argument(
        "--config",
        help="where in s3 to load configuration")

    ARGS = PARSER.parse_args()
    GLOBAL_START_TIME = time()

    configuration = fetchConfig(ARGS.config)
    fetchSalesForceObject(configuration)

    GLOBAL_END_TIME = time()
    print "total time - " + str(GLOBAL_END_TIME - GLOBAL_START_TIME)
