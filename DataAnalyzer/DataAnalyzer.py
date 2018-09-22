import spacy
import time
import sys
import os

from pymongo import MongoClient


def main():
    client = MongoClient(appname="Data Analyzer")

    try:
        client.address  # Throws exception if the database is not connected
    except Exception as e:
        print("Could not connect to MongoDB, check if the process 'mongod' is running. Exception: ", e)
        exit(-1)

    db_collection = client.twitter.main

    # Query the DB for max 35,000 records that did not analyzed yet.
    results = db_collection.find({'auxpass': {'$exists': False}}, limit=35000)

    nlp = spacy.load('en')


    for res in results:
        total_count = 0
        auxpass_count = 0
        doc = nlp(res['text'])

        for token in doc:
            # print(token.text, token.dep_)
            if token.dep_ != 'punct':
                total_count += 1
                if token.dep_ == 'auxpass':
                    auxpass_count += 1

        auxpass = auxpass_count / total_count
#        print(res)
        db_collection.update({"_id": res['_id']}, {'$set': {'auxpass': auxpass}})


if __name__ == '__main__':
    # If running with windows scheduler (the running process is python which has different working directory).
    # os.chdir("<DataAnalyzer Project Directory Path>")
    sys.stdout = open("log.txt", "a")
    sys.stderr = sys.stdout  # prints errors to the log file.

    print("\n", time.ctime(), ":")  # Start time printing
    start = time.time()

    main()

    end = time.time()
    print("Execution Time: ", (end - start) / 60, "minutes\n")
    exit(0)
