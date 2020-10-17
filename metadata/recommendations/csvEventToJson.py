import sys
import argparse
import csv
import json
import pytz
import io

from StringIO import StringIO
from datetime import datetime, date, time


def fileTransform(args):
    filename = args.filename

    jsonFilename = filename.split(".")[0] + ".json"

    with io.FileIO(jsonFilename, "w") as file:
        file.write("")
        file.close()

    count = 0
    with open(filename, 'rb') as csvfile:

        jsonFile = open(jsonFilename, 'rw+')

        trackreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in trackreader:
            entityId = row[0].strip()
            targetEntityId = row[1].strip()
            eventTime = datetime.strptime(
                row[2], '%d-%b-%y').strftime('%Y-%m-%d')

            json.dump({"event": "view", "entityType": "user", "entityId": entityId, "targetEntityType": "item",
                       "targetEntityId": targetEntityId, "eventTime": eventTime}, jsonFile)

            jsonFile.write('\n')

            count += 1

            if (count % 100000 == 0):
                print(json.dumps({"event": "view", "entityType": "user", "entityId": entityId, "targetEntityType": "item",
                                  "targetEntityId": targetEntityId, "eventTime": eventTime}))

        jsonFile.close()

if __name__ == "__main__":

    argsParser = argparse.ArgumentParser(
        description="From csv track to prediction json event")
    argsParser.add_argument('--filename', default='file_not_found')
    args = argsParser.parse_args()

    fileTransform(args)
