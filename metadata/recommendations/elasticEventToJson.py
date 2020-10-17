import sys
import argparse
import csv
import json
import pytz
import io
import pprint
from datetime import datetime, date, time
from elasticsearch import Elasticsearch
from StringIO import StringIO

es = Elasticsearch(host='0.0.0.0')
def search(offset=0,size=1):
    body = {
     "query": {
       "bool": {
         "must": [
           {
             "range": {
               "DATE_MAX": {
                 "gt": "2016/01/01 00:00:00",
                 "lt": "2016/12/31 00:00:00"
               }
             }
           }
         ],
         "must_not": [],
         "should": []
       }
     },
     "from" : offset,
     "size" : size,
     "sort": [],
     "aggs": {}
    }
    res = es.search(index="index_name", doc_type='index_type', body=body)

    return res

def fileTransform(args):
    page = 0
    size = 1000
    next_search = True
    count = 0
    fcount = 0
    filename = args.filename

    while next_search == True:
        offset = page * size
        hits = search(offset, size)
        print(len(hits['hits']['hits']), offset, size)
        jsonFilename = filename.split(".")[0] + "_" + str(fcount) + ".json"
        if len(hits['hits']['hits']) > 0:
            if (count % 1000000 == 0 and count >0):
                fcount += 1
                jsonFilename = filename.split(".")[0] + "_" + str(fcount) + ".json"
                with io.FileIO(jsonFilename, "w") as file:
                    file.write("")
                    file.close()


            with open(jsonFilename, 'a') as jsonFile:
                for row in hits['hits']['hits']:
                    row = row['_source']
                    entityId = row['ID_U'].strip()
                    targetEntityId = row['ID_G'].strip()
                    eventTime = datetime.strptime(
                        row['DATE_LAST'], '%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    if row['ID_G'] != "" and row['ID_U'] != "" :
                        json.dump({"event": "view", "entityType": "user", "entityId": entityId, "targetEntityType": "item", "targetEntityId": targetEntityId, "eventTime": eventTime}, jsonFile)
                        jsonFile.write('\n')
                        count += 1
                        if (count % 100000 == 0):
                            print(json.dumps({"event": "view", "entityType": "user", "entityId": entityId, "targetEntityType": "item", "targetEntityId": targetEntityId, "eventTime": eventTime}))

            jsonFile.close()
        else:
            next_search = False
        page += 1






if __name__ == '__main__':
    argsParser = argparse.ArgumentParser(
        description="From csv track to prediction json event")
    argsParser.add_argument('--filename', default='file_not_found')
    args = argsParser.parse_args()

    fileTransform(args)
