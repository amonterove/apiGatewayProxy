#!/usr/bin/env python

import os
import sys
import argparse
import pytz
import cx_Oracle
import json
from StringIO import StringIO

def populate(conn):

    counter = 0
    arraysize = 1000

    connectionCursor = conn.cursor()
    connectionCursor.arraysize = arraysize
    query = ("SELECT * FROM DUAL")

    connectionCursor.execute(query)

    jsonFile = open('json_metadata.json', 'rw+')

    for result in connectionCursor:
        groupId = result[0]
        contentType = str(result[1])
        accessFormat = str(result[2])
        status = str(result[3])
        pgRate = str(result[4])
        properties = {}
        properties['contentType'] = [contentType]
        properties['accessFormat'] = [accessFormat]
        properties['status'] = [status]

        if len(pgRate):
            properties['pgRate'] = [pgRate]

        # regiones
        query = "SELECT * FROM DUAL where ID = " + \
            str(groupId) + " order by ID"
        cursor = conn.cursor()
        cursor.arraysize = arraysize
        cursor.execute(query)
        regiones = []
        for row in cursor:
            regiones.append(row[0])
        cursor.close()

        # generos
        query = ("SELECT * FROM DUAL "
                 "where ID = " + str(groupId) + " "
                 "group by ID, NAME "
                 )

        cursor = conn.cursor()
        cursor.arraysize = arraysize
        cursor.execute(query)
        generos = []
        genresName = []
        for row in cursor:
            generos.append(str(row[0]))
            genresName.append(str(row[1]))
        cursor.close()

        # talents
        query = ("SELECT * "
                 "from DUAL "
                 "where ID = " + str(groupId)
                 )

        cursor = conn.cursor()
        cursor.execute(query)
        talents = []
        for row in cursor:
            talents.append(str(row[0]))
        cursor.close()

        if len(regiones):
            properties['regions'] = [regiones]
        else:
            properties['status'] = ["disabled"]

        if len(generos):
            properties['genres'] = [generos]

        if len(genresName):
            properties['genresName'] = [genresName]

        if len(talents):
            properties['talents'] = [talents]

        json.dump({"event": "$set", "entityType": "item",
                   "entityId": groupId, "properties": properties}, jsonFile)

        if (counter % 100 == 0):
            print(json.dumps({"event": "$set", "entityType": "item",
                              "entityId": groupId, "properties": properties}))

        jsonFile.write('\n')

        counter += 1

    jsonFile.close()

    connectionCursor.close()

    print "Metadata: ", str(counter), "items"


def setupDB(args):
    databases = {}
    databases['test'] = {}
    databases['test']['user'] = "USER"
    databases['test']['password'] = "PASS"
    databases['test']['hostname'] = "0.0.0.0:0"
    databases['test']['service_name'] = "TEST"

    databases['prod'] = {}
    databases['prod']['user'] = "USER"
    databases['prod']['password'] = "PASS"
    databases['prod']['hostname'] = "0.0.0.0:0"
    databases['prod']['service_name'] = "PROD"

    db = databases[args.env]

    os.environ["NLS_LANG"] = "SPANISH_MEXICO.AL32UTF8"
    connection = cx_Oracle.connect(db['user'], db['password'], db['hostname'] + "/" + db['service_name'])

    connection.current_schema = 'SCHEMA'
    return connection


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Import metadata for recommendation engine")
    parser.add_argument('--access_key', default='invalid_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--env', default="test")

    args = parser.parse_args()
    try:
        connection = setupDB(args)
        populate(connection)
        connection.close()
    except:
        e = sys.exc_info()
        print e
        exit(1)
