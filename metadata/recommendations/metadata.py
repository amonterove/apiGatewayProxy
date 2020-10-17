#!/usr/bin/env python

import os,sys
import argparse
import predictionio
import pytz
import cx_Oracle

def populate(conn,client):

    counter = 0
    id_instalacion = "0"

    cur = conn.cursor()
    cur.arraysize = 1000
    query = "SELECT * FROM DUAL"
    cur.execute(query)

    for result in cur:
        group = result[0]
        group_str = str(group)
        content_type = str(result[1])
        access_format = str(result[2])
        status = str(result[3])
	    rating = str(result[4])
        properties = {}
        properties['content_type'] = [content_type]
        properties['access_format'] = [access_format]
        properties['status'] = [status]
	    properties['rating'] = [rating]

        #regiones
        query = "SELECT * FROM DUAL where ID = " + group_str
        cur1 = conn.cursor()
        cur1.execute(query)
        regiones = []
        for row in cur1:
            regiones.append(row[0])
        cur1.close()

        #generos
        query = "SELECT * FROM DUAL where ID = " + group_str
        cur1 = conn.cursor()
        cur1.execute(query)
        generos = []
        for row in cur1:
            generos.append(str(row[0]))
        cur1.close()

        #talents
        query = "SELECT * FROM DUAL where ID = " + group_str
        cur1 = conn.cursor()
        cur1.execute(query)
        talents = []
        for row in cur1:
            talents.append(str(row[0]))
        cur1.close()

        if len(regiones):
            properties['regions'] = regiones
        else:
            continue

        if len(generos):
            properties['genres'] = generos

        if len(talents):
            properties['talents'] = talents

        client.create_event(
            event="$set",
            entity_type="item",
            entity_id=group,
            properties=properties
        )
        counter += 1
    cur.close()

def setupDB(args):
    id_instalacion = "0"
    db_user = "USER"
    db_pass = "PASS"
    db_charset = "UTF8"
    db_server = "0.0.0.0:0"
    db_name = "TEST"

    os.environ["NLS_LANG"] = "SPANISH_MEXICO.AL32UTF8"
    conn = cx_Oracle.connect(db_user, db_pass, db_server + "/" + db_name)

    conn.current_schema = 'SCHEMA'
    return conn

def setupClient(args):
    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=1000)

    return client

if __name__ == "__main__":
    # parsear los argumentos e ir al proceso en si
    parser = argparse.ArgumentParser(
        description="Import metadata for recommendation engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")

    args = parser.parse_args()

    try:
        conn = setupDB(args)
        client = setupClient(args)
        populate(conn,client)
        conn.close()
    except:
        e = sys.exc_info()
        print e
        exit(1)
