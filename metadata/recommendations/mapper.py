#!/usr/bin/env python

import sys
import json
from datetime import datetime, date, time

for line in sys.stdin:

    line = line.strip().split(",")

    entityId = line[0].strip()
    targetEntityId = line[1].strip()
    eventTime = datetime.strptime(
        line[2], '%d-%b-%y').strftime('%Y-%m-%d')

    jsonString = json.dumps({"event": "view", "entityType": "user", "entityId": entityId, "targetEntityType": "item",
                                  "targetEntityId": targetEntityId, "eventTime": eventTime})
    print '%s' % (jsonString)
