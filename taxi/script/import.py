# -*- coding: utf-8 -*-

import numpy as np 
from geopy.distance import vincenty
import csv
import ast
from datetime import date, time, datetime, timedelta

def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    print('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        print('\n')

day_type_B = [
    date(2014, 1, 1),
    date(2014, 4, 18),
    date(2014, 4, 20),
    date(2014, 4, 25),
    date(2014, 5, 1),
    date(2014, 6, 10),
    date(2013, 8, 15),
    date(2013, 12, 8),
    date(2013, 12, 25)
]

day_type_C = []
for day in day_type_B:
    day_type_C.append(day - timedelta(days=1))

def dist(polyline):
    dists = []
    polyline = ast.literal_eval(polyline)
    for elt1, elt2 in zip(polyline, polyline[1::]):
        dists.append(vincenty(elt1, elt2).meters)
    return sum(dists)

def verif(values):
    # verif tripId
    if not values["tripId"]:
        raise ValueError("error TRIP_ID")

    # verif callType
    if not values["callType"] in ['A', 'B', 'C']:
        if values["originCall"]:
            print "WARNING: CALL_TYPE auto set to A"
            values["callType"] = 'A'
        elif values["originStand"]:
            print "WARNING: CALL_TYPE set to B"
            values["callType"] = 'B'
        else:
            raise ValueError("error CALL_TYPE")

    # verif originCall
    if not values["originCall"]:
        if values["callType"] == 'A':
            # print "WARNING: CALL_TYPE set to C due to missing ORIGIN_CALL data"
            values["callType"] = 'C'
    else:
        if values["callType"] != 'A':
            print "WARNING: CALL_TYPE set to A due to ORIGIN_CALL data"
            values["callType"] = 'A'

    # verif originStand
    if not values["originStand"]:    
        if values["callType"] == 'B':
            # print "WARNING: CALL_TYPE set to C due to missing ORIGIN_STAND data"
            values["callType"] = 'C'
    else:
        if values["callType"] != 'B':
            # print "WARNING: CALL_TYPE set to B due to ORIGIN_STAND data"
            values["callType"] = 'B'

    # verif taxiId
    if not values["taxiId"]:
        raise ValueError("error TAXI_ID")

    # verif timestamp
    values["timestamp"] = date.fromtimestamp(int(values["timestamp"]))

    # verif dayType
    if values["timestamp"] in day_type_B:
        values["dayType"] = 'B'
    elif values["timestamp"] in day_type_C:
        values["dayType"] = 'C'
    else:
        values["dayType"] = 'A'

    # verif missingData
    if not values["polyline"]:
        values["missingData"] = True
    else:
        values["missingData"] = False

if __name__ == "__main__":

    with open("../data/train.csv") as f:
        print "Reading CSV..."
        reader = csv.reader(f, delimiter=',', quotechar='"')
        row_count = sum(1 for row in reader)
        print str(row_count) + " rows read"
        f.seek(0)
        headers = next(reader)
        print headers
        #['TRIP_ID',
        # 'CALL_TYPE',
        # 'ORIGIN_CALL',
        # 'ORIGIN_STAND',
        # 'TAXI_ID',
        # 'TIMESTAMP',
        # 'DAY_TYPE',
        # 'MISSING_DATA',
        # 'POLYLINE']
        for i, row in enumerate(reader):
            if i%5000==0: print_progress(i, row_count, bar_length=50)
            values = dict()
            values["tripId"] = row[0]
            values["callType"] = row[1]
            values["originCall"] = row[2]
            values["originStand"] = row[3]
            values["taxiId"] = row[4]
            values["timestamp"] = row[5]
            values["dayType"] = row[6]
            values["missingData"] = row[7]
            values["polyline"] = row[8]
            try:
                values = verif(values)
            except Exception as e:
                print str(i) + " : " + e
