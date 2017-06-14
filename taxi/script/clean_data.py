# -*- coding: utf-8 -*-

import numpy as np 
from geopy.distance import vincenty
import csv
import ast
from datetime import date, time, datetime, timedelta
import pprint
# from cassandra.cluster import Cluster
# from cassandra.query import BatchStatement

# cluster = Cluster()
# session = cluster.connect("e35_taxi")

BATCH_SIZE = 500

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

    print("%s |%s| %s%s %s\r" % (prefix, bar, percents, '%', suffix), end='')

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

Y = 2000 # dummy
seasons = [('winter', (date(Y,  1,  1),  date(Y,  3, 20))),
           ('spring', (date(Y,  3, 21),  date(Y,  6, 20))),
           ('summer', (date(Y,  6, 21),  date(Y,  9, 22))),
           ('autumn', (date(Y,  9, 23),  date(Y, 12, 20))),
           ('winter', (date(Y, 12, 21),  date(Y, 12, 31)))]

def get_season(now):
    if isinstance(now, datetime):
        now = now.date()
    now = now.replace(year=Y)
    return next(season for season, (start, end) in seasons
                if start <= now <= end)

def dist(polyline): # as list
    dists = []
    for elt1, elt2 in zip(polyline, polyline[1::]):
        dists.append(vincenty(elt1, elt2).meters)
    return sum(dists)

def init_dico(row):
    values = dict()
    values["trip_id"] = row[0]
    values["call_type"] = row[1]
    values["origin_call"] = row[2]
    values["origin_stand"] = row[3]
    values["taxi_id"] = row[4]
    values["timestamp"] = row[5]
    values["day_type"] = row[6]
    values["missing_data"] = row[7]
    values["polyline"] = row[8]
    return values

def verif(values):
    # verif trip_id
    if not values["trip_id"]:
        raise ValueError("error TRIP_ID")

    # verif call_type
    if not values["call_type"] in ['A', 'B', 'C']:
        if values["origin_call"]:
            print ("WARNING: CALL_TYPE auto set to A")
            values["call_type"] = 'A'
        elif values["origin_stand"]:
            print ("WARNING: CALL_TYPE set to B")
            values["call_type"] = 'B'
        else:
            raise ValueError("error CALL_TYPE")

    # verif origin_call
    if not values["origin_call"]:
        values["origin_call"] = "null"
        if values["call_type"] == 'A':
            # print "WARNING: CALL_TYPE set to C due to missing ORIGIN_CALL data"
            values["call_type"] = 'C'
    else:
        if values["call_type"] != 'A':
            print ("WARNING: CALL_TYPE set to A due to ORIGIN_CALL data")
            values["call_type"] = 'A'

    # verif origin_stand
    if not values["origin_stand"]:  
        values["origin_stand"] = "null"  
        if values["call_type"] == 'B':
            # print "WARNING: CALL_TYPE set to C due to missing ORIGIN_STAND data"
            values["call_type"] = 'C'
    else:
        if values["call_type"] != 'B':
            # print "WARNING: CALL_TYPE set to B due to ORIGIN_STAND data"
            values["call_type"] = 'B'

    # verif taxi_id
    if not values["taxi_id"]:
        raise ValueError("error TAXI_ID")

    # # verif timestamp
    # dummy = date.fromtimestamp(int(values["timestamp"]))
    # if not isinstance(dummy, date):
    #     raise ValueError("error TIMESTAMP")

    # verif day_type
    if values["timestamp"] in day_type_B:
        values["day_type"] = 'B'
    elif values["timestamp"] in day_type_C:
        values["day_type"] = 'C'
    else:
        values["day_type"] = 'A'

    # verif missing_data
    values["polyline"] = ast.literal_eval(values["polyline"])

    # verif missing_data
    if len(values["polyline"]) < 2:
        values["missing_data"] = True
    elif values["missing_data"] == "False":
        values["missing_data"] = False
    elif values["missing_data"] == "True":
        values["missing_data"] = True

    return values


def addFacts(values):
    # Datetime
    date_value = datetime.fromtimestamp(int(values["timestamp"]))
    # year
    values["year"] = date_value.year
    # month
    values["month"] = date_value.month
    # day
    values["day"] = date_value.day
    # hour
    values["hour"] = date_value.hour
    # minute
    values["minute"] = date_value.minute
    # season
    values["season"] = get_season(date_value)
    # weekday
    values["weekday"] = date_value.weekday()

    # Location
    if not values["missing_data"]:     
        # start_loc
        values["start_loc_long"] = round(float(values["polyline"][0][0]), 3)
        values["start_loc_lat"] = round(float(values["polyline"][0][1]), 3)
        # end_loc
        values["end_loc_long"] = round(float(values["polyline"][-1][0]), 3)
        values["end_loc_lat"] = round(float(values["polyline"][-1][1]), 3)
        # distance
        values["distance"] = int(round(float(dist(values["polyline"])), 0))
    else:
        # values["start_loc_long"] = "null"
        # values["start_loc_lat"] = "null"
        # values["end_loc_long"] = "null"
        # values["end_loc_lat"] = "null"
        # values["distance"] = "null"
        raise ValueError("polyline missing_data")

    return values


if __name__ == "__main__":
    with open("../data/train.csv") as f, open("../data/train_clean.csv", "w") as fout:
        print ("Reading CSV...")
        reader = csv.reader(f, delimiter=',', quotechar='"')
        row_count = sum(1 for row in reader)
        print (str(row_count) + " rows read")
        f.seek(0)
        headers = next(reader)
        step = int(row_count/1000)
        for i, row in enumerate(reader):
            if i < 10 or i%step==0: print_progress(i, row_count, bar_length=50)
            values = init_dico(row)
            try:
                values = verif(values)
                values = addFacts(values)
                if i == 0: 
                    writer = csv.DictWriter(fout, 
                        fieldnames=list(values.keys()), 
                        delimiter=',',
                        quotechar='"',
                        quoting=csv.QUOTE_ALL)
                    writer.writeheader()
                writer.writerow(values)

            except Exception as e:
                if str(e) == "polyline missing_data" :
                    pass
                else:
                    print ("ERROR: exception occured for row " + str(i))
                    print (e)
                    print ("\n")
