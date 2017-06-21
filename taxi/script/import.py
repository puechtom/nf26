# -*- coding: utf-8 -*-

import numpy as np 
from geopy.distance import vincenty
import csv
import ast
from datetime import date, time, datetime, timedelta
import pprint
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

cluster = Cluster()
session = cluster.connect("e35_taxi")

BATCH_SIZE = 100

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

if __name__ == "__main__":
    with open("../data/train_clean.csv") as f:
        print ("Reading CSV...")
        reader = csv.reader(f, delimiter=',', quotechar='"')
        row_count = sum(1 for row in reader)
        print (str(row_count) + " rows read")
        f.seek(0)
        headers = next(reader)
        step = int(row_count/100)
        n=0
        batch = BatchStatement()
        for i, values in enumerate(reader):
            if i < 10 or i%step == 0:
                print_progress(i, row_count,
                    suffix=str("Row " + str(i)),
                    bar_length=50)
            try:
                query = ("INSERT INTO taxi("
                    + "trip_id,"
                    + "call_type,"
                    + "origin_call,"
                    + "origin_stand,"
                    + "taxi_id,"
                    # + "timestamp,"
                    + "year,"
                    + "month,"
                    + "day,"
                    + "hour,"
                    + "minute,"
                    # + "season,"
                    # + "weekday,"
                    # + "day_type,"
                    # + "missing_data,"
                    # + "polyline,"
                    # + "start_loc_long,"
                    # + "start_loc_lat,"
                    # + "end_loc_long,"
                    # + "end_loc_lat,"
                    + "distance) VALUES("
                    + str(values[headers.index("trip_id")]) + ", "
                    + "'" + str(values[headers.index("call_type")]) + "'" + ", "
                    + str(values[headers.index("origin_call")])  + ", "
                    + str(values[headers.index("origin_stand")]) + ", "
                    + str(values[headers.index("taxi_id")]) + ", "
                    # + str(values[headers.index("timestamp")]) + ", "
                    + str(values[headers.index("year")]) + ", "
                    + str(values[headers.index("month")]) + ", "
                    + str(values[headers.index("day")]) + ", "
                    + str(values[headers.index("hour")]) + ", "
                    + str(values[headers.index("minute")]) + ", "
                    # + "'" + str(values[headers.index("season")]) + "'" + ", "
                    # + str(values[headers.index("weekday")]) + ", "
                    # + "'" + str(values[headers.index("day_type")]) + "'" + ", "
                    # + str(values[headers.index("missing_data")]) + ", "
                    # + "'" + str(values[headers.index("polyline")]) + "'" + ", "
                    # + str(values[headers.index("start_loc_long")]) + ", "
                    # + str(values[headers.index("start_loc_lat")]) + ", "
                    # + str(values[headers.index("end_loc_long")]) + ", "
                    # + str(values[headers.index("end_loc_lat")]) + ", "
                    + str(values[headers.index("distance")]) + ")")
                
                if n%BATCH_SIZE == 0:
                    n=1
                    print("Executing batch...")
                    session.execute(batch)
                    batch = BatchStatement()
                    print("Batch executed")
                else:
                    n+=1
                    batch.add(query)
            except Exception as e:
                if str(e) == "polyline missing_data" :
                    pass
                else:
                    print ("ERROR: exception occured for row " + str(i))
                    print (e)
                    print ("\n")
    
    print ("\n")
    print("Insert complete")