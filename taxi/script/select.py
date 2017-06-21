from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
import random
import numpy as np
import pprint
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import heapq
import math
import operator

cluster = Cluster()
session = cluster.connect("e35_taxi")
session.default_timeout = 30

def select_start_avg():
    list_x = list()
    list_y = list()
    label_x = list()
    query = "SELECT start_loc_long, start_loc_lat, avg(distance) as avg_dist FROM start_dist GROUP BY start_loc_long, start_loc_lat;"
    statement = SimpleStatement(query, fetch_size=1000)
    for row in session.execute(statement):
        list_y.append(row.avg_dist)
        list_x.append(str(row.start_loc_long) + "," + str(row.start_loc_lat))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75
    index_y = map(list_y.index, heapq.nlargest(10, list_y)) 
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))
    ax.barh(ind, list_y, width)
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance moyenne par lieux de départ')
    plt.xlabel('Distance moyenne')
    plt.ylabel('Lieux de départ')   
    plt.savefig("barplot_start_distance.png")

def select_start_count():
    list_x = list()
    list_y = list()
    label_x = list()
    query = "SELECT start_loc_long, start_loc_lat, count(trip_id) as tot FROM start_dist GROUP BY start_loc_long, start_loc_lat;"
    statement = SimpleStatement(query, fetch_size=1000)
    for row in session.execute(statement):
        list_y.append(row.tot)
        list_x.append(str(row.start_loc_long) + "," + str(row.start_loc_lat))
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75
    index_y = map(list_y.index, heapq.nlargest(10, list_y)) 
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))
    ax.barh(ind, list_y, width)
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Meilleurs lieux de départ')
    plt.xlabel('Nombre de courses')
    plt.ylabel('Lieux de départ')   
    plt.savefig("barplot_start_count.png")

def select_weekday_count():
    list_x = list()
    list_y = list()
    label_x = list()
    query = "SELECT weekday, count(*) AS tot FROM weekday GROUP BY weekday;"
    statement = SimpleStatement(query, fetch_size=5000)
    for row in session.execute(statement):
        list_y.append(row.tot)
        list_x.append(row.weekday)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))
    ax.barh(ind, list_y, width)
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Nombre de trajet par jour de la semaine')
    plt.xlabel('Nombre de trajet')
    plt.ylabel('Jour de la semaine')   
    plt.savefig("barplot_weekday_count.png")

def select_weekday_dist():
    list_x = list()
    list_y = list()
    label_x = list()
    query = "SELECT weekday, avg(distance) AS avg_dist FROM weekday GROUP BY weekday;"
    statement = SimpleStatement(query, fetch_size=5000)
    for row in session.execute(statement):
        list_y.append(row.avg_dist)
        list_x.append(row.weekday)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75 # the width of the bars
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y)) 
    ax.barh(ind, list_y, width)
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Distance moyenne parcourue par jour de la semaine')
    plt.xlabel('Distance moyenne')
    plt.ylabel('Jour de la semaine')   
    plt.savefig("barplot_dayofweek_count.png")

def select_taxi_dist():
    list_x = list()
    list_y = list()
    label_x = list()
    query = "SELECT taxi_id, sum(distance) as tot FROM taxi GROUP BY taxi_id;"
    statement = SimpleStatement(query, fetch_size=5000)
    for row in session.execute(statement):
        list_y.append(row.tot)
        list_x.append(row.taxi_id)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))
    ax.barh(ind, list_y, width)
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Taxi ayant parcouru le plus de distance')
    plt.xlabel('Distance parcourue (en m)')
    plt.ylabel('Taxi id')   
    plt.savefig("barplot_taxi_dist.png")

def select_taxi_count():
    list_x = list()
    list_y = list()
    label_x = list()
    query = "SELECT taxi_id, count(trip_id) as tot FROM taxi GROUP BY taxi_id;"
    statement = SimpleStatement(query, fetch_size=5000)
    for row in session.execute(statement):
        list_y.append(row.tot)
        list_x.append(row.taxi_id)
    fig = plt.figure()    
    ax = fig.add_axes([0.2, 0.1, 0.6, 0.75])
    width = 0.75
    index_y = map(list_y.index, heapq.nlargest(10, list_y))
    for indexi in index_y:
       label_x.append(list_x[indexi])
    list_y = heapq.nlargest(10, list_y)
    ind = np.arange(len(list_y))
    ax.barh(ind, list_y, width)
    ax.set_yticks(ind+width/2)
    ax.set_yticklabels(label_x, minor=False)
    plt.title('Taxi ayant réalisé le plus de courses')
    plt.xlabel('Nombre de courses réalisées')
    plt.ylabel('Taxi id')   
    plt.savefig("barplot_taxi_count.png")

if __name__ == "__main__":
    select_start_count()