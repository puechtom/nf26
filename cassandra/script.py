#!/usr/bin/python

import csv
from cassandra.cluster import Cluster


class prg :




cluster = Cluster()
session = cluster.connect("e35")

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def create():
    f = open("movie_metadata.csv", "rb")
    reader = csv.reader(f, delimiter=',')
    headers = next(reader)
    firstLine = next(reader)
    create = "CREATE TABLE movies ("
    for i in range(0, len(headers)) :
        create += headers[i]
        if (RepresentsInt(firstLine[i])):
            create += " int"
        else:
            create += " text"
        create += ","
    create += "primary key (movie_title, director_name));"
    session.execute(create)

def insert():
    f = open("movie_metadata.csv", "rb")
    reader = csv.reader(f, delimiter=',', quotechar='|')
    headers = next(reader)
    for row in reader :
        insert = "INSERT INTO MOVIES ("
        values = "VALUES ("
        insert += ','.join(headers)
        insert += ")"
        for i in range(0, len(headers)) :
            if (RepresentsInt(row[i])) 

