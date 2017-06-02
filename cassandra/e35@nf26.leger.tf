#!/usr/bin/python

import csv

f = open("movie_metadata.csv", "rb")
reader = csv.reader(f, delimiter=',')
headers = reader.next()
create = "CREATE TABLE movies ("
for header in headers :
    create += header
