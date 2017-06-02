#!/usr/bin/python
import csv
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect("e35_taxi")

def create_date():
    query = "CREATE TABLE date(" +
            "timestamp bigInt, " +
            "year int, " +
            "month int, " +
            "week int, " +
            "season int, " +
            "day int, " +
            "day_of_week int, " +
            "hour int, " +
            "date_type varchar, " +
            "PRIMARY KEY(timestamp, year, month, day, hour, dateType));"
    session.execute(query)

def create_location():
    query = "CREATE TABLE location(" +
            "latitude double, " +
            "longitude double, " +
            "PRIMARY KEY(latitude, longitude));"
    session.execute(query)

def create_taxi():
    query = "CREATE TABLE taxi(" +
            "id bigInt, " +
            "PRIMARY KEY(id));"
    session.execute(query)

def create_call():
    query = "CREATE TABLE call(" +
            "id bigInt, " +
            "origin_call bigInt, " +
            "origin_stand bigInt, " +
            "call_type varchar, " +
            "PRIMARY KEY(timestamp, year, month, day, hour, dateType));"
    session.execute(query)

def create_fact():
    query = "CREATE TABLE fact(" +
            "fk_call bigInt, " +
            "fk_date bigInt, " +
            "fk_taxi bigInt, " +
            "fk_latitude bigInt, " +
            "fk_longitude bigInt, " +
            "distance double, " +
            "PRIMARY KEY(timestamp, year, month, day, hour, dateType));"
    session.execute(query)

def main():
    create_date()
    create_location()
    create_taxi()
    create_call()
    create_fact()

if __name__ == "__main__":
    maint()
