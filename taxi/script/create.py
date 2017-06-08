#!/usr/bin/python
import csv
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect("e35_taxi")
# CREATE KEYSPACE IF NOT EXISTS e35_taxi WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 5};

def create_date():
    query = """CREATE TABLE date(
            timestamp double,
            year int,
            month int,
            week int,
            season int,
            day int,
            day_of_week int,
            hour int,
            date_type varchar,
            PRIMARY KEY(timestamp, year, month, day, hour, date_type));"""
    session.execute(query)

def create_location():
    query = """CREATE TABLE location(
            latitude double,
            longitude double,
            PRIMARY KEY(latitude, longitude));"""
    session.execute(query)

def create_taxi():
    query = """CREATE TABLE taxi(
            id double,
            PRIMARY KEY(id));"""
    session.execute(query)

def create_call():
    query = """CREATE TABLE call(
            id double,
            origin_call double,
            origin_stand double,
            call_type varchar,
            PRIMARY KEY(id, origin_call, origin_stand, call_type));"""
    session.execute(query)

def create_fact():
    query = """CREATE TABLE fact(
            fk_call double,
            fk_date double,
            fk_taxi double,
            fk_latitude double,
            fk_longitude doulbe,
            distance double,
            PRIMARY KEY(timestamp, year, month, day, hour, dateType));"""
    session.execute(query)

def main():
    create_date()
    create_location()
    create_taxi()
    create_call()
    # create_fact()

if __name__ == "__main__":
    main()
