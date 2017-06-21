#!/usr/bin/python
import csv
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect("e35_taxi")
# CREATE KEYSPACE IF NOT EXISTS e35_taxi WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 5};

def table_by_start_by_dist():
    query = """CREATE TABLE IF NOT EXISTS start_dist(
            trip_id double,
            taxi_id int,
            year int,
            month int,
            day int,
            hour int,
            minute int,
            start_loc_long double,
            start_loc_lat double,
            end_loc_long double,
            end_loc_lat double,
            distance int,
            PRIMARY KEY((start_loc_long, start_loc_lat), distance, year, month, day, hour, minute, trip_id));"""
    session.execute(query)

def table_by_start():
    query = """CREATE TABLE IF NOT EXISTS start_date(
            trip_id double,
            taxi_id int,
            hour int,
            minute int,
            weekday int,
            start_loc_long double,
            start_loc_lat double,
            distance int,
            PRIMARY KEY((start_loc_long, start_loc_lat), weekday, hour, minute, trip_id));"""
    session.execute(query)

def table_by_end_by_dist():
    query = """CREATE TABLE IF NOT EXISTS end_dist(
            trip_id double,
            taxi_id int,
            year int,
            month int,
            day int,
            hour int,
            minute int,
            end_loc_long double,
            end_loc_lat double,
            distance int,
            PRIMARY KEY((end_loc_long, end_loc_lat), distance, year, month, day, hour, minute, trip_id));"""
    session.execute(query)

def table_by_end():
    query = """CREATE TABLE IF NOT EXISTS end_date(
            trip_id double,
            taxi_id int,
            hour int,
            minute int,
            weekday int,
            end_loc_long double,
            end_loc_lat double,
            distance int,
            PRIMARY KEY((end_loc_long, end_loc_lat), weekday, hour, minute, trip_id));"""
    session.execute(query)

def table_by_date():
    query = """CREATE TABLE IF NOT EXISTS date(
            trip_id double,
            call_type varchar,
            origin_call int,
            origin_stand int,
            taxi_id int,
            timestamp int,
            year int,
            month int,
            day int,
            hour int,
            minute int,
            season varchar,
            weekday int,
            day_type varchar,
            missing_data boolean,
            polyline varchar,
            start_loc_long double,
            start_loc_lat double,
            end_loc_long double,
            end_loc_lat double,
            distance int,
            PRIMARY KEY((year, month), day, hour, minute, trip_id));"""
    session.execute(query)

def table_by_loc():
    query = """CREATE TABLE IF NOT EXISTS loc(
            trip_id double,
            call_type varchar,
            origin_call int,
            origin_stand int,
            taxi_id int,
            year int,
            month int,
            day int,
            hour int,
            minute int,
            season varchar,
            weekday int,
            day_type varchar,
            start_loc_long double,
            start_loc_lat double,
            end_loc_long double,
            end_loc_lat double,
            distance int,
            PRIMARY KEY((start_loc_long, start_loc_lat, end_loc_long, end_loc_lat), weekday, hour, minute, trip_id));"""
    session.execute(query)

def table_by_weekday():
    query = """CREATE TABLE IF NOT EXISTS weekday(
            trip_id double,
            call_type varchar,
            origin_call int,
            origin_stand int,
            taxi_id int,
            timestamp int,
            year int,
            month int,
            day int,
            hour int,
            minute int,
            season varchar,
            weekday int,
            day_type varchar,
            missing_data boolean,
            polyline varchar,
            start_loc_long double,
            start_loc_lat double,
            end_loc_long double,
            end_loc_lat double,
            distance int,
            PRIMARY KEY((weekday), distance, year, month, hour, minute, trip_id));"""
    session.execute(query)

def table_by_taxi():
    query = """CREATE TABLE IF NOT EXISTS taxi(
            trip_id double,
            call_type varchar,
            origin_call int,
            origin_stand int,
            taxi_id int,
            year int,
            month int,
            day int,
            hour int,
            minute int,
            distance int,
            PRIMARY KEY((taxi_id), distance, year, month, hour, minute, trip_id));"""
    session.execute(query)

def main():
    # table_by_start_by_dist()
    # table_by_start()
    # table_by_end_by_dist()
    # table_by_end()
    table_by_taxi()
    # table_by_date()
    # table_by_weekday()
    # create_date()
    # create_location()
    # create_taxi()
    # create_call()
    # create_fact()

if __name__ == "__main__":
    main()
