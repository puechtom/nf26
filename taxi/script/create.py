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
            PRIMARY KEY(trip_id));"""
    session.execute(query)

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
            PRIMARY KEY((end_loc_long, end_loc_lat), distance, year, month, day, hour, minute, trip_id));"""
    session.execute(query)

def table_by_end():
    query = """CREATE TABLE IF NOT EXISTS end_date(
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
            PRIMARY KEY((end_loc_long, end_loc_lat), year, month, day, hour, minute, trip_id));"""
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

def main():
    # table_by_start_by_dist()
    table_by_start()
    # table_by_end_by_dist()
    # table_by_end()
    # table_by_date()
    # table_by_weekday()
    # create_date()
    # create_location()
    # create_taxi()
    # create_call()
    # create_fact()

if __name__ == "__main__":
    main()
