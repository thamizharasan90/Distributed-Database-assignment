#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    rangefptr=open('RangeQueryOut.txt','w')
    connection_cursor=openconnection.cursor()
    connection_cursor.execute("select table_name from information_schema.tables where table_name like 'rangeratingspart%' or table_name like 'roundrobinratingspart%'");
    data=connection_cursor.fetchall()
    for dat in data:
        connection_cursor.execute("select * from %s where rating >= %f and rating <=%f" %(dat[0],ratingMinValue,ratingMaxValue));
        da=connection_cursor.fetchall()
        for d in da:
            value=[dat[0], str(d[0]), str(d[1]), str(d[2])]
            s=','.join(value)
            rangefptr.write(s)
            rangefptr.write("\n")

def PointQuery(ratingsTableName, ratingValue, openconnection):
    pointfptr = open('PointQueryOut.txt', 'w')
    connection_cursor = openconnection.cursor()
    connection_cursor.execute("select table_name from information_schema.tables where table_name like 'rangeratingspart%' or table_name like 'roundrobinratingspart%'");
    data = connection_cursor.fetchall()
    for dat in data:
        connection_cursor.execute(
            "select * from %s where rating = %f " % (dat[0], ratingValue));
        da = connection_cursor.fetchall()
        for d in da:
            value = [dat[0], str(d[0]), str(d[1]), str(d[2])]
            s=','.join(value)
            pointfptr.write(s)
            pointfptr.write("\n")


