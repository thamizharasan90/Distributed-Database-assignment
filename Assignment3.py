#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import threading
import sys


##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    connection_cursor=openconnection.cursor()
    connection_cursor.execute("select min(%s) from %s " %(SortingColumnName,InputTable))
    minrating=connection_cursor.fetchone()[0]
    connection_cursor.execute("select max(%s) from %s" %(SortingColumnName,InputTable))
    maxrating=connection_cursor.fetchone()[0]
    step=(maxrating-minrating)/5
    allthreads=[]
    incr=1
    while incr < 6:
        temp=minrating+step
        temptable="tempsort" + `incr`
        if incr <= 4:
            th=threading.Thread(target=createrangepartition, args=(InputTable,temptable,SortingColumnName,minrating,temp,openconnection))
        else:
            temp=temp+0.5
            th=threading.Thread(target=createrangepartition, args=(InputTable, temptable, SortingColumnName, minrating, temp, openconnection))
        allthreads.append(th)
        #print allthreads
        print step,minrating,temp
        th.start()
        minrating=temp
        incr=incr+1
    openconnection.commit()
    for j in allthreads:
        j.join()
    connection_cursor.execute("create table if not exists %s as (select * from %s where  1=2)" %(OutputTable,InputTable))
    connection_cursor.execute("select table_name from information_schema.tables where table_name like 'tempsort%'");
    data = connection_cursor.fetchall()
    for dat in data:
        connection_cursor.execute("insert into %s select * from %s" %(OutputTable,dat[0]))
    openconnection.commit()
   # for dat in data:
   #     connection_cursor.execute("drop table %s" % (dat[0]))
    openconnection.commit()
    connection_cursor.close()

def createrangepartition (InputTable,parttable,SortingColumnName,minrating,maxrating,openconnection):
    connection_cursor=openconnection.cursor()
    connection_cursor.execute("create table if not exists %s as select * from %s where %s < %s  and %s >= %s order by %s " %(parttable,InputTable,SortingColumnName,maxrating,SortingColumnName,minrating,SortingColumnName))
    connection_cursor.close()
    openconnection.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    connection_cursor = openconnection.cursor()
    connection_cursor.execute("select min(%s) from %s " % (Table1JoinColumn, InputTable1))
    minrating = connection_cursor.fetchone()[0]
    connection_cursor.execute("select max(%s) from %s" % (Table1JoinColumn, InputTable1))
    maxrating = connection_cursor.fetchone()[0]
    step = (maxrating - minrating) / 5
    allthreads = []
    incr = 1
    while incr < 6:
        temp = minrating + step
        temptable = "tempjoin" + `incr`
        if incr <= 4:
            th = threading.Thread(target=createjoin,
                                  args=(InputTable1, InputTable2, temptable, Table1JoinColumn, Table2JoinColumn, minrating, temp, openconnection))
        else:
            temp = temp + 2
            th = threading.Thread(target=createjoin,
                                  args=(InputTable1,InputTable2, temptable,Table1JoinColumn,Table2JoinColumn, minrating, temp, openconnection))
        allthreads.append(th)
        th.start()
        minrating = temp
        incr = incr + 1
    openconnection.commit()
    for j in allthreads:
        j.join()
    connection_cursor.execute("create table if not exists %s  as (select * from %s where  1=2)" % (OutputTable, 'tempjoin1'))
    connection_cursor.execute("select table_name from information_schema.tables where table_name like 'tempjoin%'");
    data = connection_cursor.fetchall()
    for dat in data:
        connection_cursor.execute("insert into %s select * from %s" % (OutputTable, dat[0]))
    openconnection.commit()
    for dat in data:
        connection_cursor.execute("drop table %s" % (dat[0]))
    openconnection.commit()
    connection_cursor.close()

def createjoin (InputTable1, InputTable2, jointable ,Table1JoinColumn,Table2JoinColumn,minrating,maxrating,openconnection):
    connection_cursor=openconnection.cursor()
    connection_cursor.execute("create table if not exists %s as select * from %s table1 INNER JOIN %s table2 on table1.%s=table2.%s and table1.%s<%s and table1.%s>=%s"
                              %(jointable,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,Table1JoinColumn,maxrating,Table1JoinColumn,minrating))
    openconnection.commit()
    connection_cursor.close()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment2"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment2 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
