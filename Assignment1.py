#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'test_dds_assgn1'
partition_number=3

def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    try:
        connection_cursor=openconnection.cursor()
        connection_cursor.execute("drop table if exists %s" %(ratingstablename))
        connection_cursor.execute("CREATE TABLE %s (UserID REAL, MovieID REAL, Rating REAL)" %(ratingstablename))
        table_ptr=open(ratingsfilepath,"r")
        for table_line in table_ptr:
            result=table_line.split("::")
            connection_cursor.execute("INSERT INTO %s VALUES(%f,%f,%f) " %(ratingstablename, float(result[0]), float(result[1]),float(result[2])))
        openconnection.commit()
    finally:
        if connection_cursor:
            connection_cursor.close()

def findrecordlength(ratingstablename,connection_cursor):
    connection_cursor.execute("SELECT distinct Rating from %s" %(ratingstablename))
    data=connection_cursor.fetchall()
    data_list=[]
    for row in data:
        data_list.append(row[0])
    return sorted(data_list)





def rangepartition(ratingstablename, numberofpartitions, openconnection):
    connection_cursor=openconnection.cursor()
    rating_record=findrecordlength(ratingstablename,connection_cursor)
    rating_size=len(rating_record)
    record_length=rating_size/numberofpartitions
    print record_length
    table_index_range=[]
    for i in range(0, numberofpartitions):
        table_index_range.append([])
    count=0
    p=0
    while(count < numberofpartitions):
        if (count==numberofpartitions-1):
            table_index_range[count].extend(rating_record[p:])
            break
        table_index_range[count].extend(rating_record[p:p+record_length])
        count+=1
        p=p+record_length
    connection_cursor.execute("drop table if exists meta_range")
    connection_cursor.execute("CREATE TABLE meta_range (TABLE_NAME varchar, VALUE real)")
    for i in range(0,numberofpartitions):
        connection_cursor.execute("drop table if exists range_part%d" %(i))
        connection_cursor.execute("CREATE TABLE range_part%d (UserID REAL, MovieID REAL, Rating REAL)" %(i))
        values = table_index_range[i]
        for value in values:
            connection_cursor.execute("INSERT INTO meta_range (TABLE_NAME,VALUE) VALUES ('range_part%d',%f) " %(i,value))
    connection_cursor.execute("select * from meta_range")
    new_data = connection_cursor.fetchall()
    for dat in new_data:
        connection_cursor.execute("INSERT INTO  %s (UserID,MovieID,Rating) select  * from %s  where rating = %f " %(dat[0], ratingstablename,float(dat[1])))







def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    connection_cursor=openconnection.cursor()
    rating_record=findrecordlength(ratingstablename,connection_cursor)
    rating_size=len(rating_record)
    record_length=rating_size/numberofpartitions
    table_index=[]
    for i in range(0, numberofpartitions):
        table_index.append([])
    for i in range(0,rating_size):
        table_index[i%(numberofpartitions)].append(rating_record[i])
    connection_cursor.execute("drop table if exists meta_roundrobin")
    connection_cursor.execute("CREATE TABLE meta_roundrobin (TABLE_NAME varchar, VALUE real)")
    for i in range(0,numberofpartitions):
        connection_cursor.execute("drop table if exists rrobin_part%d" %(i))
        connection_cursor.execute("CREATE TABLE rrobin_part%d (UserID REAL, MovieID REAL, Rating REAL)" %(i))
        values = table_index[i]
        for value in values:
            connection_cursor.execute("INSERT INTO meta_roundrobin (TABLE_NAME,VALUE) VALUES ('rrobin_part%d',%f) " %(i,value))
    connection_cursor.execute("select * from meta_roundrobin")
    new_data=connection_cursor.fetchall()
    for dat in new_data:
        connection_cursor.execute("INSERT INTO  %s (UserID,MovieID,Rating) select  * from %s  where rating = %f " %(dat[0], ratingstablename,float(dat[1])))






def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    connection_cursor=openconnection.cursor()
    connection_cursor.execute("select table_name from meta_roundrobin rm where rm.value = %f" %(rating))
    insert_data=connection_cursor.fetchall()
    print insert_data
    for ins_dat in insert_data:
        final_value=ins_dat[0]
    end_value=final_value.split("rrobin_part")
    print end_value[0]
    print end_value[1]
    connection_cursor.execute("INSERT INTO rrobin_part%d (UserID,MovieID,Rating) VALUES (%f,%f,%f) " %(float(end_value[1]),userid,itemid,rating))


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    connection_cursor=openconnection.cursor()
    connection_cursor.execute("select table_name from meta_range rm where rm.value = %f" %(rating))
    insert_data=connection_cursor.fetchall()
    print insert_data
    for ins_dat in insert_data:
        final_value=ins_dat[0]
    end_value=final_value.split("range_part")
    print end_value[0]
    print end_value[1]
    connection_cursor.execute("INSERT INTO range_part%d (UserID,MovieID,Rating) VALUES (%f,%f,%f) " %(float(end_value[1]),userid,itemid,rating))



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass

def deletepartitionsandexit(openconnection):
    connection_cursor=openconnection.cursor()
    connection_cursor.execute("select * from meta_roundrobin")
    new_data = connection_cursor.fetchall()
    list=set()
    #print new_data
    for dat in new_data:
        list.add(dat[0])
    for final_list in list:
        connection_cursor.execute("drop table %s " % (final_list))
    connection_cursor.execute("drop table meta_roundrobin")
    connection_cursor.execute("select * from meta_range")
    range_data=connection_cursor.fetchall()
    range_list=set()
    for range_dat in range_data:
        range_list.add(range_dat[0])
    for fin_list in range_list:
        connection_cursor.execute("drop table %s" %(fin_list))
    connection_cursor.execute("drop table meta_range")




if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            #loadratings("Ratings",'ratings.dat', con)
            #loadratings("Ratings",'test_data.dat', con)
            #for partition_number in range(1,11):
            rangepartition("Ratings",partition_number,con)
            #roundrobininsert("Ratings",10,10,3,con)
            #rangeinsert("Ratings",10,10,5,con)
            #for partition_number in range(1,11):
            #roundrobinpartition("Ratings", partition_number, con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
