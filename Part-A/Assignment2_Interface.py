#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading
import pandas as pd
import numpy as np

# Do not close the connection inside this file i.e. do not perform openConnection.close()

def runQueryInThread(query, conn):
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()

class threadImpl(threading.Thread):
    def __init__(self, name, query, conn):
        threading.Thread.__init__(self)
        self.name = name
        self.query = query
        self.conn = conn
    
    def run(self):
        runQueryInThread(self.query, self.conn)

def get_rect_median(rectsTable, conn):
    with conn.cursor() as cur:
        q = "SELECT * FROM {0} WHERE longitude1 in (SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY longitude1) FROM {1})".format(rectsTable, rectsTable)
        cur.execute(q)
        data = cur.fetchone()
        return data

def rect_partition(rectsTable, conn):
    with conn.cursor() as cur:
        med_data = get_rect_median(rectsTable, conn)
        q1 = "CREATE TABLE Rect1 AS SELECT * FROM {0} WHERE latitude2<={1}".format(rectsTable, med_data[3])
        q2 = "CREATE TABLE Rect1 AS SELECT * FROM {0} WHERE latitude1>={1}".format(rectsTable, med_data[1])
        q3 = "CREATE TABLE Rect1 AS SELECT * FROM {0} WHERE longitude2<={1}".format(rectsTable, med_data[2])
        q4 = "CREATE TABLE Rect1 AS SELECT * FROM {0} WHERE longitude1>={1}".format(rectsTable, med_data[0])

        cur.execute(q1)
        cur.execute(q2)
        cur.execute(q3)
        cur.execute(q4)

        rFrags = ['Rect1', 'Rect2', 'Rect3', 'Rect4']
    conn.commit()
    return rFrags

def point_partition(ptsTable, rectsTable, conn):
    with conn.cursor() as cur:
        med_data = get_rect_median(rectsTable, conn)
        q1 = "CREATE TABLE Point1 AS SELECT * FROM {0} WHERE latitude<={1}".format(ptsTable, med_data[3])
        q2 = "CREATE TABLE Point2 AS SELECT * FROM {0} WHERE latitude>={1}".format(ptsTable, med_data[1])
        q3 = "CREATE TABLE Point3 AS SELECT * FROM {0} WHERE longitude<={1}".format(ptsTable, med_data[2])
        q4 = "CREATE TABLE Point4 AS SELECT * FROM {0} WHERE longitude>={1}".format(ptsTable, med_data[0])

        cur.execute(q1)
        cur.execute(q2)
        cur.execute(q3)
        cur.execute(q4)

        pFrags = ['Point1', 'Point2', 'Point3', 'Point4']

def createThreads(executeList, conn):
    t1 = threadImpl("t1", executeList[0], conn)
    t2 = threadImpl("t2", executeList[1], conn)
    t3 = threadImpl("t3", executeList[2], conn)
    t4 = threadImpl("t4", executeList[3], conn)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()

def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    #Implement ParallelJoin Here.
    executeList = []
    with openConnection.cursor() as cur:
        query = "CREATE TABLE Output{0} AS SELECT Count(*) AS points_count, {1}.geom from {2}, {1} WHERE ST_contains({1}.geom, {2}.geom) GROUP BY {1}.geom ORDER BY points_count ASC"

        dropTables(openConnection)
        rFrags = rect_partition(rectsTable, openConnection)
        pFrags = point_partition(pointsTable, rectsTable, openConnection)
        for i in range(1,5):
            executeList.append(query.format(i, rFrags[i-1], pFrags[i-1]))
        createThreads(executeList, openConnection)
        main_query = """SELECT SUM(points_count) as points_count FROM (SELECT * FROM Output1 UNION SELECT * FROM Output2 UNION SELECT * FROM Output3 UNION SELECT * FROM Output4) as total
                        GROUP BY total.geom ORDER BY points_count"""
        cur.execute(main_query)
        data = cur.fetchall()
        df = pd.DataFrame(data)
        np.savetxt(r'output_part_a.txt', df.values, fmt="%d")

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
        print("Deleted tables")
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


