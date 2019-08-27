import jaydebeapi as JDBC
import datetime
import pandas as pd
import numpy as np
import json
import os
import csv

# Set jar path for drivers
with open("jdbcConn.json") as jPath:
    dbparams = json.load(jPath)

def get_JDBC_connection():
    db = dbparams["my_database_name"]
    c = JDBC.connect(db["class_path"], db["jdbc_url"], [db["user"], db["passwd"]], db["jar_path"])
    print("\n\n\t\t\t\tLogged in to Teradata as %s" %db['user'])
    return c

def query_db(sql_string):
    conn = get_JDBC_connection()
    df = pd.read_sql(sql_string, conn)
    conn.close()
    print("\n\n\t\t\t\tLogged out of {}!".format(dbparams['my_database_name']))
    return df

def executeOn_db(sql_script):
    conn = get_JDBC_connection()
    f = open(sql_script, 'r')
    qry = " ".join(f.readlines())
    df = pd.read_sql(qry, conn)
    print("\n\n\t\t\t\tLogged out of {}!".format(dbparams['my_database_name']))
    return df

def writeTo_db(dataFile, tableName):
    conn = get_JDBC_connection()
    curs = conn.cursor()
    
    with open(dataFile, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)
        qry = """ INSERT INTO %s({0}) values({1}) """ %tableName
        qry = qry.format(','.join(columns), ','.join('?' * len(columns)))

        for data in reader:
            curs.execute(qry, data)

    print("\n\n\t\t\t\tData written to {}\n\n\t\t\t\tLogged out of {}!".format(tableName, dbparams['my_database_name']))
    curs.close()
    conn.close()

def writeDfTo_db(dataFrame, tableName):
    conn = get_JDBC_connection()
    curs = conn.cursor()

    cols = dataFrame.columns.tolist()
    data = dataFrame.values.tolist()
    data = dataFrame.fillna('')

    qry = """ INSERT INTO %s({0}) values({1}) """ %tableName
    qry = qry.format(','.join(cols), ','.join('?' * len(cols)))

    for row in data:
        curs.execute(qry, row)

    print("\n\n\t\t\t\tData written to {}}\n\n\t\t\t\tLogged out of {}!".format(tableName, dbparams['my_database_name']))
    curs.close()
    conn.close()
