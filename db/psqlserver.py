#!/usr/bin/python3

"""
Esta clase es utlizada crear conexiones a sqlserver. 
Y tiene varios metodos para hacer una consulta select y retorna los datos en un dataframe o un diccionario.
"""
import sys
import pandas as pd
import pyodbc
import os
from dbutils.pooled_db import PooledDB

class SqlServerConnection():

    def __init__(self):
        self.server = ''
        self.database = ''
        self.username = ''
        self.password = ''
        self.port = ''
        self.driver = ''
        self.Pool = ''
       
    def createPool(self):
        self.Pool = PooledDB(creator=pyodbc, mincached=2, maxcached=5, maxshared=3, maxconnections=20, blocking=True, DRIVER=self.driver, SERVER=self.server, PORT=self.port, DATABASE=self.database, UID=self.username, PWD=self.password)

    # Query like insert, update or delete. It Returns rows affected
    def executeCommand(self, query):
        conn = self.Pool.connection()
        cur = conn.cursor()
        #cur.execute(query)
        rows_affected = cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
        return rows_affected
        
    # Return results from a query
    def executeQuery(self, conn ,query):
        cursor = conn.cursor()
        if cursor is not None:
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        else:
            print('Connection not established.')
            return []

    def executeQueryToDic(self, query):
        if self.cursor is not None:
            try:
                self.cursor.execute(query)
                results = self.cursor.fetchall()
                return dict(results)
            except ValueError:
                print('Only query that return two columns')
                return {}
        else:
            print('Connection not established.')
            return []

    # Transform to pandas object
    def queryToPandas(self, query):
        try:
            conn = self.Pool.connection()
            df = pd.read_sql_query(query,conn)
            conn.close()
            return df
        except Exception as e:
            print('Failed on create dataframe operation. {0}'.format(e))
            sys.exit()