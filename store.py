import sqlite3
import pandas as pd
import os

class StoreDB:
    def __init__(self, path_db, create=False):
        self.db_exists(path_db, create)
        
        return
    
    def connect(self):
        self.conn = sqlite3.connect(self.path_db)
        self.curs = self.conn.cursor()
        self.curs.execute("PRAGMA foreign_keys = ON;")
        return
    
    def close(self):
        self.conn.close()
        return
    
    def run_query(self, sql, params=None):
        self.connect()
        results = pd.read_sql(sql, self.conn, params = params)
        self.close()
        return results
    
    def db_exists(self, path_db, create):
        '''
        Check if the database file exists,
        if it does not, then either alert the user
        or create it if 'create' is True
        '''
        if os.path.exists(path_db):
            self.path_db = path_db
        else:
            if create == True:
                conn = sqlite3.connect(path_db)
                conn.close()
                print('Database created at', path_db)
            else:
                raise FileNotFoundError(path_db + ' does not exist.')
        return
    
    def drop_all_tables(self, are_you_sure=False):
        '''
        Drop all tables from the database
        '''
        self.connect()
        
        try:
            self.curs.execute("DROP TABLE IF EXISTS tOrderDetail;")
            self.curs.execute("DROP TABLE IF EXISTS tProd;")
            self.curs.execute("DROP TABLE IF EXISTS tOrder;")
            self.curs.execute("DROP TABLE IF EXISTS tCust;")
            self.curs.execute("DROP TABLE IF EXISTS tZip;")
            self.curs.execute("DROP TABLE IF EXISTS tState;")
        except Exception as e:
            self.close()
            raise e
        self.close()
        return
    
    def build_tables(self):
        '''
        Build all tables in the database,
        assuming they do not exist
        '''
        self.connect()
        
        sql = """
        CREATE TABLE tCust (
            cust_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first TEXT NOT NULL,
            last TEXT NOT NULL,
            addr TEXT NOT NULL,
            zip TEXT NOT NULL REFERENCES tZip(zip)
        )
        ;"""
        self.curs.execute(sql)
        
        sql = """
        CREATE TABLE tZip (
            zip TEXT PRIMARY KEY CHECK(length(zip)==5),
            city TEXT NOT NULL,
            state_id TEXT NOT NULL REFERENCES tState(state_id)
        )
        ;"""
        self.curs.execute(sql)
        
        
        sql = """
        CREATE TABLE tState (
            state_id TEXT PRIMARY KEY CHECK(length(state_id)==2),
            state TEXT NOT NULL
        )
        ;"""
        self.curs.execute(sql)
        
        sql = """
        CREATE TABLE tOrder (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            cust_id INTEGER NOT NULL REFERENCES tCust(cust_id),
            date TEXT NOT NULL CHECK(date LIKE '____-__-__')
        )
        ;"""
        self.curs.execute(sql)
        
        sql = """
        CREATE TABLE tOrderDetail (
            order_id INTEGER REFERENCES tOrder(order_id),
            prod_id INTEGER REFERENCES tProd(prod_id),
            qty INTEGER NOT NULL,
            PRIMARY KEY(order_id, prod_id)
        )
        ;"""
        self.curs.execute(sql)
        
        sql = """
        CREATE TABLE tProd (
            prod_id INTEGER PRIMARY KEY,
            prod_desc TEXT NOT NULL,
            unit_price NUMERIC NOT NULL
        )
        ;"""
        self.curs.execute(sql)
        
        self.close()
        return
    
    def load_lookup_tables(self):
        '''
        Load any data that does not change (i.e., tProd, tState, tZip)
        '''
        return
    
    def load_new_data(self):
        '''
        Check if there are any new sales files to load, and load them,
        then move them into a different folder, so we don't try to load them twice
        '''

        # Get a list of all new sales files

        # for each file:
        try:
                # for each row:
                    # get or create cust_id for this name/addr/zip combo
                    # get or create order_id for this cust_id/date combo
                    order_id = get_order_id(...)
            
                    # Fill in tOrderDetail
                    # INSERT INTO tOrderDetail (...) VALUES (:order_id,...)
        except...:
                # Detailed error handling and informative messages, i.e. what row
                # of data were we on
                
                # Undo all changes since the last commit
                conn.rollback()
        # move the file into a different directory
        
        return

    def get_customer_id(self, first,last,addr,zip):
        # Check if cust_id exists for this name/addr/zip combo

        # If so, return it (run a SELECT)
        # If not, create it (run an INSERT)
        # If needed, rerun the SELECT

        return cust_id

    def get_order_id(self, cust_id, date):
        # similar logic to the above
        return order_id