from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fwm_declarative_models import Base
import MySQLdb

class Config(object):

    # Database credentials
    FWM_DB = 'fwm'
    FWM_DB_USER = 'fwm'
    FWM_DB_PASSWD = 'Uj7iathime'
    FWM_DB_HOST = '172.16.1.53'

    def initDB(self):
        # create an engine
        self.engine = create_engine('mysql://'+self.FWM_DB_USER+':'+self.FWM_DB_PASSWD+'@'+self.FWM_DB_HOST) # connect to server
        # Create a Session
        self.Session = sessionmaker(bind=self.engine)
        # Select the database
        self.engine.execute('USE '+self.FWM_DB)
        self.Base = Base

    def create_tables(self):
        # Create all the tables
        self.Base.metadata.create_all(self.engine)

class Database:
    host = '172.16.1.53'
    user = 'fwm'
    passwd = 'Uj7iathime'
    db = 'fwm'

    def __init__ (self):
        self.connection = MySQLdb.connect(self.host, self.user, self.passwd, self.db)
        self.cursor = self.connection.cursor()

    # Insert into the database
    def insert(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Exception, e:
            print 'I am here ..', str(e)
            self.connection.rollback()
    
    # Insert without commiting into the database
    def insert_no_commit(self, query):
        try:
            self.cursor.execute(query)
        except:
            self.connection.rollback()
  
    # Insert with no commit and data as an argument
    def insert_no_commit_new(self, query, data):
        try:
            self.cursor.execute(query, data)
        except Exception, e:
            print 'I am getting here ...', str(e)
            self.connection.rollback()
   
    # Insert bulk without commiting into the database
    def insert_many_no_commit(self, query):
        try:
            self.cursor.executemany(query)
        except:
            self.connection.rollback()
    
    # Insert bulk without commiting into the database and data as an argument
    def bulk_insert_no_commit(self, query, data):
        try:
            self.cursor.executemany(query, data)
        except:
            self.connection.rollback()
    
    # Explicit commit
    def explicit_commit(self):
        self.connection.commit()

    # Create the table
    def createTable(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            print 'There is an exception'
            self.connection.rollback()

    # Delete the table
    def deleteTable(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            self.connection.rollback()

    # Check if a table already exists
    def checkTableExists(self, tableName):
        query = """
                SHOW TABLES LIKE '%s'
                """ % tableName

        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result:
            return True

        return False

    def query(self, query):
        cursor = self.connection.cursor( MySQLdb.cursors.DictCursor )
        cursor.execute(query)
        return cursor.fetchall()

    def query_new(self, query, data):
        cursor = self.connection.cursor( MySQLdb.cursors.DictCursor )
        cursor.execute(query, (data,))
        return cursor.fetchall()
    
    def query_tuple(self, query, data):
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, data)
        return cursor.fetchall()

    def __del__(self):
        self.connection.close()
