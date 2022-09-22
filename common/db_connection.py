import psycopg2
import pyodbc


class SqlServerConnection:
    def __init__(self, connection):
        self.connection = connection

    def initConn(self):
        conn = pyodbc.connect(self.connection)
        return conn

class PosgerSqlConnection:
    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def initConn(self):
        conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port= self.port)
        return conn