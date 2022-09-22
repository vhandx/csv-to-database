"""
    Quy trinh import file csv vao Database
    0, Khoi tao thong tin
    1, Tao def 10p quet file 1 lan trong 1 folder
        Neu co file => Doc file => Day queue => Jobs lang nghe queue day database
        Neu co loi => Day queue loi
        Cuoi => Tao file error (Neu co)
        Move file vao thu muc archive
"""
import sys
import csv
import os
import pandas
import logging
import math
import datetime


from dotenv import load_dotenv, find_dotenv
from common.db_connection import SqlServerConnection
from entity.op00 import Op00
from pathlib import Path

class Op00Service:

    load_dotenv(find_dotenv())
    pathImport = os.getenv("OP_00_IMPORT")
    pathArchive = os.getenv("OP_00_ARCHIVE")
    pathError = os.getenv("OP_00_ERROR")
    maxSize = os.getenv("BATH_MAXSIZE")
    sqlServerConn = os.getenv("SQL_SERVER_CONN")

    def __init__(self) -> None:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        pass

    def process(self):
        
        logging.info("-----------------Start OP00.process()-----------------")

        # Doc file trong folder
        dir_list = os.listdir(self.pathImport)

        for f in dir_list:
            if os.path.splitext(f)[1].lower() in ['.csv']:
                logging.info(f)
                self.csvProcess(f)
            if os.path.splitext(f)[1].lower() in ['.xls','.xlsx']:
                logging.info(f)
                # self.excelProcess(f)

    def csvProcess(self, file):

        try:
            logging.info("-----------------Start OP00.csvProcess()-----------------")
            # Read csv file
            pathFull = self.pathImport + file
            pathArchive = self.pathArchive + file

            df = pandas.read_csv(pathFull, header=4, encoding='utf8', sep='\t')

            self.toDatabase(df)
            
            self.fileMove(pathFull, pathArchive)

            logging.info("-----------------End OP00.csvProcess()-----------------")
        except:
            logging.error("-----------------Error OP00.csvProcess()-----------------")
        

    def excelProcess(self, file):
        try:

            logging.info("-----------------Start OP00.excelProcess()-----------------")
            
            pathFull = self.pathImport + file
            pathArchive = self.pathArchive + file


            df = pandas.read_excel(pathFull,header=4,sheet_name='Sheet1')
            
            self.toDatabase(df)

            self.fileMove(pathFull, pathArchive)

            logging.info("-----------------End OP00.excelProcess()-----------------")
        except:
            logging.error("-----------------Error OP00.excelProcess()-----------------")

    def toDatabase(self, df):
        op00s = []
        op00ErrorS = []

        try:
            for i, r in df.iterrows():            
                op00 = Op00(r['No'],  r['Agent Code'], r['Agent Name'])

                if len(op00s) == int(self.maxSize) or i == len(df):
                    logging.info('Insert to SQL')
                    
                    sqlServer = SqlServerConnection(self.sqlServerConn)
                    conn = sqlServer.initConn()
                    cur = conn.cursor()

                    params = [(str(i.agentCode), f'{i.agentName}', i.no) for i in op00s]

                    # Comma-separated dataframe columns
                    sql = "insert into op_00 values (newid(), ?, ?, ?, 1, 'System', GETUTCDATE(), 'System', GETUTCDATE())"

                    try:
                        cur.executemany(sql, params)
                        conn.commit()
                    except (Exception) as error:
                        logging.error("Error: %s" % error)
                        conn.rollback()
                        cur.close()
                    cur.close()
                    conn.close()

                    op00s = []
                else:
                    # check
                    if (not op00.agentCode) or (math.isnan(op00.agentCode)):
                        op00ErrorS.append(op00)
                    else:
                        op00s.append(op00)
            if len(op00ErrorS) > 0 :
                self.objectToCsv(op00ErrorS)
        except:
            logging.error("-----------------Error OP00.toDatabase()-----------------")

    def fileMove(self, f, t):
        try:
            logging.info("-----------------{}--to--{}-------------".format(f, t))
            Path(f).rename(t)
        except:
            logging.error("-----------------Error OP00.fileMove()-----------------")


    def objectToCsv(self, objects):
        try:
            no = []
            agentCode = []
            agentName = [] 
            for o in objects:
                no.append(o.no)
                agentCode.append(o.agentCode)
                agentName.append(o.agentName)

            dict = {'No': no, 'Agent Code': agentCode, 'Agent Name': agentName}
            df = pandas.DataFrame(dict) 
            # saving the dataframe 
            
            errorPath = "{}Error{}.csv".format(self.pathError, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
            df.to_csv(errorPath) 
        except:
            logging.error("-----------------Error OP00.objectToCsv()-----------------")


