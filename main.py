"""
    Chuong trinh thuc thi doc => check => xu ly loi => day vao database theo tung row
"""
import schedule
import time

from service.op00_service import Op00Service

def job00():
    op00 = Op00Service()
    op00.process()


schedule.every(10).seconds.do(job00)

while True:
    schedule.run_pending()
    time.sleep(1)