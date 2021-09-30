import sqlite3
from datetime import datetime
import threading
import re
import pandas as pd
from threading import Thread, currentThread
import time
import pymysql

class Trigger(threading.Thread):
    def __init__(self, row, conn):
        super().__init__()
        self.row = row
        self.conn = pymysql.connect(host='210.121.218.5', user='zabbix', passwd='zabbix', port=3306, db='test', charset='euckr')
        self.curs = self.conn.cursor()

    def run(self):
        now = datetime.now()
        if now.minute % 10 <= 5:
            from_min = str(now.minute // 10) + "0"
            to_min = str(now.minute // 10) + "4"
        else:
            from_min = str(now.minute // 10) + "5"
            to_min = str(now.minute // 10) + "9"
        from_clock = str(now.year) + "-" + str(now.month) + "-" + str(now.day) + " " + str(now.hour) + ":" + from_min
        to_clock = str(now.year) + "-" + str(now.month) + "-" + str(now.day) + " " + str(now.hour) + ":" + to_min

        if self.row[1][6] == "in":
            first = int(self.row[1][7])
            second = self.row[1][4]
            third = int(self.row[1][7])*0.7
        elif self.row[1][6] == "out":
            first = int(self.row[1][8])
            second = self.row[1][4]
            third = int(self.row[1][8])*0.7

        sql = 'SELECT itemid, FROM_UNIXTIME(clock) AS clock, VALUE, (value / {} * 100) as per  \
            FROM ZABBIXDB.history_uint \
            WHERE itemid = {} AND FROM_UNIXTIME(clock) >= "{}" AND FROM_UNIXTIME(clock) <= "{}" AND value >= {}'.format(first, second, from_clock, to_clock, third)
        # print(sql)
        self.curs.execute(sql)
        rows = self.curs.fetchall()
        if rows == ():
            pass
        else :
            print(rows)
        self.conn.close()

        print("last를 위한: ", datetime.now().hour, datetime.now().minute, datetime.now().second)

# print("main thread start")
if __name__ == '__main__':

    print(datetime.now().hour,datetime.now().minute, datetime.now().second)
    conn = pymysql.connect(host='210.121.218.5', user='zabbix', passwd='zabbix', port=3306, db='test', charset='euckr')
    curs = conn.cursor()
    sql = 'SELECT * FROM ZABBIXDB.new_db WHERE ingre != "" AND engre != ""'
    curs.execute(sql)
    rows = curs.fetchall()
    df = pd.DataFrame(rows)
    conn.close()

    print(datetime.now().hour,datetime.now().minute, datetime.now().second)
    for row in df.iterrows():
        t = Trigger(row, conn)                # sub thread 생성
        t.start()                       # sub thread의 run 메서드를 호출
    print(datetime.now().hour,datetime.now().minute, datetime.now().second)
    # conn.close()
