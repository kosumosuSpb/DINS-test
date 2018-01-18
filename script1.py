import csv
from datetime import datetime
import pymysql
import settings

data = {}
anomaly = False #это решение для первой части тестового задания, аномалия не использовалась
with open('raw_data.csv', newline='') as myFile:
    reader = csv.reader(myFile)
    for i, row in enumerate(reader):
        ts, apiname, method, code = row
        if i > 0 and code[0] == '5':
            ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S,%f')
            ts = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0)
            key = ts, apiname, method
            data[key] = data.get(key, 0) + 1

db = pymysql.connect(**settings.db_params)
cursor = db.cursor()
for key, val in sorted(data.items()):
    ts, apiname, method = key
    sql = """INSERT INTO respondtable(timeframe_start, api_name, http_method, count_http_code_5xx, is_anomaly)
            VALUES (%s, %s, %s, %s, %s)
            """
    cursor.execute(sql, (ts, apiname, method, val, anomaly))
db.commit()
db.close()
