import pymysql
import settings
from statistics import mean, stdev
from collections import defaultdict

data = {}
counterlist = defaultdict(list)
sigma = {}

db = pymysql.connect(**settings.db_params)
cursor = db.cursor()
cursor.execute("SELECT timeframe_start, api_name, http_method, count_http_code_5xx, is_anomaly from respondtable")
for row in cursor.fetchall():
    ts, apiname, method, counter, anomaly = row
    name_met = apiname, method
    t_name_met = ts, apiname, method
    counterlist[name_met].append(counter)
    data[t_name_met] = counter
for key in counterlist.keys():
    if len(counterlist[key]) > 1:
        sigma[key] = stdev(counterlist[key]), mean(counterlist[key])
for key, val in data.items():
    ts, apiname, method = key
    name_met = apiname, method
    if sigma.get(name_met) != None and abs(sigma[name_met][1] - val) > sigma[name_met][0]*3:
        cursor.execute("UPDATE respondtable SET is_anomaly = 1 WHERE timeframe_start = %s AND api_name = %s AND http_method = %s", (ts, apiname, method))
db.commit()
db.close()
