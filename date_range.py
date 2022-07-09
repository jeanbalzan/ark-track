import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

date_range = list(pd.date_range(start = "2018-01-01", end = "2022-06-30", freq='2M').strftime("%Y-%m-%d"))

datez1 = []

for i in range(len(date_range)-1):
    datez1.append([date_range[i], date_range[i+1]])
    date_range[i+1] =  (datetime.datetime.strptime(date_range[i+1], "%Y-%m-%d")+relativedelta(days=+1)).strftime("%Y-%m-%d")

print(datez1)    
