from darts.models import XGBModel
from darts import TimeSeries
import numpy as np
from matplotlib import pyplot as plt
import datetime

import pandas as pd
import vaex as vx

instanceSpec = {
    'vcpu': 4,
    'cpuPerf': 3.1,
    'memory': 16,
    'network': 10
}
instanceSpec['cpuRes'] = instanceSpec['vcpu'] * instanceSpec['cpuPerf']
specList = ['cpuRes'] + list(instanceSpec.keys())[2:]

ins = {x: instanceSpec[x] for x in specList}
optiSpec = instanceSpec.copy()

df = vx.open('dataset/alibabaClusterTrace/2018/machine_usage.hdf5')
df = df[-187_962:]

datetimeList = []
now = datetime.datetime.now()
for i in range(len(df)):
    datetimeList.append(now)
    now = now - datetime.timedelta(minutes=1)
df = df.to_pandas_df()
df = df.interpolate(method='linear')
df['datetime'] = list(reversed(datetimeList))
df['net'] = df[['net_in', 'net_out']].mean(axis=1)

metricClass = ['cpu_util_percent', 'mem_util_percent', 'net']
print(df[['datetime'] + metricClass])

for i, metric in enumerate(metricClass):
    series = TimeSeries.from_dataframe(df, 'datetime', metric)

    train, val = series[:-288], series[-288:]

    model = XGBModel(lags=2016)
    model.fit(train)

    prediction = model.predict(len(val))

    peak = max(prediction.values())

    optiSpec[specList[i]] = peak / 100 * instanceSpec[specList[i]]

print(optiSpec)
