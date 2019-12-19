import pandas as pd
from datetime import datetime
import math
import plotly.express as px
import plotly.graph_objects as go
import collections
import statistics
import os
import threading 
from multiprocessing.dummy import Pool as ThreadPool
import ray
import psutil
import time

df = pd.read_csv('/home/s/Documents/Taxi/stops/stops.csv')

lon = df['Longitude']
lat = df['Latitude']
fig = px.scatter(x=lon,y=lat)
fig.show()