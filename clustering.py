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

def removeDuplicates(lon, lat):
	# x = [i for i in range(len(lat))]
	# fig = px.scatter(x=lon,y=lat)
	# fig.show()
	#Starts here
	dc = 50
	visited = [False for i in range(len(lon))]
	window = changeinlat(dc)
	count=0
	lon1 = []
	lat1 = []
	for i in range(len(lon)):
		if(visited[i]==True):
				continue
		cluster_lat = [lat[i]]
		cluster_lon = [lon[i]]
		mean_lat = lat[i]
		mean_lon = lon[i]
		visited[i] = True
		for j in range(len(lon)):
			if(i==j):
				continue
			if(visited[j]==True):
				continue
			#checking if point j in in the dc distance window
			if(mean_lon-window<lon[j]<mean_lon+window and mean_lat-window<lat[j]<mean_lat+window):
				visited[j] = True
				cluster_lon.append(lon[j])
				cluster_lat.append(lat[j])
				mean_lon = statistics.mean(cluster_lon)
				mean_lat = statistics.mean(cluster_lat)
		# if(len(cluster_lat)>1):
			# print(statistics.stdev(cluster_lat))
		if(len(cluster_lat)<30):
			continue
		lon1.append(mean_lon)
		lat1.append(mean_lat)
		count+=1
	# x = [i for i in range(len(lon1))]
	# fig = px.scatter(x=lon1,y=lat1)
	# fig.show()
	return lon1, lat1


df = pd.read_csv('/home/s/Documents/Taxi/stops/stops.csv')

lon = df['Longitude']
lat = df['Latitude']
lon1, lat1 = removeDuplicates(lon, lat)
fig = px.scatter(x=lon1,y=lat1)
fig.show()