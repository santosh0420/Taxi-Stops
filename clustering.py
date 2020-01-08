import pandas as pd
from datetime import datetime
import math
from matplotlib import pyplot as plt
import plotly
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
from sklearn.cluster import AgglomerativeClustering
from sklearn import cluster
import numpy as np


def main():
	path = '/home/s/Documents/Taxi/Taxi-Stops/stops.csv'
	df = pd.read_csv(path)
	df = df.sort_values('Latitude')
	lon0 = df['Longitude']
	lat0 = df['Latitude']
	time1 = [t[1:-1].split(', ') for t in list(df['Time'])]
	# print(time1)
	vehicle1 = df['Vehicle_No']
	n = len(df.index)
	lon = np.asarray(lon0[:n]).reshape(-1,1)
	lat = np.asarray(lat0[:n]).reshape(-1,1)
	points = np.concatenate((lat, lon), axis=1)*(6378137/180)*math.pi
	radius = input('Enter the raduis of cluster(Recommended 20-25 meters): ')
	# clustering = cluster.OPTICS(min_samples=5, max_eps=5, metric='euclidean', xi=0.05).fit(points)
	clustering = cluster.Birch(threshold=int(radius), branching_factor=500, n_clusters=None, compute_labels=True, copy=True).fit(points)
	# clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=40).fit(points)
	col = []
	cent_lat1 = []
	cent_lon1 = []
	min_taxis = input('Enter minimum number of Taxis for a location to be considered a stop: ')
	res  = dict(collections.Counter(clustering.labels_))
	for key, value in res.items():
			cent_lat1.append(clustering.subcluster_centers_[key][0]*(180/(6378137*math.pi)))
			cent_lon1.append(clustering.subcluster_centers_[key][1]*(180/(6378137*math.pi)))
			col.append(len(cent_lat1))
	num_clusters = len(cent_lat1)
	df = pd.DataFrame(list(zip(cent_lon1, cent_lat1)), columns = ['Longitude', 'Latitude'])
	df.to_csv('centers.csv', index = False)
	print('Cluster centers are stored in "centers.csv"')
	lat1 = []
	lon1 = []
	col = []
	time = [[] for i in range(num_clusters)]
	vehicle = [[] for i in range(num_clusters)]
	for i in range(n):
		if(res[clustering.labels_[i]]>int(min_taxis)):
			lat1.append(points[i][0]*(180/(6378137*math.pi)))
			lon1.append(points[i][1]*(180/(6378137*math.pi)))
			time[clustering.labels_[i]].extend(time1[i])
			vehicle[clustering.labels_[i]].append(vehicle1[i])
			col.append(clustering.labels_[i])
	print(time)	
	df = pd.DataFrame(list(zip(lon1, lat1)), columns = ['Longitude', 'Latitude'])
	df.to_csv('clusters.csv', index = False)
	print('Clusters are stored in "clusters.csv"')
	fig = go.Figure(data=go.Scatter(
    x=lat1,
    y=lon1,																		
    mode='markers',
    marker=dict(color=col),
    text = col))
	# plotly.offline.plot(fig, filename='stops.html')
	print('Plot is stored in "stops.html".')
if __name__ == '__main__':
	main()