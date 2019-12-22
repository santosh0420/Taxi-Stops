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
	path = input('Stop points file path(CSV format):')
	df = pd.read_csv(path)
	df = df.sort_values('Latitude')
	lon0 = df['Longitude']
	lat0 = df['Latitude']
	n = 50000
	lon = np.asarray(lon0[:n]).reshape(-1,1)
	lat = np.asarray(lat0[:n]).reshape(-1,1)
	points = np.concatenate((lat, lon), axis=1)*(6378137/180)*math.pi
	# clustering = cluster.OPTICS(min_samples=5, max_eps=5, metric='euclidean', xi=0.05).fit(points)
	clustering = cluster.Birch(threshold=25, branching_factor=500, n_clusters=None, compute_labels=True, copy=True).fit(points)
	# clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=40).fit(points)
	lat1 = []
	lon1 = []
	min_taxis = input('Enter minimum number of Taxis for a location to be considered a stop: ')
	res  = dict(collections.Counter(clustering.labels_))
	for key, value in res.items():
		if(value>min_taxis):
			lat1.append(clustering.subcluster_centers_[key][0]*(180/(6378137*math.pi)))
			lon1.append(clustering.subcluster_centers_[key][1]*(180/(6378137*math.pi)))
			col.append(len(lat1))
	df = pd.DataFrame(list(zip(lon1, lat1)), columns = ['Longitude', 'Latitude'])
	df.to_csv('centers.csv', index = False)
	print('Cluster centers are stored in "centers.csv"')
	lat1 = []
	lon1 = []
	col = []
	for i in range(n):
		if(res[clustering.labels_[i]]>min_taxis):
			lat1.append(points[i][0]*(180/(6378137*math.pi)))
			lon1.append(points[i][1]*(180/(6378137*math.pi)))
			col.append(clustering.labels_[i])
	df = pd.DataFrame(list(zip(lon1, lat1)), columns = ['Longitude', 'Latitude'])
	df.to_csv('clusters.csv', index = False)
	print('Clusters are stored in "clusters.csv"')
	fig = go.Figure(data=go.Scatter(
    x=lat1,
    y=lon1,																		
    mode='markers',
    marker=dict(color=col),
    text = col))
	plotly.offline.plot(fig, filename='stops.html')
	print('Plot is stored in "stops.html".')
if __name__ == '__main__':
	main()