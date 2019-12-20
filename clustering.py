import pandas as pd
from datetime import datetime
import math
from matplotlib import pyplot as plt
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
import numpy as np
import hdbscan


def main():
	df = pd.read_csv('/home/s/Taxi/stops/stops.csv')
	df = df.sort_values('Latitude')
	lon0 = df['Longitude']
	lat0 = df['Latitude']
	n = 10000
	lon = np.asarray(lon0[:n]).reshape(-1,1)
	lat = np.asarray(lat0[:n]).reshape(-1,1)
	points = np.concatenate((lat, lon), axis=1)*(6378137/180)*math.pi
	clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=40).fit(points)
	lat1 = []
	lon1 = []
	# plt.scatter(points[:, 0], points[:, 1], c=clustering.labels_,
	#                         cmap=plt.cm.nipy_spectral)
	# plt.show()
	res = collections.Counter(clustering.labels_)
	# print(res)
	# print(clustering.labels_)
	# print(res)
	col = []
	for i in range(n):
		if(res[clustering.labels_[i]]>6):
			lat1.append(points[i][0]*(180/(6378137*math.pi)))
			lon1.append(points[i][1]*(180/(6378137*math.pi)))
			col.append(clustering.labels_[i])
	# print(len(lat1))
	# print(lon1)
	# # lon1, lat1 = removeDuplicates(lon1, lat1)

	# fig = px.scatter(x=points[:, 0], y=points[:, 1], color =clustering.labels_)
	# fig = go.Figure(data=go.Scatter(
 #    x=lat1,
 #    y=lon1,
 #    mode='markers',
 #    marker=dict(color=col)))

	fig.show()

if __name__ == '__main__':
	main()