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
		

num = 0

def rad(d):
    return float(d) * math.pi/180.0

#distance between two latitudes
def GetDistance(lng1,lat1,lng2,lat2):
	EARTH_RADIUS=6378.137
	radLat1 = rad(lat1)
	radLat2 = rad(lat2)
	a = radLat1 - radLat2
	b = rad(lng1) - rad(lng2)
	s = 2 * math.asin(math.sqrt(math.pow(math.sin(a/2),2) +math.cos(radLat1)*math.cos(radLat2)*math.pow(math.sin(b/2),2)))
	s = s * EARTH_RADIUS
	s = round(s * 10000,2) / 10;
	return s

#change in latitude for a distance(dist)
def changeinlat(dist):
	EARTH_RADIUS=6378.137
	ch = ((dist*180)/(1000*EARTH_RADIUS))/(math.pi)
	return ch

#To find mean of points in dc window for a perticular taxi
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
		lon1.append(mean_lon)
		lat1.append(mean_lat)
		count+=1
	# x = [i for i in range(len(lon1))]
	# fig = px.scatter(x=lon1,y=lat1)
	# fig.show()
	return [mean_lon, mean_lat]

#Difference in two time stamps in Seconds
def diffinsec(t1, t2):
	if(t1<t2):
		return 0
	diff = str(datetime.strptime(t1, '%H:%M:%S') - datetime.strptime(t2, '%H:%M:%S'))
	return  int(diff.split(':')[0])*3600+int(diff.split(':')[1])*60+int(diff.split(':')[2])

@ray.remote
def find_stoppoints(file):
	path = '/home/s/Taxi/1Jan'
	dt = 15
	dc = 50
	df = pd.read_csv(path+'/'+file)
	data_freq = []
	i=0
	temp_lat= []
	temp_lon = []
	while i<(len(df.index)-1):
		data_freq.append(diffinsec(df['DateTimeReceived'][i].split(' ')[1][:-4], df['DateTimeReceived'][i+1].split(' ')[1][:-4]))
		i+=1
	i=0
	while i<len(df.index):
		if(len(df.index)<10):
			break
		steps=1
		terminate = False
		if(i+1==len(df.index)-1):
			break
		while(steps*statistics.mean(data_freq[i:i+steps])<dt*60):
			steps+=1
			if(i+steps>=len(df.index)-3):
				terminate = True
				break
		if terminate:
			break
		dist = 0
		j=1
		lat = []
		lon = []
		for j in range(1,steps-1):
			if(i+j==len(df.index)):
				break
			d12 = GetDistance(df['Longitude'][i+j], df['Latitude'][i+j], df['Longitude'][i+j-1], df['Latitude'][i+j-1])
			if(d12/data_freq[i+j-1]*(18/5)>70):
				continue
			dist+=d12
			lat.append(df['Latitude'][i+j])
			lon.append(df['Longitude'][i+j])
		if(len(lat)<3):
			i+=steps
			continue
		mean_lat = statistics.mean(lat)
		std_lat = statistics.stdev(lat)
		mean_lon = statistics.mean(lon)
		std_lon = statistics.stdev(lon)
		displacement = GetDistance(lon[-1], lat[-1],lon[0], lat[0])
		velocity = float(str((displacement/(statistics.mean(data_freq[i:i+steps])*steps))*(18/5)))
		calculated_speed = (dist/(statistics.mean(data_freq[i:i+steps])*steps))*(18/5)
		stdev = GetDistance(mean_lon, mean_lat, mean_lon+std_lon, mean_lat+std_lat)
		mean_speed = statistics.mean(df['Speed'][i:i+steps])
		if(mean_speed<0.7 and velocity<0.7 and stdev<dc):
			temp_lat.append(mean_lat)
			temp_lon.append(mean_lon)
		i+=steps
	if(len(temp_lon)==0):
		return ['NULL', 'NULL']
	temp = removeDuplicates(temp_lon, temp_lat)
	global num
	num+=1
	print(str(num)+' .................   Size  '+str(os.stat(path+'/'+file).st_size/1000)+' Kb ................ '+file)
	return temp
        #print(temp)
	# x = [i for i in range(len(lat))]
	# fig = px.scatter(x=lon,y=lat)
	# fig.show()

def main():
	start = time.time()
	path = '/home/s/Taxi/1Jan'
	files = sorted(os.listdir(path))[:25]
	lon = []
	lat = []
	n = len(files)
	num_cpus = psutil.cpu_count(logical=False)
	print(num_cpus)
	ray.init(num_cpus=24)
	# pool = ThreadPool(50)
	# stops = pool.map(find_stoppoints, files)
	# pool.close()
	# pool.join()
	# print(stops)
	result = []
	result = ray.get([find_stoppoints.remote(f) for f in files])

	for i in range(len(result)):
		if(result[i][0]!='NULL'):
			lon.append(result[i][0])
			lat.append(result[i][1])
	df = pd.DataFrame(list(zip(lon, lat)), columns = ['Longitude', 'Latitude'])
	df.to_csv('stops.csv')
	print(result)
	print('Complete')
	end = time.time()
	print(" Time elapsed: "+str(end-start))
	#kjfakjfakj
	#kjsdnkjsdfkjsd
	#ksdkjsdfkjsdf
	#dfjdsfdjk
	#sdkfsdf

if __name__ == '__main__':
	main()

