import pandas as pd
from datetime import datetime
import math
import plotly.express as px
import plotly.graph_objects as go
import plotly
import collections
import statistics
import os
import threading 
from multiprocessing.dummy import Pool as ThreadPool
import ray
import psutil
import time
import itertools
		

num = 0
path = ''

def rad(d):
    return float(d) * math.pi/180.0

#distance between two corrdinates
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

def average_time(t1, t2):
	t1 = t1.split(' ')[1][:-4]
	t2 = t2.split(' ')[1][:-4]
	hour1 = int(t1.split(':')[0])
	hour2 = int(t2.split(':')[0])
	minute1 = int(t1.split(':')[1])
	minute2 = int(t2.split(':')[1])
	# average = ''
	# if(hour2<hour1):
	# 	if(30+(minute2+minute1)/2>60):
	# 		average+=str(hour1)+':'+str(int((30+(minute2+minute1)/2)-60))
	# 	else:
	# 		average+=str(hour2)+':'+str(int((minute2+minute1)/2))
	# else:
	# 	average+=str(hour2)+':'+str(int((minute2+minute1)/2))
	# return average+':0'
	return ((hour1*60+minute1)+(hour2*60+minute2))/2

#To find mean of points in dc window for a perticular taxi
def removeDuplicates(lon, lat, time):
	dc = 50
	visited = [False for i in range(len(lon))]
	window = changeinlat(dc)
	count=0
	lon1 = []
	lat1 = []
	time1 = []
	for i in range(len(lon)):
		if(visited[i]==True):
				continue
		time2 = [time[i]]
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
				time2.append(time[j])
		# if(len(cluster_lat)>1):
			# print(statistics.stdev(cluster_lat))
		lon1.append(mean_lon)
		lat1.append(mean_lat)
		time1.append(time2)
		count+=1
	return lon1, lat1, time1


#Difference in two time stamps in Seconds
def diffinsec(t1, t2):
	if(t1<t2):
		return 0
	diff = str(datetime.strptime(t1, '%H:%M:%S') - datetime.strptime(t2, '%H:%M:%S'))
	return  int(diff.split(':')[0])*3600+int(diff.split(':')[1])*60+int(diff.split(':')[2])

@ray.remote
def find_stoppoints(file):
	global path
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
	time = []
	names = []
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
			time.append(average_time(df['DateTimeReceived'][i], df['DateTimeReceived'][i+steps-1]))
			temp_lat.append(mean_lat)
			temp_lon.append(mean_lon)
		i+=steps
	if(len(temp_lon)==0):
		return [temp_lon, temp_lat, time, names]
	temp = removeDuplicates(temp_lon, temp_lat, time)
	global num
	num+=1
	print(str(num)+' .................   Size  '+str(os.stat(path+'/'+file).st_size/1000)+' Kb ................ '+file)
	temp = temp+ (file[:-4],)
	return temp
   	
def main():
	start = time.time()
	global path
	# path = input('Enter Splitted csv files folder path:')
	path = '/home/s/Documents/Taxi/26Dec'
	files = sorted(os.listdir(path))
	lon = []
	lat = []
	n = len(files)
	num_cpus = psutil.cpu_count(logical=False)
	print('Your system has '+str(2*num_cpus)+" CPUs")
	ray.init(num_cpus=num_cpus*2)
	# pool = ThreadPool(50)
	# stops = pool.map(find_stoppoints, files)
	# pool.close()
	# pool.join()
	# print(stops)
	result = []
	result = ray.get([find_stoppoints.remote(f) for f in files])
	time1 = []
	names = []
	print(len(result[0]))
	for i in range(len(result)):
		if(len(result[i][0])!=0):
			for j in range(len(result[i][0])):
				lon.append(result[i][0][j])
				lat.append(result[i][1][j])
				time1.append(result[i][2][j])
				names.append(result[i][3])
	print(time1)
	time2 = merged = list(itertools.chain(*time1))
	x = [1 for i in range(len(time2))]
	fig = go.Figure(data=go.Scatter(x=time2,y=x,mode='markers'))
	plotly.offline.plot(fig, filename='stops.html')
	df = pd.DataFrame(list(zip(lon, lat, names, time1)), columns = ['Longitude', 'Latitude', 'Vehicle_No', 'Time'])
	df.to_csv('stops.csv', index = False)
	print('Complete')
	end = time.time()
	print(" Time elapsed: "+str(end-start))

if __name__ == '__main__':
	main()

