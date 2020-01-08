import os

path = '/home/s/Documents/Taxi/temp'
files = os.listdir(path)
for i in range(len(files)):
	if(name=='NONE'):
		continue
	j = files[i].find('DL')
	new_name = files[i][j:]
	os.rename(path+'/'+files[i], path+'/'+new_name)