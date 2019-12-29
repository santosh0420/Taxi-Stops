import pandas as pd 
import ray
import time
import pandas
import os
import psutil

total_files = 0
target_path = ''

@ray.remote
def split_in_files(path):
	global total_files
	global target_path
	df = pd.read_csv(path)
	df = df.iloc[1:]
	df=df.reset_index(drop=True)
	df.columns = ['Vehicle_No','UnitID','DateTimeReceived','DataStampDate','Latitude','Longitude','Speed']
	uniquelist = list(df['UnitID'].unique())
	print('Number of Taxis '+str(len(uniquelist)))
	total_files+=len(df.index)

	prev = df['UnitID'][0]
	j=0
	for i in range(len(uniquelist)):
		print(str(prev)+"Generating File for Taxi ..... "+str(i+1))
		start = j
		end = j
		while True:
			if(j==len(df.index)):
				reg_no = df['Vehicle_No'][start].replace(" ","")
				if(reg_no[:2]!='DL' and reg_no[:2]!='HR'):
					reg_no = 'NULL'
				df.iloc[start:end].drop(['UnitID'], axis = 1).drop(['Vehicle_No'], axis=1).to_csv(target_path+str(prev)+' '+reg_no+'.csv', index=False)
				break
			elif(j<len(df.index) and prev==df['UnitID'][j]):
				j+=1
				end+=1
			elif(j<len(df.index) and prev!=df['UnitID'][j]):
				reg_no = df['Vehicle_No'][start].replace(" ","")
				if(reg_no[:2]!='DL' and reg_no[:2]!='HR'):
					reg_no = 'NULL'
				df.iloc[start:end].drop(['UnitID'], axis = 1).drop(['Vehicle_No'], axis=1).to_csv(target_path+str(prev)+' '+reg_no+'.csv', index=False)
				prev = df['UnitID'][j]
				break
	# os.remove(path)


def main():
	global target_path
	start = time.time()
	# print('Folder should only contain CSV Files')
	# path = input('Enter path where CSV files are stored: ')
	# target_path = input('Enter the new Directory path where splitted files will be stored: ')
	path = '/home/s/all'
	# target_path = '/home/s/all0'
	target_path+='/'
	paths1 = os.listdir(path)
	num_cpus = psutil.cpu_count(logical=False)
	print('Your system has '+str(2*num_cpus)+' CPUs')
	ray.init(num_cpus=num_cpus*2)
	print(paths1)
	for paths2 in paths1:
		paths = os.listdir(path+'/'+paths2)
		print(paths)
		target_path = (path+'/'+paths2).replace('all', 'all0')+'/'
		ray.get([split_in_files.remote(path+'/'+paths2+'/'+p) for p in paths])
	print('Complete')
	end = time.time()
	print(" Time elapsed: "+str(end-start))
	print(total_files)

if __name__ == '__main__':
	main()