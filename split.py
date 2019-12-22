import pandas as pd 
import ray
import time
import pandas

total_files = 0
target_path = ''

@ray.remote
def split_in_files(path):
	global total_files
	global target_path
	df = pd.read_csv(path)
	df = df.iloc[1:]
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
				df.iloc[start:end].drop(['UnitID'], axis = 1).drop(['Vehicle_No'], axis=1).to_csv(target_path+str(prev)+'.csv', index=False)
				break
			elif(j<len(df.index) and prev==df['UnitID'][j]):
				j+=1
				end+=1
			elif(j<len(df.index) and prev!=df['UnitID'][j]):
				df.iloc[start:end].drop(['UnitID'], axis = 1).drop(['Vehicle_No'], axis=1).to_csv(target_path+str(prev)+'.csv', index=False)
				prev = df['UnitID'][j]
				break


def main():
	global target_path
	start = time.time()
	Print('Folder should only contain CSV Files')
	path = input('Enter path where CSV files are stored: ')
	target_path = input('Enter the new Directory path where splitted files will be stored: ')
	paths = os.listdir(path)
	num_cpus = psutil.cpu_count(logical=False)
	print('Your system has '+str(2*num_cpus))
	ray.init(num_cpus=num_cpus*2)
	ray.get([split_in_files.remote(path+'/'+p) for p in paths])
	print('Complete')
	end = time.time()
	print(" Time elapsed: "+str(end-start))
	print(total_files)

if __name__ == '__main__':
	main()