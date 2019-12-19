import pandas as pd 
import ray
import time
import pandas

@ray.remote
def split_in_files(path):
	df = pd.read_csv(path)
	uniquelist = list(df['UnitID'].unique())
	print('Number of Taxis '+str(len(uniquelist)))

	prev = df['UnitID'][0]
	j=0
	for i in range(len(uniquelist)):
		print(str(prev)+"Generating File for Taxi ..... "+str(i+1))
		start = j
		end = j
		while True:
			if(j==len(df.index)):
				df.iloc[start:end].drop(['UnitID'], axis = 1).drop(['Vehicle_No'], axis=1).to_csv('/home/s/Taxi/1Jan/'+str(prev)+'.csv', index=False)
				break
			elif(j<len(df.index) and prev==df['UnitID'][j]):
				j+=1
				end+=1
			elif(j<len(df.index) and prev!=df['UnitID'][j]):
				df.iloc[start:end].drop(['UnitID'], axis = 1).drop(['Vehicle_No'], axis=1).to_csv('/home/s/Taxi/1Jan/'+str(prev)+'.csv', index=False)
				prev = df['UnitID'][j]
				break


def main():
	start = time.time()
	path = '/home/s/Taxi'
	paths = []
	for i in range(3,5):
		paths.append(path+"/"+str(i+1)+'.csv')
	ray.init(num_cpus=24)
	ray.get([split_in_files.remote(p) for p in paths])
	print('Complete')
	end = time.time()
	print(" Time elapsed: "+str(end-start))

if __name__ == '__main__':
	main()