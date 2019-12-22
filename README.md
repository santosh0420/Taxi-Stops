# Taxi-Stops
Split.py - To split large csv files in smaller csv files( 1 file per taxi ) to reduce memory overhead while proocessing stops.
Staypoints.py - Finds stop locations of taxis and saves them in stops.csv. 
Clustering.py - Finds clusters.
Split.py ---------> Staypoints.py -------->Clustering.py --------->Clusters.csv

Step 1: Install all pre-requisites
pip3 install ray
pip3 install matplotlib
pip3 install psutil
pip3 install plotly
pip3 install pandas

Step 2: Split large CSV files using split.py 
	i) Enter folder location where large file is stored.
	ii) This folder should contain only those files which you want to split.
	iii) Enter new folder location where you want to store individual files for taxis.

Step 3: Run staypoints.py on these splitted files to generate a csv files named stops.csv that contains stop points for all these taxis.

Step 4: Run clustering.py on stops.csv file to find cluster. You can also update the parameter minimum number of autos.
This will generate two files named clusters.csv and centers.csv(centers of all clusters found).
