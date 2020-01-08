#!/bin/sh
### Set the job name (for your reference)
#PBS -N job1
### Set the project name, your department code by default
#PBS -P ee
### Request email when job begins and ends
####
#PBS -q low
#PBS -l select=4:ncpus=24
### Specify "wallclock time" required for this job, hhh:mm:ss
#PBS -l walltime=01:00:00

#PBS -l software=cluster
# After job starts, must goto working directory. 
# $PBS_O_WORKDIR is the directory from where the job is fired. 
echo "==============================="
echo $PBS_JOBID
cat $PBS_NODEFILE
echo "==============================="
cd $PBS_O_WORKDIR
#job 
python3 staypoints.py
#NOTE
# The job line is an example : users need to change it to suit their applications
# The PBS select statement picks n nodes each having m free processors
# OpenMPI needs more options such as $PBS_NODEFILE