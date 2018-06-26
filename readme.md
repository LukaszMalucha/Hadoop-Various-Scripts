## Big Data Scripts

### Hortonworks Sandbox

http://127.0.0.1:8888/

##### Dev:
User:       maria_dev 
Password:   maria_dev

##### Admin:
su root 
ambari-admin-password-reset


#### Installation requirements:

Anaconda Command Prompt:
ssh maria_dev@127.0.0.1 -p 2222

su root

yum install python-pip

pip install mrjob==0.5.11

yum install nano

### MAPREDUCE

##### Dataset # 1 - MovieLens 100K Dataset
https://grouplens.org/datasets/movielens/

##### Scripts:
mapreduce/*

##### Run locally:
python RatingsBreakdown.py u.dataset

##### Run with Hadoop 
python MostPopularMovie.py -r hadoop --hadoop-streaming-jar/usr/hdp/current/hadoop-mapreduce-client/hadopp-streaming.jar u.data


### PIG


##### Dataset # 1 - MovieLens 100K Dataset
https://grouplens.org/datasets/movielens/


##### Scripts:
pig/*

### SPARK