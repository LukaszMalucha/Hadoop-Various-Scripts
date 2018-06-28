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


### HIVE

#### cmd
LOAD DATA LOCAL INPATH '${env:HOME}/ml-100k/u.data'
OVERWRITE INTO TABLE ratings;


### SQOOP

##### HDFS data import with explicit mysql driver
sqoop import --connect jdbc:mysql://localhost/movielens --driver com.mysql.jdbc.Driver --table movies

##### Direct HIVE import
sqoop import --connect jdbc:mysql://localhost/movielens --driver com.mysql.jdbc.Driver --table movies --hive-import

##### Data export (target table must already exist in MySQL)

sqoop export --connect jdbc:mysql://localhost/movielens -m 1 --driver com.mysql.jdbc.Driver --table exported_movies --export-dir
/apps/hive/warehouse/movies --input-fields-terminated-by '\0001'


##### login

mysql -u root -p 
default password: hadoop


### SPARK

##### Scripts:
spark/*

##### Movie Recommendation with ALS:
export SPARK_MAJOR_VERSION=2
movie_recommend.py


### MONGODB WITH SPARK

##### Scripts:
mongoDB_spark/*

##### Install MongoDB
su root
cd /var/lib/ambari-server/resources/stacks
cd HDP
cd 2.5 
cd services
git clone https://github.com/nikunjness/mongo-ambari.git
sudo services ambari restart

Ambari -> Actions -> Add Service --> MongoDB

pip intall pymongo


### FLUME WITH SPARK

##### Scripts:
flume/*

##### Flume Config:
mkdir checkpoint
export SPARK_MAJOR_VERSION=2
spark-submit --packages org.apache.spark:spark-streaming-flume_2.11:2.0.0 SparkFlume.py
cd /usr/hdp/current/flume-server/
bin/flume-ng agent --conf conf --conf-file ~/sparkstreamingflume.conf --name a1

