from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

## Create a dictionary in order to convert movie ID's to movie names

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames
    
## Convert each line of data into  (movieID,(rating, 1.0)) in order to add 
## all the ratings for each movie and then divide by total numRating for avg

def parseInput(line):
    fields = line.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # Spark Session
    spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()
    
    movieNames = loadMovieNames()
    
    # RDD
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd
    
    ratingsRDD = lines.map(parseInput)
    
    ratings = spark.createDataFrame(ratingsRDD).cache()
    
    als =ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)
    
    # Print out ratings for user 0
    print("\nRatings for user ID 0:")
    userRatings = ratings.filter("userID = 0")
    
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']
    
    
    print("\nTop 20 recommendations:")
    ## Movies with rtings count > 100
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    ## Test dataframe that compares user vs every movie with rating count > 100
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))
    
    
    # Run
    recommendations = model.transform(popularMovies)
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)
    
    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])
        
    spark.stop()    