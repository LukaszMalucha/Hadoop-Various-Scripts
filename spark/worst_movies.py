from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

## Create a dictionary in order to convert movie ID's to movie names

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
    
## Convert each line of data into  (movieID,(rating, 1.0)) in order to add 
## all the ratings for each movie and then divide by total numRating for avg

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # Spark Session
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    
    movieNames = loadMovieNames()
    
    # RDD
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    
    movies = lines.map(parseInput)
    
    movieDataset = spark.createDataFrame(movies)
    
    # Reduce to (movieID, (sumOfRatings, totalRatings))
    averageRatings = movieDataset.groupBy("movieID").avg("rating")
    
    counts = movieDataset.groupBy("movieID").count()
    
    # Join
    averageAndCounts = counts.join(averageRatings, "movieID")

    
    # Top 10
    results = averageAndCounts.orderBy("avg(rating)").take(10)
    
    for result in results:
        print(movieNames[result[0]], result[1], result[2])
        
    spark.stop()    
    