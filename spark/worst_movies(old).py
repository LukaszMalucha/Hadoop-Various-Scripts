from pyspark import SparkConf, SparkContext

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
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)
    
    movieNames = loadMovieNames()
    
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    
    # Convert to (movieID, (rating, 1.0)) with function parseInput
    movieRatings = lines.map(parseInput)
    
    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1] ))
    
    # Map to (movieID, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1] )
    
    # Sort by avg rting
    sortedMovies = averageRatings.sortBy(lambda x: x[1])
    
    # Top 10
    results = sortedMovies.take(10)
    
    for result in results:
        print(movieNames[result[0], result[1]])
    