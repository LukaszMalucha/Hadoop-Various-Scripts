## Map-Reducer function example for movie ratings count

from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdown(MRJob):
    def steps(self):
        return [                                                                ## mr steps
            MRStep(mapper=self.mapper_get_ratings,
            reducer=self.reducer_count_ratings)
            ]


    def mapper_get_ratings(self, _,line):                                       ## three params - self, possible key reducer, each input line of data
        (userID, movieID, rating, timestamp) = line.split('\t')                 ## split with tab character
        yield rating, 1                                                         ## key-value pair (rating and 1 for a count)
        
    def reducer_count_ratings(self, key, values):
        yield key, sum(values)                                                  ## summing-up '1' values from mapping function


if __name__ == '__main__':
    RatingsBreakdown.run()