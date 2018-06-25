## Map-Reducer function example for ratings count aggregated by id, sorted by rating count

from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdown(MRJob):
    def steps(self):
        return [                                                                ## mr steps
            MRStep(mapper=self.mapper_get_ratings,
            reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sorted_output)
            ]


    def mapper_get_ratings(self, _,line):                                       ## three params - self, possible key reducer, each input line of data
        (userID, movieID, rating, timestamp) = line.split('\t')                 ## split with tab character
        yield movieID, 1                                                        ## key-value pair (rating and 1 for a count)
        
    def reducer_count_ratings(self, key, values):
        yield str (sum(values)).zfill(5), key                                   ## zero-fill up to 5 digits
    
    def reducer_sorted_output(self, count, movies):
        for movie in movies:
            yield movie, count


if __name__ == '__main__':
    RatingsBreakdown.run()