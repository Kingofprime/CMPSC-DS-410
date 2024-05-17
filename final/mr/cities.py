from mrjob.job import MRJob
from mrjob.step import MRStep

class City(MRJob):
    def steps(self):
        return [
            #MRStep(mapper=self.mapper, reducer=self.reducer)
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        # Skip lines with headers or source information.
        if 'Source:' in line or 'Population' in line:
            return
        parts = line.split("\t")
        if len(parts) >= 7:
            state = parts[2]
            city = parts[0]
            zipcodes =len(parts[5].split(" "))
            yield  zipcodes , 1
        else:
            yield 0 , 1

    def combiner(self, state, values):
        # Initialize counters for number of cities, total population, and max zip codes.
        num_cities = 0

        # Aggregate values from mapper.
        for n in values:
            num_cities += n
        
        yield state , num_cities

    def reducer(self, state, values):
        # Initialize counters for number of cities, total population, and max zip codes.
        num_cities = 0

        # Aggregate values from combiner.
        for n in values:
            num_cities += n
        
        yield state , num_cities

if __name__ == '__main__':
    City.run()

