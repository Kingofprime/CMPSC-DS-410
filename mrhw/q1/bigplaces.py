from mrjob.job import MRJob
from mrjob.step import MRStep

class BigPlaces(MRJob):
    """
    This MRJob script identifies states with counties that have at least two cities
    with populations of 80,000 or more. It processes input lines containing city,
    state, county, and population data, and outputs each state along with the count
    of such 'big' counties.
    """

    def steps(self):
        """
        Defines the steps of the MapReduce job with two stages:
        1. Mapping city data to state and county, followed by a reduction to count cities
           with a population of 80,000 or more per county.
        2. Reducing the output of the first stage to count 'big' counties per state.
        """
        return [
            MRStep(mapper=self.mapper_cities, reducer=self.reducer_count),
            MRStep(reducer=self.reducer_count_big)
        ]

    def mapper_cities(self, _, line):
        """
        Mapper function that processes each line of input data. Skips headers and non-data lines.
        For valid data lines, it extracts city, state, county, and population. If the population
        is 80,000 or more, it emits a key-value pair with the state and county as the key and
        '1' as the value to indicate a 'big' city in that county.

        Args:
            _: Unused parameter, conventionally representing the key in map-reduce which is None here.
            line (str): A line of input data containing city information.

        Yields:
            tuple: ((state, county), 1) for each 'big' city identified.
        """
        # Skipping header or any non-data lines
        if 'Source:' in line:
            return
        if 'Population' in line:
            return 
        parts = line.split("\t")
        if len(parts) >= 5:
            city, state, county, population = parts[0], parts[1], parts[3], int(parts[4])
            if population >= 80000:
                # Emitting state and county as a combined key with '1' indicating a 'big' city
                yield (state, county), 1

    def reducer_count(self, state_county, city_counts):
        """
        Reducer function to count the number of 'big' cities in each county. If a county has at
        least two 'big' cities, it emits the state as a key and '1' as a value to indicate a 'big'
        county within that state.

        Args:
            state_county (tuple): The state and county as a combined key.
            city_counts (iterable): An iterable of counts (1s) for each 'big' city in the county.

        Yields:
            tuple: (state, 1) for each 'big' county identified.
        """
        total_big_cities = sum(city_counts)
        if total_big_cities >= 2:
            state, county = state_county
            # Emit state with '1' indicating a 'big' county
            yield state, 1

    def reducer_count_big(self, state, big_county_counts):
        """
        Reducer function to count the number of 'big' counties per state. It sums the values
        emitted by the previous reducer to determine the total count of 'big' counties in each state.

        Args:
            state (str): The state.
            big_county_counts (iterable): An iterable of counts (1s) for each 'big' county in the state.

        Yields:
            tuple: (state, total_big_counties) where total_big_counties is the count of 'big' counties
                   in the state.
        """
        # Summing the number of 'big' counties per state
        total_big_counties = sum(big_county_counts)
        yield state, total_big_counties

if __name__ == '__main__':
    BigPlaces.run()
