from mrjob.job import MRJob 

class City(MRJob): 
    def mapper(self, _, line): # Mapper function to process each line of the input data.
        # Skip lines with headers or source information.
        if 'Source:' in line:
          return
        if 'Population' in line:
          return 
        parts = line.split("\t")
        # Check if the line has enough parts
        if len(parts) >= 7:
            state = parts[2]
            city = parts[0]
            population = int(parts[4])
            zipcodes = parts[5].split(" ")
            yield state, (1, population, len(zipcodes))
 #Combiner function to combine the output from mapper
    #def combiner(self, state, values):
    #    num_cities = 0
     #   total_population = 0
      #  maxzips = 0
      #  for n,p,z in values:
       #     num_cities = n + num_cities
        #    total_population = p + total_population
         #   maxzips = max(maxzips, z)
        #yield state, (num_cities, total_population, maxzips)
# Reducer function to aggregate data by state.
    def reducer(self, state, values):
        num_cities = 0
        total_population = 0
        maxzips = 0
        for n,p,z in values:
            num_cities = n + num_cities
            total_population = p + total_population
            maxzips = max(maxzips, z)
        yield state, (num_cities, total_population, maxzips)
# if you don't have these two lines, your code will not do anything
if __name__ == '__main__':
    City.run()
