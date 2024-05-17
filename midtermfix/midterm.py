from mrjob.job import MRJob

class AirportSoloBookings(MRJob):
    
    def mapper(self, _, line):
        # Skip the header or any line that does not contain relevant data
        if 'PASSENGERS' in line or not line.strip():
            return
        
        fields = line.split(',')
        origin = fields[3]  # Assuming the 4th column is 'origin'
        passengers = float(fields[7])  # Assuming the 8th column is 'passengers'
        is_solo = 1 if passengers == 1 else 0
        # Emit origin airport code with a tuple of (is_solo, 1) to count solo and total bookings
        yield origin, (is_solo, 1)

    def combiner(self, origin, values):
        solo, total = 0, 0
        for is_solo, count in values:
            solo += is_solo
            total += count
        # Emit accumulated counts for this origin from this mapper
        yield origin, (solo, total)

    def reducer(self, origin, values):
        solo, total = 0, 0
        for is_solo, count in values:
            solo += is_solo
            total += count
        # Calculate the fraction of solo bookings
        fraction_solo = solo / total if total else 0
        yield origin, fraction_solo

if __name__ == '__main__':
    AirportSoloBookings.run()
