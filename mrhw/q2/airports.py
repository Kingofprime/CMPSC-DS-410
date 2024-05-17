from mrjob.job import MRJob
from mrjob.step import MRStep

class airport(MRJob):
    """
    This MRJob analyzes airport traffic data, calculating the total number of incoming
    and outgoing passengers for each airport. It outputs the airport code and the total
    numbers if the difference between incoming and outgoing passengers is 9 or more.
    """
    
    def mapper(self, _, line):
        """
        Maps each line of input data to key-value pairs where the key is the airport
        code and the value is a tuple indicating the direction ('in' or 'out') and
        the number of passengers.

        Args:
            _: Unused parameter, conventionally representing the key in map-reduce which is None here.
            line (str): A line of input data.

        Yields:
            tuple: (airport code, ('direction', passengers)) for each record.
        """
        # Skip the header or any line that does not contain relevant data
        if 'PASSENGERS' in line:
            return
        
        fields = line.split(',')
        if len(fields) == 8:
            origin, destination, passengers = fields[3], fields[5], float(fields[7])
            
            # Emit data for outgoing and incoming passengers
            yield origin, ('out', passengers)
            yield destination, ('in', passengers)

    def combiner(self, airport, traffic_data):
        """
        Combines counts for each airport locally before sending to the reducer to
        minimize data transfer. It sums up the incoming and outgoing passengers separately.

        Args:
            airport (str): The airport code.
            traffic_data (iterable): Iterable of tuples indicating direction and passenger counts.

        Yields:
            tuple: (airport, (total_out, total_in)) with aggregated counts for the airport.
        """
        total_out, total_in = 0.0, 0.0
        for direction, passengers in traffic_data:
            if direction == 'out':
                total_out = passengers+total_out
            else:  # 'in'
                total_in = passengers+total_in
        yield airport, (total_out, total_in)

    def reducer(self, airport, combined_counts):
        """
        Further aggregates the counts from the combiner and calculates the final totals
        for incoming and outgoing passengers. Outputs the airport code and the totals if
        the difference between incoming and outgoing passengers is 9 or more.

        Args:
            airport (str): The airport code.
            combined_counts (iterable): Iterable of combined counts for incoming and outgoing passengers.

        Yields:
            tuple: (airport, (total_out, total_in)) if the difference in counts is 9 or more.
        """
        total_out, total_in = 0.0, 0.0
        for out, in_n in combined_counts:
            total_out = out+total_out
            total_in = in_n+total_in
        # Output if the difference in passenger counts meets the threshold
        if abs(total_out - total_in) >= 9:
            yield airport, (total_out, total_in)

if __name__ == '__main__':
    airport.run()
