from mrjob.job import MRJob

class airport(MRJob):
    
    def mapper(self, _, line):
        # Skip the header or any line that does not contain relevant data
        if 'PASSENGERS' in line:
            return
        
        fields = line.split(',')
        origin = fields[3]
        passen = float(fields[7])
        yield origin , passen

    def combiner(self, origin, values):
        t_lol , t_main = 0.0,0.0
        for v in values:
            if v == 1:
                t_lol = 1 + t_lol
            t_main = t_main + 1
        yield origin, (t_lol,t_main)

    def reducer(self, airport, values):
        solo, tot = 0.0,0.0
        for v,y in values: 
            solo = solo + v
            tot = tot + y
        final = solo/tot 
        yield airport, final

if __name__ == '__main__':
    airport.run()
