from mrjob.job import MRJob

class WordCount(MRJob):
      def mapper (self, key, line):
          words = line.split()
          for w in words:
              if 'e' in w:
                 yield w,1


      def reducer(self,key,values):
          t = sum(values)
          if t >=2:
              yield key, t

if __name__ == '__main__':
    WordCount.run()
