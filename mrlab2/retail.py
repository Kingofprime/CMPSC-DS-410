from mrjob.job import MRJob

class retail(MRJob):
      def mapper(self,key,line):
         if "InvoiceNo" not in line:
              words = line.split('\t')  # Testfile uses "," to split up the data, found this in datasets/retail
              invoiceno = words[0]  
              quantity = int(words[3])  
              unitprice = float(words[5])  
              amount = quantity * unitprice
              yield invoiceno, (quantity, amount)

      def reducer(self,key,values):
          total_quant = 0 
          total_amount = 0
          for quantity, amount in values:
              total_quant = total_quant + quantity
              total_amount = total_amount + amount
          yield key,(total_quant,total_amount)






if __name__ =='__main__':
    retail.run()

