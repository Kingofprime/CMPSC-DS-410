from mrjob.job import MRJob
from mrjob.step import MRStep

class MyQuery(MRJob):
    def mapper_price(self, _, line):    # This function acts as a mapper that processes each line of the input data.
    # It filters out headers and yields key-value pairs based on the data's structure.
        if "CustomerID" in line:
           return
        if "StockCode" in line:
           return
        if "InvoiceNo" in line:
           return
        fields = line.split('\t')  # Split the line into fields based on the tab character.
         # Different formats of lines are handled based on their length.
        if len(fields) == 2:
           self.set_status("alive")
        elif len(fields) == 3:
             stockcode = fields[0]
             unitprice = float(fields[2])
             yield stockcode, unitprice
        elif len(fields) == 5:
             invoiceno = fields[0]
             stockcode = fields[1]
             quantity  = int(fields[2])
             customerid= fields[4]
             yield stockcode, (customerid, invoiceno, quantity)
          

    def reducer_price(self, key, valuelist):  # Reducer to aggregate data by stock code and calculate total amount per invoice.
        mylist = list(valuelist)
        unitprice = None
        stockcode = key
        for v in mylist: # Find the unit price for the stock code.
            if isinstance(v,float):
               unitprice = v
               break
        for v in mylist: # Calculate the total amount per invoice for each stock code.
            if isinstance(v,list):
               customerid, invoiceno, qty = v
               yield invoiceno , (customerid, stockcode, qty*unitprice)

    def mapper_cid(self, key, value):  # Mapper to re-key the data by customer ID and calculate the amount spent.
        (customerid, stockcode, amount) = value
        yield customerid,amount

    def reducer_cid(self,key,valuelist): # Reducer to sum up all amounts spent per customer ID.
        yield key,sum(valuelist)

    def steps(self): # Defines the steps of the MapReduce job, chaining two MapReduce steps.
        return [
            MRStep(mapper=self.mapper_price, reducer=self.reducer_price),
            MRStep(mapper=self.mapper_cid, reducer=self.reducer_cid)
        ]

if __name__ == '__main__':
    MyQuery.run()

