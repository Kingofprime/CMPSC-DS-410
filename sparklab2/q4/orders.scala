//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q4 {

    def main(args: Array[String]) = {
        val sc = getSC()
        val (customers,orders, items) = getRDD(sc)
        val cityReduce = doOrders(customers,orders, items)
        saveit(cityReduce, "lab2q4")
    }

    def getSC() = {
        val conf = new SparkConf()
            .setAppName("word count")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc:SparkContext) = {
        val customers = sc.textFile("/datasets/orders/customers.csv")
        val orders = sc.textFile("/datasets/orders/orders*")
        val items = sc.textFile("/datasets/orders/items.csv")
        (customers,orders, items)
    }

    def doOrders(customers: RDD[String], orders: RDD[String], items : RDD[String]): RDD[(String, Double)] = {
        val customerPairs = customers.filter(_.nonEmpty).filter(line => !line.startsWith("CustomerID\tCountry")).flatMap(line => {
            val cparts = line.split("\t")
            if (cparts.length == 2){
            Some((cparts(0), cparts(1)))}
            else{
                None}
        })

        val orderPairs = orders.filter(_.nonEmpty).filter(line => !line.startsWith("InvoiceNo\tStockCode")).flatMap(line => {
            val oparts = line.split("\t",5)
            if(oparts.length == 5){ 
            Some((oparts(1), (oparts(4), oparts(2).toDouble)))}
            else{
                None}
        })
        val itemPairs = items.filter(_.nonEmpty).filter(line => !line.startsWith("StockCode\tDescription")).flatMap(line => {
            val iparts = line.split("\t")
            if (iparts.length == 3){
            Some((iparts(0), iparts(2).toDouble))}
            else{
                None}
        })
        
        val totalPricePerItem = orderPairs.join(itemPairs) //This is the wide dependency transformation
            .map { case (stockCode, ((customerId, quantity), unitPrice)) => (customerId, quantity * unitPrice) }
        val totalSpendingByCountry = customerPairs.join(totalPricePerItem) //This is the wide dependency transformation
            .map { case (customerId, (country, totalPrice)) => (country, totalPrice) }
            .reduceByKey(_ + _) //This is the wide dependency transformation

        totalSpendingByCountry
    }

    def getTestRDD(sc: SparkContext): (RDD[String], RDD[String], RDD[String]) = {
        val cust = List(
            "CustomerID\tCountry",
            "11111\tChina",
            "22222\tIndia",
            "33333\tUSA",
            "44444\tFramce",
            "55555\tUK",
            "66666\tSpain")
        
        val order = List ("InvoiceNo\tStockCode\tQuantity\tInvoiceDate\tCustomerID",
            "123456\t12345\t1\t12/1/2016 8:26\t55555",
            "123456\t23456\t1\t12/1/2016 8:26\t55555",
            "123456\t67890\t1\t12/1/2016 8:26\t55555",
            "234567\t89012\t1\t12/1/2016 8:26\t33333",
            "234567\t78901\t1\t12/1/2016 8:26\t33333",
            "345678\t12345\t1\t12/1/2016 8:26\t66666",
            "456789\t12345\t1\t12/1/2016 8:26\t22222")

        val items = List("StockCode\tDescription\tUnitPrice",
                         "12345\tdsff\t300.52",
                         "23456\tdhs\t280.19",
                         "34567\tyinhe\t45.31",
                         "45678\tstiga\t40.01",
                         "56789\tcat\t12.01")
        
        val rddcust = sc.parallelize(cust)
        val rddorders = sc.parallelize(order)
        val rdditems = sc.parallelize(items)
        
        (rddcust,rddorders,rdditems)
    }

    def runTest(sc: SparkContext) = {
        val (testCustomers,testOrders,testItems) = getTestRDD(sc)
        val resultRDD = doOrders(testCustomers,testOrders,testItems)
        resultRDD.collect().foreach(println)
    }

    def saveit(cityReduce: RDD[(String, Double)], name: String) = {
        cityReduce.saveAsTextFile(name)
    }
}
