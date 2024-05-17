//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q3 {

    def main(args: Array[String]) = {
        val sc = getSC()
        val (customers,orders) = getRDD(sc)
        val cityReduce = doOrders(customers,orders)
        saveit(cityReduce, "lab2q3")
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
        (customers,orders)
    }

    def doOrders(customers: RDD[String], orders:RDD[String]): RDD[(String, Int)] = {
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
            Some((oparts(4), oparts(2).toInt))}
            else{
                None}
        })

        val joined = customerPairs.join(orderPairs) //This is the wide dependency transformation
        
        val countryQuantities = joined.map {case (customerId, (country, quantity)) => (country, quantity)}
            .reduceByKey(_ + _) //This is the wide dependency transformation

        countryQuantities
    }

    def getTestRDD(sc: SparkContext): (RDD[String], RDD[String]) = {
        val cust = List(
            "CustomerID\tCountry",
            "11111\tChina",
            "22222\tIndia",
            "33333\tUSA",
            "44444\tFramce",
            "55555\tUK",
            "66666\tSpain")
        
        val order = List ("InvoiceNo\tStockCode\tQunatity\tInvoiceDate\tCustomerID",
            "123456\t12345\t1\t12/1/2016 8:26\t55555",
            "123456\t23456\t1\t12/1/2016 8:26\t55555",
            "123456\t67890\t1\t12/1/2016 8:26\t55555",
            "234567\t89012\t1\t12/1/2016 8:26\t33333",
            "234567\t78901\t1\t12/1/2016 8:26\t33333",
            "345678\t12345\t1\t12/1/2016 8:26\t66666",
            "456789\t12345\t1\t12/1/2016 8:26\t22222")
        
        val rddcust = sc.parallelize(cust)
        val rddorders = sc.parallelize(order)
        (rddcust,rddorders)
    }

    def runTest(sc: SparkContext) = {
        val (testCustomers,testOrders) = getTestRDD(sc)
        val resultRDD = doOrders(testCustomers,testOrders)
        resultRDD.collect().foreach(println)
    }

    def saveit(cityReduce: RDD[(String, Int)], name: String) = {
        cityReduce.saveAsTextFile(name)
    }
}
