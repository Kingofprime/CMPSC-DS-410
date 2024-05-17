//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {

    def main(args: Array[String]) = {
        val sc = getSC()
        val myrdd = getRDD(sc)
        val cityReduce = doOrders(myrdd)
        saveit(cityReduce, "lab2q2")
    }

    def getSC() = {
        val conf = new SparkConf()
            .setAppName("word count")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc:SparkContext) = {
        sc.textFile("/datasets/orders/customers.csv")
    }

    def doOrders(input: RDD[String]): RDD[(String, Int)] = {
       val cleanedRDD = input.filter(_.nonEmpty).filter(line => !line.startsWith("CustomerID\tCountry"))
       val orderData = cleanedRDD.flatMap(line => {
           val words = line split("\t")
           if (words.length == 2){
              val country = words(1)
              Some((country,1))
           }
           else{
               None
           }
       })
       val CityReduce = orderData.reduceByKey(_ + _) //This is the wide dependency transformation
       CityReduce
    }

    def getTestRDD(sc: SparkContext): RDD[String] = {
        val testList = List(
            "CustomerID\tCountry",
            "11111\tChina",
            "22222\tIndia",
            "33333\tUSA",
            "44444\tFramce",
            "55555\tUK",
            "66666\tSpain")
        sc.parallelize(testList)
    }

    def runTest(sc: SparkContext) = {
        val testRDD = getTestRDD(sc)
        val resultRDD = doOrders(testRDD)
        resultRDD.collect().foreach(println)
    }

    def saveit(cityReduce: RDD[(String, Int)], name: String) = {
        cityReduce.saveAsTextFile(name)
    }
}
