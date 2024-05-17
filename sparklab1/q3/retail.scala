//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q3 {
    def main(args : Array[String]) = {
        val sc = getSC()
        val myrdd = getRDD(sc)
        val counts = doRetail(myrdd)
        saveit(counts, "spark3output") 
    }
    def getSC() = {
        val conf = new SparkConf().setAppName("word count")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc: SparkContext) = {
        sc.textFile("/datasets/retailtab")
    }

    def doRetail(input: RDD[String]): RDD[(String, Double)] = {
        val head = "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry"
        input.filter(_!= head).map { line =>
          val word = line.split("\t")
          val invno = word(0)
          val quant = word(3).toInt
          val unitp = word(5).toDouble
          (invno, quant*unitp)
        }.reduceByKey(_+_)
          
    }
    def getTestRDD(sc: SparkContext): RDD[String] = {
        val line = List ("InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry","536365\t85123A\tWHITE HANGING HEART T-LIGHT HOLDER\t6\t12/1/2010 8:26\t2.55\t17850\tUnited Kingdom","536365\t71053\tWHITE METAL LANTERN\t6\t12/1/2010 8:26\t3.39\t17850\tUnited Kingdom")
        sc.parallelize(line)
    }
    def runTest(sc: SparkContext) = {
        val testsc = getSC()
        val testrdd = getTestRDD(sc)
        val test = doRetail(testrdd)
        test.collect().foreach(println)
    }
    def saveit(counts: RDD[(String, Double)], name: String) = {
        counts.saveAsTextFile(name)
    }

}
