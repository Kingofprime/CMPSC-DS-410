//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {  
    def main(args: Array[String]) = {  
        val sc = getSC()
        val myrdd = getRDD(sc)
        val counts = doWordCount(myrdd) 
        saveit(counts, "spark2output")  
    }


    def getSC() = {
        val conf = new SparkConf().setAppName("word count")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc: SparkContext) = {
        sc.textFile("/datasets/wap")
    }
    def doWordCount(input: RDD[String]) = {
        val words = input.flatMap(_.split(" "))
        val ok = words.filter(_.contains("e"))
        val kv = ok.map(word => (word, 1))
        val counts = kv.reduceByKey((x, y) => x + y).filter(_._2 >=2)
        counts
    }

    def getTestRDD(sc: SparkContext) = {
        val mylines = List("it was the best of times, wasn't it","it was the worst of times of all time")
        sc.parallelize(mylines, 3)
    }

    def runTest() = {
        val testsc = getSC()
        val testrdd = getTestRDD(testsc)
        val sum = doWordCount(testrdd)
        sum.collect().foreach(println)
    }

    def saveit(counts: RDD[(String, Int)], name: String) = {
        counts.saveAsTextFile(name)
    }
}
