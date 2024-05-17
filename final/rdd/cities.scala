//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()
        val myrdd = getRDD(sc)
        val cityReduce = doCity(myrdd)
        saveit(cityReduce, "finalrdd")
    }

    def getSC() = {
        val conf = new SparkConf()
            .setAppName("word count")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc:SparkContext) = {
        sc.textFile("/datasets/cities/cities.csv")
    }

    def doCity(input: RDD[String]): RDD[(Int, Int)] = {
        val cleanedRDD = input.filter(_.nonEmpty).filter(line => !line.startsWith("Source:")).filter(line => !line.startsWith("City\tState"))
        val cityData = cleanedRDD.flatMap(line => {
            val cols = line.split("\t")
            if (cols.length >= 4) {
               val state = cols(1)
               val zipCodes = cols(5).split(" ").length
               Some( zipCodes , 1 )}
            else {
               Some( 0,2 )
            }
        })

        val cityReduce = cityData.reduceByKey(_+_) //This is the wide dependency transformation
        //cityReduce.map{ case ( state, ( city, zipcodes)) => (zipcodes , city) }
        cityReduce   
    }

    def getTestRDD(sc: SparkContext): RDD[String] = {
        val testList = List(
            "Source: https://simplemaps.com/data/us-cities",
            "City\tState Abbreviation\tState\tCounty\tPopulation\tZip Codes (space separated)\tID",
            "Kohatk\tAZ\tArizona\tPinal\t0\t85634\t1840022983",
            "Ironville\tPA\tPennsylvania\tBlair\t1\t16686\t1840152922",
            "Newkirk\tNM\tNew Mexico\tGuadalupe\t0\t88417\t1840024978",
            "Falcon Village\tTX\tTexas\tStarr\t0\t78545\t1840018314",
            "Millerstown\tPA\tPennsylvania\tBlair\t2\t16662 16882\t1840153020")
        sc.parallelize(testList)
    }

    def runTest(sc: SparkContext) = {
        val testRDD = getTestRDD(sc)
        val resultRDD = doCity(testRDD)
        resultRDD.collect().foreach(println)
    }

    def saveit(cityReduce: RDD[(Int,Int)], name: String) = {
        cityReduce.saveAsTextFile(name)
    }
        


}

