import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q3 {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark) 
        val counts = doCity(mydf) 
        saveit(counts, "finaldf")
    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }


    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }
     def doCity(input: DataFrame): DataFrame = {
        val cleanedInput = input
        .filter(!($"City".contains("Source:")) && !($"State Abbreviation".rlike("https?")))
        .withColumn("ZipCount", callUDF("zipCounter", $"Zip Codes (space separated)"))
        // used rlike to remove bad output
        cleanedInput
        .groupBy("ZipCount")
        .agg(
            count("City").as("Number of Cities")
        )
    }
    def getDF(spark: SparkSession): DataFrame = {
         val mySchema = new StructType()
                        .add("City", StringType, true)
                        .add("State Abbreviation", StringType, true)
                        .add("State", StringType, true)
                        .add("County", StringType, true)
                        .add("Population", IntegerType, true)
                        .add("Zip Codes (space separated)", StringType, true)
                        .add("ID", LongType, true)

        spark.read.format("csv").schema(mySchema)
        .option("header", "true")
        .option("delimiter", "\t") 
        .option("mode", "PERMISSIVE")
        .load("/datasets/cities/cities.csv")
    }
     def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._
        Seq(
           ("ALwar", "CC", "rajas", "disco", 1540000, "11111 92211", "1"),
           ("Ajmer", "WE", "rajas69", "disco", 30000, "22222", "2"),
           ("Jaipur", "NV", "cas123", "BOT", 24000, "12345 12346 12347", "3")
        ).toDF("City", "State Abbreviation", "State", "County", "Population", "Zip Codes (space separated)", "ID")
    }

    def runTest(spark: SparkSession) = {
        val testDF = getTestDF(spark)
        val result = doCity(testDF)
        result.collect().foreach(println)
    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)

    }

}


