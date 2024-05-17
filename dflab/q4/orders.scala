import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q4 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val (c, o, i) = getDF(spark)
        val counts = doOrders(c,o,i)
        saveit(counts, "dflabq4")  // save the rdd to your home directory in HDFS
    }
    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }
    
    def getDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        val customerSchema = new StructType()
        .add("CustomerID", StringType, true)
        .add("Country", StringType, true)
        
        val ordersSchema = new StructType()
        .add("InvoiceNo", StringType, true)
        .add("StockCode", StringType, true)
        .add("Quantity", IntegerType, true)
        .add("InvoiceDate", StringType, true)  
        .add("CustomerID", StringType, true)
        
        val itemsSchema = new StructType()
        .add("StockCode", StringType, true)
        .add("Description", StringType, true)
        .add("UnitPrice", DoubleType, true)

        val customers = spark.read
        .format("csv")
        .schema(customerSchema)
        .option("header", "true")
        .option("delimiter", "\t")
        .option("mode", "PERMISSIVE")
        .load("/datasets/orders/customers.csv")
        .na.fill("blank", Seq("CustomerID"))

        val orders = spark.read
        .format("csv")
        .schema(ordersSchema)
        .option("header", "true")
        .option("delimiter", "\t")
        .option("mode", "PERMISSIVE")
        .load("/datasets/orders/orders*")
        .na.fill("blank", Seq("CustomerID"))

        val items = spark.read
        .format("csv")
        .schema(itemsSchema)
        .option("header", "true")
        .option("delimiter", "\t")
        .option("mode", "PERMISSIVE")
        .load("/datasets/orders/items.csv")

      (customers, orders, items)
    }
    
    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {
        val joinedDF = orders
        .join(customers, "CustomerID")  
        .join(items, "StockCode")      

        val withTotalSpent = joinedDF.withColumn("TotalSpent", $"Quantity" * $"UnitPrice")

        val resultDF = withTotalSpent.groupBy("Country")
        .agg(sum("TotalSpent").alias("TotalSpent"))
        .orderBy("Country") 
        resultDF
    }
    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        import spark.implicits._
        val customers = Seq(
            ("12346", "United Kingdom"),
            ("12347", "Iceland"),
            ("", "")
        ).toDF("CustomerID", "Country")

        val orders = Seq(
            ("536365", "85123A", 6, "12346"),
            ("536365", "71053", 6, "12347")
        ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")

        val items = Seq(
            ("85123A", "Description1", 2.55),
            ("71053", "Description2", 3.39)
        ).toDF("StockCode", "Description", "UnitPrice")

        (customers, orders, items)
    }
    def runTest(spark: SparkSession) = {
        val (customers, orders, items) = getTestDF(spark)
        val result = doOrders(customers, orders, items)
        result.collect().foreach(println)
    }
    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)

    }


}

