import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, when}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}


object CustomersOrders {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("B task I").setMaster("local[1]") //initialize spark
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val customersFilePath = args(0) + "/customers.csv" //path variables
    val ordersFilePath = args(0) + "/orders.csv"
    val outputPath = args(1) + "/task_I"

    val customersFields = StructType(List(StructField("custkey", StringType, true), //columns names and types
      StructField("name", StringType, true),
      StructField("address",StringType,true),
      StructField("nationkey", StringType, true),
      StructField("Phone", StringType, true),
      StructField("actbal",DoubleType,true),
      StructField("mktsegment",StringType,true),
      StructField("comment",StringType,true)))

    val ordersFields = StructType(List(StructField("orderkey",StringType,true),
      StructField("custkey",StringType,true),
      StructField("orderstatues",StringType,true),
      StructField("price",DoubleType,true),
      StructField("orderdate",DateType,true),
      StructField("orderpriority",StringType,true),
      StructField("clerk",StringType,true),
      StructField("shippriority",StringType,true),
      StructField("comment",StringType,true)))

    //load dataset
    val customersDF = spark.read.option("inferSchema", "true").option("header", "false").option("delimiter", "|").schema(customersFields).csv(customersFilePath)
    val ordersDF = spark.read.option("inferSchema", "true").option("header", "false").option("delimiter", "|").schema(ordersFields).csv(ordersFilePath)

    customersDF.createOrReplaceTempView("customers") //transform them to use with spark.sql
    ordersDF.createOrReplaceTempView("orders")

    val customersOrders = spark.sql(
      """SELECT cust.name AS name, cust.address AS address, AVG(ord.price) AS priceAverage
        |FROM customers AS cust
        |JOIN orders as ord
        |ON cust.custkey == ord.custkey
        |WHERE cust.actbal > 2000 AND ord.orderdate > '1996-01-01'
        |GROUP BY name, address
        |ORDER BY name
        |""".stripMargin) //command for the task

    //save file
    customersOrders.coalesce(1).write.format("csv").save(outputPath)


  }
}
