import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, when}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}


object CustomersOrders {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("B task I").setMaster("local[1]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    if(args(1) == "taskI") {
      val customersFilePath = args(0) + "/customers.csv"
      val ordersFilePath = args(0) + "/orders.csv"

      val customersFields = StructType(List(StructField("custkey", StringType, true),
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

      val customersDF = spark.read.option("inferSchema", "true").option("header", "false").option("delimiter", "|").schema(customersFields).csv(customersFilePath)
      val ordersDF = spark.read.option("inferSchema", "true").option("header", "false").option("delimiter", "|").schema(ordersFields).csv(ordersFilePath)

      customersDF.createOrReplaceTempView("customers")
      ordersDF.createOrReplaceTempView("orders")

      val customersOrders = spark.sql(
        """SELECT cust.name AS name, cust.address AS address, AVG(ord.price) AS priceAverage
          |FROM customers AS cust
          |JOIN orders as ord
          |ON cust.custkey == ord.custkey
          |WHERE cust.actbal > 2000 AND ord.orderdate > '1996-01-01'
          |GROUP BY name, address
          |ORDER BY name
          |""".stripMargin)
      """customersOrders.coalesce(1).write.format("csv").save("file:///home/hdoop/Bureau/AIM-3/HW1_CANEVET/B/output/task_i")"""

    } else {
      val initial_vertice = "17038"
      """
      val mazeDF = spark.read.option("inferSchema", "true").option("header", "false").option("delimiter", "\t").csv("input/ca-GrQc.txt").toDF("key","value")
      val mazeGrouped = mazeDF.groupBy("key").agg(concat_ws("|", collect_list("value")) as "values")
      val AdjencyListDF = mazeGrouped.withColumn("State",lit("UNDISCOVERED")).withColumn("weight", when(col("key") === initial_vertice,lit(0)).otherwise(lit("INFINITE"))).sort(col("weight"))
      val adjencyListRDD = AdjencyListDF.select(col("key"),col("weight"),col("state"),col("values")).rdd

      adjencyListRDD.map(line =>{
        val source = line(0)
        val weight = line(1)
        val state = line(2)
        val neighbour = line(3)
        if (state == "UNDISCOVERED" && weight != "INFINITE"){
          if(line.length<5){
            (source,weight,"DISCOVERED",neighbour,source)
          } else {
            (source,weight,"DISCOVERED",neighbour,line(5))
          }
          val neighbourList = neighbour
        }else{
          line
        }
      })"""
      """val undiscovered = (AdjencyList.select(col("state")).groupBy("state").count().head())(1)"""

    }
  }
}
