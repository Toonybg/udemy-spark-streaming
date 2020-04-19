package dataframes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object dfOverview extends App{
  // Getting Started with DataFrames!
  Logger.getLogger("org").setLevel(Level.WARN)

  // Most Important Link:
  // http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
  // Start a simple Spark Session
  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
  // Create a DataFrame from Spark Session read csv
  // Technically known as class Dataset
  val df = spark.read.option("header","true").option("inferSchema","true").csv("resources/datasets/CitiGroup2006_2008")
  // Get first 5 rows
  df.head(5) // returns an Array
  println("\n")
  for(line <- df.head(10)){
    println(line)
  }

  df.columns //returns an Array

  // Find out DataTypes
  // Print Schema
  df.printSchema()

  // Describe DataFrame Numerical Columns
  df.describe()

  // Select columns .transform().action()
  df.select("Volume").show()

  import spark.implicits._
  // Multiple Columns
  df.select($"Date",$"Close").show(2)

  // Creating New Columns
  val df2 = df.withColumn("HighPlusLow",df("High")-df("Low"))
  // Show result
  df2.columns
  df2.printSchema()

  // Recheck Head
  df2.head(5)

  // Renaming Columns .as()
  df2.select(df2("HighPlusLow").as("HPL"),df2("Close")).show()

}
