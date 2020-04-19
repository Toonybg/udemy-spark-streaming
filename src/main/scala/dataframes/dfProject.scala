package dataframes
import dataframes.dfOperations.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dfProject extends App{
  // DATAFRAME PROJECT
  //set the logger to WARN
  Logger.getLogger("org").setLevel(Level.WARN)

  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

  // Loading the Netflix CSV File, and Spark infer the data types.
  val df = spark.read.option("header","true").option("inferSchema","true").csv("resources/datasets/Netflix_2011_2016.csv")

  // column names:
  df.columns

  // Schema preview:
  df.printSchema()

  // Print out the first 5 columns.
  df.head(5)

  // Use describe() to learn about the DataFrame.
  df.describe().show()

  // Create a new dataframe with a column called HV Ratio that
  // is the ratio of the High Price versus volume of stock traded
  // for a day.
  val df2 = df.withColumn("HV Ratio",df("High")/df("Volume"))

  // For Scala/Spark $ Syntax
  import spark.implicits._

  // The Peak High in Price is Monday
  df.orderBy($"High".desc).show(1)

  // What is the mean of the Close column?
  df.select(mean("Close")).show()

  // What is the max and min of the Volume column?
  df.select(max("Volume")).show()
  df.select(min("Volume")).show()

  // How many days was the Close lower than $ 600?
  df.filter($"Close"<600).count()

  // What percentage of the time was the High greater than $500 ?
  // convert count to percent
  (df.filter($"High">500).count()*1.0/df.count())*100

  // What is the Pearson correlation between High and Volume?
  df.select(corr("High","Volume")).show()

  // What is the max High per year?
  val yeardf = df.withColumn("Year",year(df("Date")))
  val yearmaxs = yeardf.select($"Year",$"High").groupBy("Year").max()
  yearmaxs.select($"Year",$"max(High)").show()

  // What is the average Close for each Calender Month?
  val monthdf = df.withColumn("Month",month(df("Date")))
  val monthavgs = monthdf.select($"Month",$"Close").groupBy("Month").mean()
  monthavgs.select($"Month",$"avg(Close)").show()

}
