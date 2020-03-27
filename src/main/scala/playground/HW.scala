package playground

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HelloWorld extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder.appName("spark").config("spark.master","local").getOrCreate()
  val df = spark.read.option("header","true").option("inferSchema","true").csv("resources/Spark DataFrames/CitiGroup2006_2008")

  for (col<-df.columns){
    print(s"$col ")
  }
  println()


  df.printSchema()
  df.describe().show()


  println("HW")
}
