package playground

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession,Row}

object HW extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder.appName("spark").config("spark.master","local").getOrCreate()
  val df = spark.read.option("header","true").option("inferSchema","true").csv("resources/datasets/CitiGroup2006_2008")

//  var df2: Dataset[Row] = spark.emptyDataFrame
  var df2: Dataset[Row] = df
  for(c <- df.columns){
    println(c)
    df2 = df2.withColumn(c.toString + "bis",df(c))
  }

  df2.printSchema()

  for (col<-df.columns){
    print(s"$col ")
  }
  println()

  df.printSchema()
  df.describe().show()


  println("HW")
}
