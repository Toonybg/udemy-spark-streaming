println("Hello World!")

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset,Row}

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

var df2: Dataset[Row] = Any.Ref
for(c <- df.columns){
  println(c)
  df2=df2.withColumn(c.toString + "bis",df(c))
}

df2.describe().show

