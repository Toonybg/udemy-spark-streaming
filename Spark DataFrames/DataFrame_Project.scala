// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!

// Start a simple Spark Session

// Loading the Netflix CSV File, and Spark infer the data types.
val df = spark.read.option("header","true").option("inferSchema","true").csv("Netflix_2011_2016.csv")


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

// The Peak High in Price is Monday
df.orderBy($"High".desc).show(1)

// What is the mean of the Close column?
df.select(mean("Close")).show()

// What is the max and min of the Volume column?
df.select(max("Volume")).show()
df.select(min("Volume")).show()

// For Scala/Spark $ Syntax
import spark.implicits._

// How many days was the Close lower than $ 600?
df.filter($"Close"<600).count()

// What percentage of the time was the High greater than $500 ?
// convert count to percent
(df.filter($"High">500).count()*1.0/df.count())*100

// What is the Pearson correlation between High and Volume?


// What is the max High per year?


// What is the average Close for each Calender Month?

