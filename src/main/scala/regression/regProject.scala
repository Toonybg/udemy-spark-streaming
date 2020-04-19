package regression
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.regression.LinearRegression
// Import VectorAssembler and Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.log4j._


object regProject extends App { ////////////////////////////////////////////
  //// LINEAR REGRESSION EXERCISE ///////////
  /// Complete the commented tasks below ///

  // Optional: Use the following code below to set the Error reporting
  Logger.getLogger("org").setLevel(Level.ERROR)


  // Spark Session
  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

  // reading Ecommerce Customers csv file with header and infered schema
  val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("resources/datasets/Ecommerce Customers_c.csv")

  // preview schema
  data.printSchema()

  // Print out an example Row
  // Various ways to do this, just
  // choose whichever way you prefer
  val colnames = data.columns
  val firstrow = data.head(1)(0)
  println("\n")
  println("Example Data Row")
  for(ind <- Range(1,colnames.length)){
    println(colnames(ind))
    println(firstrow(ind))
    println("\n")
  }


  //// Setting Up DataFrame for Machine Learning ////

  // Data has to be be in the form of two columns
  // ("label","features")


  // For Scala/Spark $ Syntax
  import spark.implicits._
  // Yearly Amount Spent Column to -> "label"
  // Get only numerical columns from the data
  val df = data.select(data("Yearly Amount Spent").as("label"),$"Avg Session Length",$"Time on App",$"Time on Website",$"Length of Membership")

  // An assembler converts the input values to a vector
  // A vector is what the ML algorithm reads to train a model

  // converting the input columns of df with VA to 'features'
  // seting the colomus to read the apropriate values
  val assembler = new VectorAssembler().setInputCols(Array("Avg Session Length","Time on App","Time on Website","Length of Membership")).setOutputCol("features")

  // transforming df to the two columns: label and features with an assembler
  val output = assembler.transform(df).select($"label",$"features")


  // createing a Linear Regression Model object
  val lr = new LinearRegression()

  // fiting the model to the data
  val lrModel = lr.fit(output)

  // coefficients and intercept for linear regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  // some summarizeing informations/stats for the model
  val trainingSummary = lrModel.summary

  // model quality: RMSE, the MSE, and R^2
  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"MSE: ${trainingSummary.meanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")

}
