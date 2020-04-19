package pca
// Import PCA, VectorAssembler and StandardScaler from ml.feature
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession

// Import Vectors from ml.linalg
import org.apache.spark.ml.linalg.Vectors
object pcaProject extends  App{
  // PCA Project Exercise

  /////////////////////////////////
  // THE DATA SET DESCRIPTION ////
  ///////////////////////////////

  // Here is a description of the dataset we will be using:

  // Breast Cancer Wisconsin (Diagnostic) Database
  //
  // Notes
  // -----
  // Data Set Characteristics:
  //     :Number of Instances: 569
  //
  //     :Number of Attributes: 30 numeric, predictive attributes and the class
  //
  //     :Attribute Information:
  //         - radius (mean of distances from center to points on the perimeter)
  //         - texture (standard deviation of gray-scale values)
  //         - perimeter
  //         - area
  //         - smoothness (local variation in radius lengths)
  //         - compactness (perimeter^2 / area - 1.0)
  //         - concavity (severity of concave portions of the contour)
  //         - concave points (number of concave portions of the contour)
  //         - symmetry
  //         - fractal dimension ("coastline approximation" - 1)
  //
  //         The mean, standard error, and "worst" or largest (mean of the three
  //         largest values) of these features were computed for each image,
  //         resulting in 30 features.  For instance, field 3 is Mean Radius, field
  //         13 is Radius SE, field 23 is Worst Radius.
  //
  //         - class:
  //                 - WDBC-Malignant
  //                 - WDBC-Benign
  //
  //     :Summary Statistics:
  //
  //     ===================================== ======= ========
  //                                            Min     Max
  //     ===================================== ======= ========
  //     radius (mean):                         6.981   28.11
  //     texture (mean):                        9.71    39.28
  //     perimeter (mean):                      43.79   188.5
  //     area (mean):                           143.5   2501.0
  //     smoothness (mean):                     0.053   0.163
  //     compactness (mean):                    0.019   0.345
  //     concavity (mean):                      0.0     0.427
  //     concave points (mean):                 0.0     0.201
  //     symmetry (mean):                       0.106   0.304
  //     fractal dimension (mean):              0.05    0.097
  //     radius (standard error):               0.112   2.873
  //     texture (standard error):              0.36    4.885
  //     perimeter (standard error):            0.757   21.98
  //     area (standard error):                 6.802   542.2
  //     smoothness (standard error):           0.002   0.031
  //     compactness (standard error):          0.002   0.135
  //     concavity (standard error):            0.0     0.396
  //     concave points (standard error):       0.0     0.053
  //     symmetry (standard error):             0.008   0.079
  //     fractal dimension (standard error):    0.001   0.03
  //     radius (worst):                        7.93    36.04
  //     texture (worst):                       12.02   49.54
  //     perimeter (worst):                     50.41   251.2
  //     area (worst):                          185.2   4254.0
  //     smoothness (worst):                    0.071   0.223
  //     compactness (worst):                   0.027   1.058
  //     concavity (worst):                     0.0     1.252
  //     concave points (worst):                0.0     0.291
  //     symmetry (worst):                      0.156   0.664
  //     fractal dimension (worst):             0.055   0.208
  //     ===================================== ======= ========
  //
  //     :Missing Attribute Values: None
  //
  //     :Class Distribution: 212 - Malignant, 357 - Benign
  //
  //     :Creator:  Dr. William H. Wolberg, W. Nick Street, Olvi L. Mangasarian
  //
  //     :Donor: Nick Street
  //
  //     :Date: November, 1995



  //////////////////////////////
  // PROJECT EXERCISE TASKS ///
  ////////////////////////////

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("PCA_Example").config("spark.master", "local").getOrCreate()

  // Use Spark to read in the Cancer_Data file.
  val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("resources/datasets/Cancer_Data")

  // Print the Schema of the data
  data.printSchema()



  // Use VectorAssembler to convert the input columns of the cancer data
  // to a single output column of an array called "features"
  // Set the input columns from which we are supposed to read the values.
  // Call this new object assembler.

  // Since there are so many columns, you may find this line useful
  // to just pass in to setInputCols

  val colnames = (Array("mean radius", "mean texture", "mean perimeter", "mean area", "mean smoothness",
    "mean compactness", "mean concavity", "mean concave points", "mean symmetry", "mean fractal dimension",
    "radius error", "texture error", "perimeter error", "area error", "smoothness error", "compactness error",
    "concavity error", "concave points error", "symmetry error", "fractal dimension error", "worst radius",
    "worst texture", "worst perimeter", "worst area", "worst smoothness", "worst compactness", "worst concavity",
    "worst concave points", "worst symmetry", "worst fractal dimension"))

  //Seting input colums with a Vector Assembler
  val assembler = new VectorAssembler().setInputCols(colnames).setOutputCol("features")

  import spark.implicits._
  // transforming  data frame to a single column: "features"
  val output = assembler.transform(data).select($"features")

  // Often its a good idea to normalize each feature to have unit standard
  // deviation and/or zero mean,vwhen using PCA.
  // This is essentially a pre-step to PCA, but its not always necessary.

  // Look at the ml.feature documentation and figure out how to standardize
  // the cancer data set. Refer to the solutions for hints if you get stuck.

  // using StandardScaler on the data
  val scaler = (new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(false))



  // fitting thge output ti scakerMidek
  val scalerModel = scaler.fit(output)

  // normalizing each feature to have unit standard deviation.
  // transform() of scalerModel object to create scaledData
  val scaledData = scalerModel.transform(output)



  // output the pcs features with 4 principal components
  // fit the data to the scaledData
  val pca = (new PCA()
    .setInputCol("scaledFeatures")
    .setOutputCol("pcaFeatures")
    .setK(4)
    .fit(scaledData))

  // transform the scaledData to pvaDataFrame
  val pcaDF = pca.transform(scaledData)

  // print pcaFeatures
  val result = pcaDF.select("pcaFeatures")
  result.show()

  // Use .head() to confirm that your output column Array of pcaFeatures
  result.head(1)
  // has only 4 principal components value > .5

}
