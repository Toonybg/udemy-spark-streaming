import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

val logger = Logger.getLogger(getClass.getName)
logger.warn("Hello World Started")
// initialise spark context
// val conf = new SparkConf().setAppName("HelloWorld")
val spark = SparkSession.builder.getOrCreate()
spark.catalog.listTables().show
spark.version
    
println("test print")

