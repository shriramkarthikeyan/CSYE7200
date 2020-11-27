import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

df = SQLContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .load("../Downloads/*.csv")