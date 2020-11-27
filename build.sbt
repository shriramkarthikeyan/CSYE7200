name := "Big_Data_Project_MA_Vehichle_Crash_Analysis"

version := "0.1"

scalaVersion := "2.12.12"
libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
   "org.apache.spark" %% "spark-sql" % "3.0.1",

  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "3.0.1",

  // https://mvnrepository.com/artifact/org.apache.spark/spark-hive
  "org.apache.spark" %% "spark-hive" % "3.0.1" % "provided",

  // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
  "org.apache.spark" %% "spark-mllib" % "3.0.1" % "provided"


)