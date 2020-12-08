import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Column, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

object test {
  def main(args: Array[String]): Unit = {
    val combinedDF = combineData()
    printResults(combinedDF)

    // Creating object of crashes class
    val obj_crash = new crashes("DORCHESTER",58,null)
    val crash_df = obj_crash.filter_crashes(combinedDF)
    //val crashes = obj_crash.crash_records(crash_df)
    //printResults(crashes)

    //split_columns_vehicle(crash_df,"VEHC_CONFIG_CL")
    //split_columns_event(crash_df,"MOST_HRMFL_EVT_CL")
    split_columns_driver(crash_df,"DRVR_CNTRB_CIRC_CL")
  }

  def combineData(): sql.DataFrame = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    //vehicleDF.coalesce(1).write.csv("src/vehicleDF.csv")

    var personDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/person/sub/*.csv")

     personDF = personDF.drop("CRASH_DATETIME"
      ,"CRASH_STATUS"
      ,"POLC_AGNCY_TYPE_DESCR"
      ,"VEHC_TRVL_DIRC_CL"
      ,"MM_RTE"
      ,"DIST_DIRC_MILEMARKER"
      ,"MILEMARKER"
      ,"EXIT_RTE"
      ,"EXIT_NUMB"
      ,"DIST_DIRC_LANDMARK"
      ,"LANDMARK"
      ,"X"
      ,"Y"
      ,"RMV_DOC_IDS"
      ,"CRASH_RPT_IDS"
      ,"RPA_ABBR"
      ,"VEHC_EMER_USE_CL"
      ,"VEHC_TOWED_FROM_SCENE_CL"
      ,"FMCSA_RPTBL_CL"
      ,"FMCSA_RPTBL"
      ,"ROAD_CNTRB_DESCR"
      ,"SCHL_BUS_RELD_DESCR"
      ,"WORK_ZONE_RELD_DESCR"
      ,"HAZ_MAT_PLACARD_DESCR"
      ,"VEHC_REG_TYPE_CODE"
    )
    personDF.show()
    personDF.printSchema()
    personDF = personDF.dropDuplicates()
    personDF

  }

  def printResults(df: sql.DataFrame): Unit = df match {
   case df =>  println("combined total records = " + df.count())
   case _ => print("invalid")
  }

  def split_columns_vehicle(df: sql.DataFrame, column_name: String) :sql.DataFrame = {
    var dataframe = df.na.fill("V1:()", Array(column_name))
    dataframe = dataframe.withColumn("One_"+ column_name, split(col(column_name),"\\)").getItem(0))
    dataframe = dataframe.withColumn("First_" + column_name, regexp_replace(col("One_"+ column_name),"[V1:\\(]*[V2:\\(]*[V3:\\(]*[V4:\\(]","")).drop("One_"+ column_name)
    //dataframe = dataframe.withColumn("Vehicle_Config",regexp_replace(col("Vehicle_One"),"V2:\\(",""))
    //dataframe = dataframe.withColumn("Vehicle_Config",regexp_replace(col("Vehicle_One"),"V3:\\(",""))
    //dataframe = dataframe.withColumn("Vehicle_Config",regexp_replace(col("Vehicle_One"),"V4:\\(",""))
//    dataframe.rdd
//      .repartition(1)
//      .map(_.toString())
//      .saveAsTextFile("target/personDF_split_cols")
    dataframe.coalesce(1).write.option("header", "true").csv("src/splitDF.csv")
    dataframe.show()
    dataframe
  }

  def split_columns_event(df: sql.DataFrame, column_name: String) :sql.DataFrame = {
    var dataframe = df.na.fill("V1:()", Array(column_name))
    dataframe = dataframe.withColumn("Event_One", split(col(column_name), "\\)").getItem(0))
    dataframe = dataframe.withColumn(column_name + "Event_Harm", regexp_replace(col("Event_One"), "[V1:\\(]*[V2:\\(]*[V3:\\(]*[V4:\\(]", ""))
    dataframe.show()
    dataframe
  }

    def split_columns_driver(df: sql.DataFrame, column_name: String) :sql.DataFrame = {
      var dataframe = df.na.fill("D1:()", Array(column_name))
      dataframe = dataframe.withColumn("Driver_One", split(col(column_name),"[\\)]*[\\/]").getItem(0))
      dataframe = dataframe.withColumn("Driver_Control",regexp_replace(col("Driver_One"),"[D1:]*[D2:]*[D3:]*[D4:]","")).drop("Driver_One")
      dataframe = dataframe.withColumn("First",regexp_replace(col("Driver_Control"),"[\\(]*[\\)]","")).drop("Driver_Control")
      dataframe = dataframe.withColumn("First_" + column_name,regexp_replace(col("First"),"[\\(]","")).drop("First")
      //dataframe.coalesce(1).write.option("header", "true").csv("src/splitDriver.csv")
      dataframe.show()
      dataframe

  }
}

