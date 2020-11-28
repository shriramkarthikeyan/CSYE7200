import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id
//import sparkObject.spark.implicits._
import org.apache.spark.sql.functions.split

object test {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    var personDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\shrir\\OneDrive - Northeastern University\\CSYE7200 Big Data Systems Enginnering with " +
        "Scala - Project\\Data\\person\\*.csv")

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
      ,"VEHC_REG_TYPE_CODE")

    personDF.printSchema()

    println(personDF.count())

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._ //
    var VEHC_SEQ_EVENTS_DF = personDF.withColumn("VEHC_SEQ_EVENTS", split($"VEHC_SEQ_EVENTS", "\\SEQ")).select(
      $"VEHC_SEQ_EVENTS".getItem(0).as("col1"),
      $"VEHC_SEQ_EVENTS".getItem(1).as("col2"),
      $"VEHC_SEQ_EVENTS".getItem(2).as("col3")
    )
    personDF = personDF.withColumn("id", monotonically_increasing_id())
    VEHC_SEQ_EVENTS_DF = VEHC_SEQ_EVENTS_DF.withColumn("id", monotonically_increasing_id())

    personDF = personDF.join(VEHC_SEQ_EVENTS_DF,usingColumn = "id")
    personDF.show()
    personDF.printSchema()
  }

}



