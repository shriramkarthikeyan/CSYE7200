import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, sql}

object test {
  def main(args: Array[String]): Unit = {
  var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val vehicleDF = sqlContext.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/vehicle/*.csv")

  val vehicleDF2 = vehicleDF.drop("CRASH_DATETIME"
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

  vehicleDF2.printSchema()

  println(vehicleDF2.count())
  vehicleDF2.show()

  }

}



