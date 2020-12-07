import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.util.control.Breaks._

// Primary constructor
class crashes(locality: String){

  var age:Int = 0
  var vehicle:String = null
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark")
    .getOrCreate()

  // Auxiliary Constructor
  def this(locality: String, age: Int, vehicle: String) {
    this(locality) // Invoking primary constructor
    this.age = age
    this.vehicle = vehicle
  }

  def filter_records(df:sql.DataFrame, column: String, value: String): sql.DataFrame ={
    var filtered_data = df.filter(col(column).like(value))
    filtered_data
  }

  def crash_records(df:sql.DataFrame): sql.DataFrame = {
    try {
      var data = df
      var desiredThings:Map[String, String] = Map()
      (if (locality != null) desiredThings = desiredThings + ("LCLTY_NAME" -> locality))
      (if (age > 0) desiredThings = desiredThings + ("DRIVER_AGE" -> age.toString))
      (if (vehicle != null) desiredThings = desiredThings + ("VEHC_CONFIG_CL" -> vehicle))

      var df_c = df.filter(col("DRIVER_AGE").like("-99"))
      println("desiredThings = " + desiredThings)

      breakable
      {
        desiredThings.keys.foreach { i =>
          println("Key = " + i)
          println(" Value = " + desiredThings(i))
          df_c = filter_records(data,col(i).toString(),desiredThings(i))
          data = df_c

//          if (data.filter(col(i).like(desiredThings(i))).count() == 0) (
//            df_c = data.filter(col("DRIVER_AGE").like("-99"))
//            )
//          if (data.filter(col(i).like(desiredThings(i))).count() == 0) (
//            break
//            )
        }
      }
//      desiredThings.keys.foreach { i =>
//        df_c = df_c.filter(col(i).isNotNull)
//      }
//
//      println("union :" + df_c.count())
//
////      println("crash df")
////      val crash_df2 = df.filter(
////        col("LCLTY_NAME").like(locality) &&
////        (col("DRIVER_AGE").like(age.toString)) //&&
////        //(col("VEHC_CONFIG_CL").like(vehicle))
////      )
////      crash_df2.count()
////      crash_df2.show()
//
//      df_c.createOrReplaceTempView("crashes")
//      val crash_df = spark.sql(
//        "select * " +
//          "from crashes "+
//          "group by STREET_NUMB,RDWY,DIST_DIRC_FROM_INT,NEAR_INT_RDWY,DIST_DIRC_EXIT,LAT,LON,DISTRICT_NUM,LCLTY_NAME,OWNER_ADDR_CITY_TOWN,OWNER_ADDR_STATE,VEHC_REG_STATE,WEATH_COND_DESCR,ROAD_SURF_COND_DESCR,MAX_INJR_SVRTY_CL,MANR_COLL_DESCR,FIRST_HRMF_EVENT_DESCR,MOST_HRMFL_EVT_CL,DRVR_CNTRB_CIRC_CL,VEHC_CONFIG_CL,HIT_RUN_DESCR,AGE_DRVR_YNGST,AGE_DRVR_OLDEST,DRVR_DISTRACTED_CL,DRIVER_AGE,DRIVER_DISTRACTED_TYPE_DESCR,DRVR_LCN_STATE,DRUG_SUSPD_TYPE_DESCR,SFTY_EQUP_DESC_1,SFTY_EQUP_DESC_2,ALC_SUSPD_TYPE_DESCR " +
//          "having count(*) > 1")
//
//      crash_df.show()
//      crash_df
      data.count()
      data.show()
      data
    }
    catch {
      case e: Exception => println("No records found", e)
        val dataf = spark.emptyDataFrame
        dataf
    }
  }

  def filter_crashes(df: sql.DataFrame): sql.DataFrame = {
    var DTInput = df.select(
      "STREET_NUMB"
      ,"RDWY"
      ,"DIST_DIRC_FROM_INT"
      ,"NEAR_INT_RDWY"
      ,"DIST_DIRC_EXIT"
      ,"LAT"
      ,"LON"
      ,"DISTRICT_NUM"
      ,"LCLTY_NAME"
      ,"OWNER_ADDR_CITY_TOWN"
      ,"OWNER_ADDR_STATE"
      ,"VEHC_REG_STATE"
      ,"WEATH_COND_DESCR"
      ,"ROAD_SURF_COND_DESCR"
      ,"MAX_INJR_SVRTY_CL"
      ,"MANR_COLL_DESCR"
      ,"FIRST_HRMF_EVENT_DESCR"
      ,"MOST_HRMFL_EVT_CL"
      ,"DRVR_CNTRB_CIRC_CL"
      ,"VEHC_CONFIG_CL"
      ,"HIT_RUN_DESCR"
      ,"AGE_DRVR_YNGST"
      ,"AGE_DRVR_OLDEST"
      ,"DRVR_DISTRACTED_CL"
      ,"DRIVER_AGE"
      ,"DRIVER_DISTRACTED_TYPE_DESCR"
      ,"DRVR_LCN_STATE"
      ,"DRUG_SUSPD_TYPE_DESCR"
      ,"SFTY_EQUP_DESC_1"
      ,"SFTY_EQUP_DESC_2"
      ,"ALC_SUSPD_TYPE_DESCR"
    )
    DTInput = DTInput.dropDuplicates()
    DTInput
  }
}