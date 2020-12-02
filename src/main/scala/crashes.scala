import org.apache.spark.sql

// Primary constructor
class crashes(locality: String){

  var age:Int = 0
  var vehicle:String = null

  // Auxiliary Constructor
  def this(locality: String, age: Int, vehicle: String) {
    this(locality) // Invoking primary constructor
    this.age = age
    this.vehicle = vehicle
  }

  def crash_records(df:sql.DataFrame): sql.DataFrame = {
    try {
      val crash_df = df.filter(
        df("LCLTY_NAME").like(locality) ||
        (df("DRIVER_AGE") === age) ||
        (df("VEHC_CONFIG_CL").like(vehicle))
      )
      crash_df.show()
      crash_df
    }
//    catch {
//      case e: Exception => println("No records found", e) None
//    }
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
    DTInput
  }
}