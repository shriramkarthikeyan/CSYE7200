import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.util.control.Breaks._

// Primary constructor
class crashes(locality: String){

  var age:Int = -1
  var vehicle:String = null
  var weather:String = null
  var road:String = null
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark")
    .getOrCreate()

  // Auxiliary Constructor
  def this(locality: String, age: Int, vehicle: String, weather: String, road: String) {
    this(locality) // Invoking primary constructor
    this.age = age
    this.vehicle = vehicle
    this.weather = weather
    this.road = road
  }

  // Method to filter records as per specific value in a columns
  def filter_records(df:sql.DataFrame, column: String, value: String): sql.DataFrame ={
    var filtered_data = df.filter(col(column).like("%"+value+"%"))
    filtered_data
  }

  // Method to get records based on constructor values
  def crash_records(df:sql.DataFrame): sql.DataFrame = {
    try {
      var data = df
      var desiredThings:Map[String, String] = Map() // Defining empty map
      // Adding values to the map
      (if (locality != null) desiredThings = desiredThings + ("LCLTY_NAME" -> locality))
      (if (age > 0) desiredThings = desiredThings + ("DRIVER_AGE" -> age.toString))
      (if (vehicle != null) desiredThings = desiredThings + ("VEHC_CONFIG_CL" -> vehicle))
      (if (weather != null) desiredThings = desiredThings + ("WEATH_COND_DESCR" -> weather))
      (if (road != null) desiredThings = desiredThings + ("ROAD_SURF_COND_DESCR" -> road))

      var df_c = df.filter(col("DRIVER_AGE").like("-99")) // creating empty dataframe
      //println("desiredThings = " + desiredThings)

      breakable
      { // Iterating through map
        desiredThings.keys.foreach { i =>
          //println("Key = " + i)
          //println(" Value = " + desiredThings(i))
          df_c = filter_records(data,col(i).toString(),desiredThings(i)) // filtering records as per values in map
          data = df_c

        }
      }

      //println("Number of records = " + data.count())
      data.show()
      data
    }
    catch {
      case e: Exception => //println("No records found", e)
        val dataf = spark.emptyDataFrame
        dataf
    }
  }

  // Method to filter specific columns from a dataframe
  def filter_crashes(df: sql.DataFrame): sql.DataFrame = {
    var DTInput = df.select(
      "CRASH_DATETIME"
    ,"DISTRICT_NUM"
       ,"LCLTY_NAME"
       ,"OWNER_ADDR_CITY_TOWN"
       ,"OWNER_ADDR_STATE"
       ,"VEHC_REG_STATE"
       ,"WEATH_COND_DESCR"
       ,"ROAD_SURF_COND_DESCR"
       ,"FIRST_HRMF_EVENT_DESCR"
       ,"VEHC_CONFG_DESCR"
       ,"AGE_DRVR_YNGST"
       ,"AGE_DRVR_OLDEST"
       ,"DRIVER_DISTRACTED_TYPE_DESCR"
       ,"DRVR_LCN_STATE"
       ,"DRUG_SUSPD_TYPE_DESCR"
       ,"ALC_SUSPD_TYPE_DESCR"
      ,"DRVR_CNTRB_CIRC_CL"
    )
    DTInput = DTInput.dropDuplicates() // dropping duplicates
    DTInput
  }
}