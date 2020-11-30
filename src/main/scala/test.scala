import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{monotonically_increasing_id, when}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.split
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.OneHotEncoder



object test {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    var personDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\shrir\\OneDrive - Northeastern University\\CSYE7200 Big Data Systems Enginnering with " +
        "Scala - Project\\Data\\person\\sub\\*.csv")


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
      ,"NON_MTRST_TYPE_CL"
      ,"NON_MTRST_ACTN_CL"
      ,"NON_MTRST_LOC_CL"
      ,"AADT"
      ,"AADT_YEAR"
      ,"PK_PCT_SUT"
      ,"AV_PCT_SUT"
      ,"PK_PCT_CT"
      ,"AV_PCT_CT"
      ,"CURB"
      ,"TRUCK_RTE"
      ,"LT_SIDEWLK"
      ,"RT_SIDEWLK"
      ,"SHLDR_LT_W"
      ,"SHLDR_LT_T"
      ,"SURFACE_WD"
      ,"SHLDR_RT_W"
      ,"SHLDR_RT_T"
      ,"OPP_LANES"
      ,"MED_WIDTH"
      ,"MED_TYPE"
      ,"URBAN_TYPE"
      ,"F_CLASS"
      ,"URBAN_AREA"
      ,"FD_AID_RTE"
      ,"FACILITY"
      ,"OPERATION"
      ,"CONTROL"
      ,"PEAK_LANE"
      ,"STREETNAME"
      ,"FROMSTREETNAME"
      ,"TOSTREETNAME"
      ,"CITY"
      ,"STRUCT_CND"
      ,"TERRAIN"
      ,"URBAN_LOC_TYPE"
      ,"AADT_DERIV"
      ,"STATN_NUM"
      ,"OP_DIR_SL"
      ,"SHLDR_UL_T"
      ,"SHLDR_UL_W"
      ,"T_EXC_TYPE"
      ,"T_EXC_TIME"
      ,"F_F_CLASS"
    )

    personDF.printSchema()

    println(personDF.count())

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._ //
    var VEHC_SEQ_EVENTS_DF = personDF.withColumn("VEHC_SEQ_EVENTS", split(col("VEHC_SEQ_EVENTS"), "\\SEQ")).select(
      col("VEHC_SEQ_EVENTS").getItem(0).as("col1"),
      col("VEHC_SEQ_EVENTS").getItem(1).as("col2"),
      col("VEHC_SEQ_EVENTS").getItem(2).as("col3")
    )
    personDF = personDF.withColumn("id", monotonically_increasing_id())
    VEHC_SEQ_EVENTS_DF = VEHC_SEQ_EVENTS_DF.withColumn("id", monotonically_increasing_id())

    personDF = personDF.join(VEHC_SEQ_EVENTS_DF,usingColumn = "id")
    personDF.show()
    personDF.printSchema()
    personDF.select("DRIVER_AGE","AGE").show()
    filterAge(personDF,46).select("CRASH_NUMB","DRIVER_AGE").show()
    personDF = personDF.withColumn("FATALITY_BIN", when(col("NUMB_FATAL_INJR") === 0, 0).otherwise(1))

    val DTInput = personDF.select(
//      "STREET_NUMB"
//      ,"RDWY"
//      ,"DIST_DIRC_FROM_INT"
//      ,"NEAR_INT_RDWY"
//      ,"DIST_DIRC_EXIT",
      "LAT"
      ,"LON"
//      ,"DISTRICT_NUM"
//      ,"LCLTY_NAME"
//      ,"OWNER_ADDR_CITY_TOWN"
      ,"OWNER_ADDR_STATE"
//      ,"VEHC_REG_STATE"
//      ,"WEATH_COND_DESCR"
//      ,"ROAD_SURF_COND_DESCR"
//      ,"MAX_INJR_SVRTY_CL"
//      ,"MANR_COLL_DESCR"
//      ,"FIRST_HRMF_EVENT_DESCR"
//      ,"MOST_HRMFL_EVT_CL"
//      ,"DRVR_CNTRB_CIRC_CL"
//      ,"VEHC_CONFIG_CL"
//      ,"HIT_RUN_DESCR"
//      ,"AGE_DRVR_YNGST"
//      ,"AGE_DRVR_OLDEST"
//      ,"DRVR_DISTRACTED_CL"
//      ,"DRIVER_AGE"
//      ,"DRIVER_DISTRACTED_TYPE_DESCR"
//      ,"DRVR_LCN_STATE"
//      ,"DRUG_SUSPD_TYPE_DESCR"
//      ,"SFTY_EQUP_DESC_1"
//      ,"SFTY_EQUP_DESC_2"
//      ,"ALC_SUSPD_TYPE_DESCR"
      ,"FATALITY_BIN"
    )
    predictFatality(DTInput)

  }

  def filterAge(df:sql.DataFrame, age:Int) : sql.DataFrame = {
    var df2 = df.filter(df("DRIVER_AGE") === age)
    df2
  }

  def predictFatality(df:sql.DataFrame) : Unit = {
    println("predictFatality Input")
    df.printSchema()
//    val training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val encoder = new OneHotEncoder()
      .setInputCols(Array(
        "OWNER_ADDR_STATE"
        ))
      .setOutputCols(Array(
        "OWNER_ADDR_STATE_VEC"
        ))

    print(encoder)
    val model = encoder.fit(df)

    val encoded = model.transform(df)
    println("encoded:")
    encoded.show()
    val assembler =  new VectorAssembler()
      .setInputCols(Array("LAT"
        ,"LON"
        ,"OWNER_ADDR_STATE_VEC"))
      .setOutputCol("features")
    println("assembler: ",assembler)
    val df3 = assembler.transform(df).select(col("FATALITY_BIN").cast(DoubleType).as("label"), col("features"))

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(encoded)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(encoded)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
  }

}



