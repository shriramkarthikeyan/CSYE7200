import org.apache.spark.ml.feature.{OneHotEncoder, PCA, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{monotonically_increasing_id, when}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.split
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.functions.col
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils


object MLUseCase {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    var personDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\shrir\\OneDrive - Northeastern University\\CSYE7200 Big Data Systems Enginnering with " +
        "Scala - Project\\Data\\person\\sub\\*.csv")


    personDF = personDF.drop("CRASH_DATETIME","CRASH_STATUS","POLC_AGNCY_TYPE_DESCR","VEHC_TRVL_DIRC_CL","MM_RTE","DIST_DIRC_MILEMARKER","MILEMARKER","EXIT_RTE","EXIT_NUMB","DIST_DIRC_LANDMARK","LANDMARK","X","Y","RMV_DOC_IDS","CRASH_RPT_IDS","RPA_ABBR","VEHC_EMER_USE_CL","VEHC_TOWED_FROM_SCENE_CL","FMCSA_RPTBL_CL","FMCSA_RPTBL","ROAD_CNTRB_DESCR","SCHL_BUS_RELD_DESCR","WORK_ZONE_RELD_DESCR","HAZ_MAT_PLACARD_DESCR","VEHC_REG_TYPE_CODE","NON_MTRST_TYPE_CL","NON_MTRST_ACTN_CL","NON_MTRST_LOC_CL","AADT","AADT_YEAR","PK_PCT_SUT","AV_PCT_SUT","PK_PCT_CT","AV_PCT_CT","CURB","TRUCK_RTE","LT_SIDEWLK","RT_SIDEWLK","SHLDR_LT_W","SHLDR_LT_T","SURFACE_WD","SHLDR_RT_W","SHLDR_RT_T","OPP_LANES","MED_WIDTH","MED_TYPE","URBAN_TYPE","F_CLASS","URBAN_AREA","FD_AID_RTE","FACILITY","OPERATION","CONTROL","PEAK_LANE","STREETNAME","FROMSTREETNAME","TOSTREETNAME","CITY","STRUCT_CND","TERRAIN","URBAN_LOC_TYPE","AADT_DERIV","STATN_NUM","OP_DIR_SL","SHLDR_UL_T","SHLDR_UL_W","T_EXC_TYPE","T_EXC_TIME","F_F_CLASS"
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

      "LAT"
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
      ,"FATALITY_BIN"
    )
    val cleanDF = cleanData(DTInput)
    predictFatalityLR(cleanDF)
    naiveBayesModel(cleanDF)

  }

  def filterAge(df:sql.DataFrame, age:Int) : sql.DataFrame = {
    var df2 = df.filter(df("DRIVER_AGE") === age)
    df2
  }

  def cleanData(df:sql.DataFrame) : sql.DataFrame = {
    df.printSchema()
    println("count before drop: ", df.count())
    val df1 = df.na.fill("NA")
    println("count after drop: ", df1.count())
    //    val training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val indexer = new StringIndexer()
      .setInputCols(Array(
        "OWNER_ADDR_CITY_TOWN"
        , "OWNER_ADDR_STATE"
        , "VEHC_REG_STATE"
        , "WEATH_COND_DESCR"
        , "ROAD_SURF_COND_DESCR"
        , "MAX_INJR_SVRTY_CL"
        , "MANR_COLL_DESCR"
        , "FIRST_HRMF_EVENT_DESCR"
        , "MOST_HRMFL_EVT_CL"
        , "DRVR_CNTRB_CIRC_CL"
        , "VEHC_CONFIG_CL"
        , "HIT_RUN_DESCR"
        , "AGE_DRVR_YNGST"
        , "AGE_DRVR_OLDEST"
        , "DRVR_DISTRACTED_CL"
        , "DRIVER_DISTRACTED_TYPE_DESCR"
        , "DRVR_LCN_STATE"
        , "DRUG_SUSPD_TYPE_DESCR"
        , "SFTY_EQUP_DESC_1"
        , "SFTY_EQUP_DESC_2"
        , "ALC_SUSPD_TYPE_DESCR"))
      .setOutputCols(Array(
        "OWNER_ADDR_CITY_TOWN_index"
        , "OWNER_ADDR_STATE_index"
        , "VEHC_REG_STATE_index"
        , "WEATH_COND_DESCR_index"
        , "ROAD_SURF_COND_DESCR_index"
        , "MAX_INJR_SVRTY_CL_index"
        , "MANR_COLL_DESCR_index"
        , "FIRST_HRMF_EVENT_DESCR_index"
        , "MOST_HRMFL_EVT_CL_index"
        , "DRVR_CNTRB_CIRC_CL_index"
        , "VEHC_CONFIG_CL_index"
        , "HIT_RUN_DESCR_index"
        , "AGE_DRVR_YNGST_index"
        , "AGE_DRVR_OLDEST_index"
        , "DRVR_DISTRACTED_CL_index"
        , "DRIVER_DISTRACTED_TYPE_DESCR_index"
        , "DRVR_LCN_STATE_index"
        , "DRUG_SUSPD_TYPE_DESCR_index"
        , "SFTY_EQUP_DESC_1_index"
        , "SFTY_EQUP_DESC_2_index"
        , "ALC_SUSPD_TYPE_DESCR_index"))

    val indexed = indexer.fit(df1).transform(df1)
    indexed.show()


    val assembler = new VectorAssembler()
      .setInputCols(Array("LAT"
        , "LON"
        , "OWNER_ADDR_CITY_TOWN_index"
        , "OWNER_ADDR_STATE_index"
        , "VEHC_REG_STATE_index"
        , "WEATH_COND_DESCR_index"
        , "ROAD_SURF_COND_DESCR_index"
        , "MAX_INJR_SVRTY_CL_index"
        , "MANR_COLL_DESCR_index"
        , "FIRST_HRMF_EVENT_DESCR_index"
        , "MOST_HRMFL_EVT_CL_index"
        , "DRVR_CNTRB_CIRC_CL_index"
        , "VEHC_CONFIG_CL_index"
        , "HIT_RUN_DESCR_index"
        , "AGE_DRVR_YNGST_index"
        , "AGE_DRVR_OLDEST_index"
        , "DRVR_DISTRACTED_CL_index"
        , "DRIVER_DISTRACTED_TYPE_DESCR_index"
        , "DRVR_LCN_STATE_index"
        , "DRUG_SUSPD_TYPE_DESCR_index"
        , "SFTY_EQUP_DESC_1_index"
        , "SFTY_EQUP_DESC_2_index"
        , "ALC_SUSPD_TYPE_DESCR_index"))
      .setOutputCol("features")
      .setHandleInvalid("skip")
    println("assembler: ", assembler)
    val df3 = assembler.transform(indexed).select(col("FATALITY_BIN").cast(DoubleType).as("label"), col("features"))
    df3
  }

  def predictFatalityLR(df:sql.DataFrame) : Unit = {

    println("assembler count from df3: ",df.count())
//    df3.write.mode('append').json("target\assembler.json")
    print("hello")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    println("set params")
    // Fit the model
    val lrModel = lr.fit(df)
    println("fit")
    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(df)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")


    /*
    Summary
     */
    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    val trainingSummary = lrModel.binarySummary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = trainingSummary.roc
    roc.show()
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

//    // Set the model threshold to maximize F-Measure
//    val fMeasure = trainingSummary.fMeasureByThreshold
//    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
//    val bestThreshold = fMeasure.where(col("F-Measure") === maxFMeasure)
//      .select("threshold").head().getDouble(0)
//    lrModel.setThreshold(bestThreshold)



  }
  def naiveBayesModel(df:sql.DataFrame) = {
    //Naive Bayes
    //    val conf = new SparkConf().setAppName("NaiveBayesExample")
    //    val sc = new SparkContext(conf)
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    println(data)
    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    // Save and load model
    model.save(sc, "target/tmp/myNaiveBayesModel")
    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
    // $example off$
  }




}


