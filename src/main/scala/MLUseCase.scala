import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.functions.{array, col, regexp_replace, split, when}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors




object MLUseCase {
  var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc)
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._ //
  def main(args: Array[String]): Unit = {

//    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//    val sqlContext = new SQLContext(sc)
    //Load all the source CSV files into a sql.DataFrame
    var personDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\shrir\\OneDrive - Northeastern University\\CSYE7200 Big Data Systems Enginnering with " +
        "Scala - Project\\Data\\person\\*.csv")

    //Drop the values which are not needed
    personDF = personDF.drop("CRASH_STATUS","POLC_AGNCY_TYPE_DESCR","VEHC_TRVL_DIRC_CL","MM_RTE","DIST_DIRC_MILEMARKER","MILEMARKER","EXIT_RTE","EXIT_NUMB","DIST_DIRC_LANDMARK","LANDMARK","X","Y","RMV_DOC_IDS","CRASH_RPT_IDS","RPA_ABBR","VEHC_EMER_USE_CL","VEHC_TOWED_FROM_SCENE_CL","FMCSA_RPTBL_CL","FMCSA_RPTBL","ROAD_CNTRB_DESCR","SCHL_BUS_RELD_DESCR","WORK_ZONE_RELD_DESCR","HAZ_MAT_PLACARD_DESCR","VEHC_REG_TYPE_CODE","NON_MTRST_TYPE_CL","NON_MTRST_ACTN_CL","NON_MTRST_LOC_CL","AADT","AADT_YEAR","PK_PCT_SUT","AV_PCT_SUT","PK_PCT_CT","AV_PCT_CT","CURB","TRUCK_RTE","LT_SIDEWLK","RT_SIDEWLK","SHLDR_LT_W","SHLDR_LT_T","SURFACE_WD","SHLDR_RT_W","SHLDR_RT_T","OPP_LANES","MED_WIDTH","MED_TYPE","URBAN_TYPE","F_CLASS","URBAN_AREA","FD_AID_RTE","FACILITY","OPERATION","CONTROL","PEAK_LANE","STREETNAME","FROMSTREETNAME","TOSTREETNAME","CITY","STRUCT_CND","TERRAIN","URBAN_LOC_TYPE","AADT_DERIV","STATN_NUM","OP_DIR_SL","SHLDR_UL_T","SHLDR_UL_W","T_EXC_TYPE","T_EXC_TIME","F_F_CLASS"
    )
    // Drop duplicate values
    personDF = personDF.dropDuplicates()
//    personDF.printSchema()

//    println(personDF.count())
    // Create  a categorical column variable to specify whether a fatality took place in that crash or not based ont he number of fatal injuries
    personDF = personDF.withColumn("FATALITY_BIN", when(col("NUMB_FATAL_INJR") === 0, 0).otherwise(1))
    val personDFSubset = personDF.select(

      "LAT"
      ,"LON"
      ,"CRASH_DATETIME"
      ,"DISTRICT_NUM"
      ,"LCLTY_NAME"
      ,"OWNER_ADDR_CITY_TOWN"
      ,"OWNER_ADDR_STATE"
      ,"VEHC_REG_STATE"
      ,"WEATH_COND_DESCR"
      ,"ROAD_SURF_COND_DESCR"
//      ,"MAX_INJR_SVRTY_CL"
      ,"MANR_COLL_DESCR"
      ,"FIRST_HRMF_EVENT_DESCR"
//      ,"MOST_HRMFL_EVT_CL"
      ,"VEHC_CONFG_DESCR"
//      ,"HIT_RUN_DESCR"
      ,"AGE_DRVR_YNGST"
      ,"AGE_DRVR_OLDEST"
//      ,"DRVR_DISTRACTED_CL"
      ,"DRVR_CNTRB_CIRC_CL"
      ,"DRIVER_AGE"
      ,"DRIVER_DISTRACTED_TYPE_DESCR"
      ,"DRVR_LCN_STATE"
      ,"DRUG_SUSPD_TYPE_DESCR"
//      ,"SFTY_EQUP_DESC_1"
//      ,"SFTY_EQUP_DESC_2"
      ,"ALC_SUSPD_TYPE_DESCR"
      ,"FATALITY_BIN"
    )


    // split the datetime column into date parts
    val personDFSubset_dt = split_date_time(personDFSubset,"CRASH_DATETIME")
    // get the first driver contribution reason for the crash
    val personDFSubset_dt_drvr = split_columns_driver(personDFSubset_dt,"DRVR_CNTRB_CIRC_CL")
    // drop latitude, longitude and the columns we cleaned above
    val NBTrainTestInput = personDFSubset_dt_drvr.drop("LAT","LON","CRASH_DATETIME","DRVR_CNTRB_CIRC_CL")

//    NBTrainTestInput.printSchema()
//    println("NB Input: ",NBTrainTestInput.show())
    ////////////////////////////////////////////////////////
    // fill nulls with NA
    val df1 = NBTrainTestInput.na.fill("NA")
//    df1.show()
    // index the string values into categorical integers
    val indexer = new StringIndexer()
      .setInputCols(Array(
        "DISTRICT_NUM"
        ,"LCLTY_NAME"
        ,"OWNER_ADDR_CITY_TOWN"
        , "OWNER_ADDR_STATE"
        , "VEHC_REG_STATE"
        , "WEATH_COND_DESCR"
        , "ROAD_SURF_COND_DESCR"
        , "FIRST_HRMF_EVENT_DESCR"
        , "VEHC_CONFG_DESCR"
        , "AGE_DRVR_YNGST"
        , "AGE_DRVR_OLDEST"
        , "DRIVER_DISTRACTED_TYPE_DESCR"
        , "DRVR_LCN_STATE"
        , "DRUG_SUSPD_TYPE_DESCR"
        , "ALC_SUSPD_TYPE_DESCR"
        ,"First_DRVR_CNTRB_CIRC_CL"
        ,"CRASH_MONTH"
        ,"CRASH_DAY"
        ,"CRASH_YEAR"
        ,"CRASH_HOUR"
      ))
      .setOutputCols(Array(
        "DISTRICT_NUM_index"
        ,"LCLTY_NAME_index"
        ,"OWNER_ADDR_CITY_TOWN_index"
        , "OWNER_ADDR_STATE_index"
        , "VEHC_REG_STATE_index"
        , "WEATH_COND_DESCR_index"
        , "ROAD_SURF_COND_DESCR_index"
        , "FIRST_HRMF_EVENT_DESCR_index"
        , "VEHC_CONFG_DESCR_index"
        , "AGE_DRVR_YNGST_index"
        , "AGE_DRVR_OLDEST_index"
        , "DRIVER_DISTRACTED_TYPE_DESCR_index"
        , "DRVR_LCN_STATE_index"
        , "DRUG_SUSPD_TYPE_DESCR_index"
        , "ALC_SUSPD_TYPE_DESCR_index"
        ,"First_DRVR_CNTRB_CIRC_CL_index"
        ,"CRASH_MONTH_index"
        ,"CRASH_DAY_index"
        ,"CRASH_YEAR_index"
        ,"CRASH_HOUR_index"
      ))
      // if the indexer sees a new value it will keep it and assign a new value instead of throwing an error
      .setHandleInvalid("keep")

    val indexed = indexer.fit(df1).transform(df1)
//    indexed.show()

    // assemble the indexed data into a feature vector and a label containing the value of FATALITY_BIN
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "DISTRICT_NUM_index"
        ,"LCLTY_NAME_index"
        ,"OWNER_ADDR_CITY_TOWN_index"
        , "OWNER_ADDR_STATE_index"
        , "VEHC_REG_STATE_index"
        , "WEATH_COND_DESCR_index"
        , "ROAD_SURF_COND_DESCR_index"
        , "FIRST_HRMF_EVENT_DESCR_index"
        , "VEHC_CONFG_DESCR_index"
        , "AGE_DRVR_YNGST_index"
        , "AGE_DRVR_OLDEST_index"
        , "DRIVER_DISTRACTED_TYPE_DESCR_index"
        , "DRVR_LCN_STATE_index"
        , "DRUG_SUSPD_TYPE_DESCR_index"
        , "ALC_SUSPD_TYPE_DESCR_index"
        ,"First_DRVR_CNTRB_CIRC_CL_index"
        ,"CRASH_MONTH_index"
        ,"CRASH_DAY_index"
        ,"CRASH_YEAR_index"
        ,"CRASH_HOUR_index"))
      .setOutputCol("features")
      .setHandleInvalid("skip")
//    println("assembler: ", assembler)
    val df3 = assembler.transform(indexed).select(col("FATALITY_BIN").cast(DoubleType).as("label"), col("features"))

    ////////////////////////////////////
    println("running Naive Bayes model")
    // map labels and features into an RDD of labeled points
    val labeled = df3.rdd.map(row => LabeledPoint(
      row.getAs[Double]("label"),
      org.apache.spark.mllib.linalg.Vectors.fromML(row.getAs[org.apache.spark.ml.linalg.SparseVector]("features"))
    ))

    ////////////////////////////////////////////////////////
    // run naive bayes model on labeled data
    val predict_fatality_NB_model = naiveBayesModel(labeled)
//    predictProbabilityOfCrash()
//    println("predict_fatality_NB_model labels: ",predict_fatality_NB_model.labels)
//    println("predict_fatality_NB_model modelType: ",predict_fatality_NB_model.modelType)
    // create a test data point
    val test_data = List((
      "6"//"DISTRICT_NUM"
      ,"DORCHESTER" //,"LCLTY_NAME"
      ,"NA"//,"OWNER_ADDR_CITY_TOWN"
      ,"NA"//, "OWNER_ADDR_STATE"
      ,"NA"//, "VEHC_REG_STATE"
      ,"Clear"//, "WEATH_COND_DESCR"
      ,"Dry"//, "ROAD_SURF_COND_DESCR"
      ,"NA"//, "FIRST_HRMF_EVENT_DESCR"
      ,"NA"//, "VEHC_CONFG_DESCR"
      ,"NA"//, "AGE_DRVR_YNGST"
      ,"NA"//, "AGE_DRVR_OLDEST"
      ,"NA"//, "DRIVER_DISTRACTED_TYPE_DESCR"
      ,"NA"//, "DRVR_LCN_STATE"
      ,"NA"//, "DRUG_SUSPD_TYPE_DESCR"
      ,"NA"//, "ALC_SUSPD_TYPE_DESCR"
      ,"Inattention"//,"First_DRVR_CNTRB_CIRC_CL"
      ,"12"//,"CRASH_MONTH"
      ,"10"//,"CRASH_DAY"
      ,"20"//,"CRASH_YEAR"
      ,"11"//,"CRASH_HOUR"

    ))
      .toDF(
      "DISTRICT_NUM"
      ,"LCLTY_NAME"
      ,"OWNER_ADDR_CITY_TOWN"
      , "OWNER_ADDR_STATE"
      , "VEHC_REG_STATE"
      , "WEATH_COND_DESCR"
      , "ROAD_SURF_COND_DESCR"
      , "FIRST_HRMF_EVENT_DESCR"
      , "VEHC_CONFG_DESCR"
      , "AGE_DRVR_YNGST"
      , "AGE_DRVR_OLDEST"
      , "DRIVER_DISTRACTED_TYPE_DESCR"
      , "DRVR_LCN_STATE"
      , "DRUG_SUSPD_TYPE_DESCR"
      , "ALC_SUSPD_TYPE_DESCR"
      , "First_DRVR_CNTRB_CIRC_CL"
      , "CRASH_MONTH"
      , "CRASH_DAY"
      , "CRASH_YEAR"
      , "CRASH_HOUR"
    )
//    test_data.show()

    /////////////search results data frame/////////////////////////
    val obj_crash = new crashes("DORCHESTER",58,null,"Clear","Dry")
    val crash_df = obj_crash.filter_crashes(personDF)
    val crashes = obj_crash.crash_records(crash_df)
    var split_diver = split_columns_driver(crashes,"DRVR_CNTRB_CIRC_CL").drop("DRVR_CNTRB_CIRC_CL")
    var split_dt_dr = split_date_time(split_diver,"CRASH_DATETIME").drop("CRASH_DATETIME")
    //////////////////////////////////////////////


    val indexed_test_data = indexer.fit(df1).transform(split_dt_dr)
//    println("indexed_test_data: ",indexed_test_data)
//    indexed_test_data.show()
//    println("indexed_test_data count: ",indexed_test_data.count())
    val assembled_test_data = assembler.transform(indexed_test_data).select(col("features"))
//    println("assembled_test_data count: ",assembled_test_data.count())
//    println("assembled_test_data: ",assembled_test_data.take(10).foreach(x => println(x + " ")))
//    assembled_test_data.show()
    val assembled_test_data_vec = assembled_test_data.select(array(assembled_test_data.columns.map(col(_)): _*)).rdd.map(_.getSeq[Double](0))
//    assembled_test_data_vec.take(10).foreach(x => println(x + " "))
//    val assembled_test_data_vector = assembled_test_data.map{x:Row => x.getAs[Vector](0)}

    val assembled_test_data_vector = assembled_test_data
      .rdd
      .map{
        row => Vectors.dense(row.getAs[Seq[Double]]("features").toArray)
      }
//    assembled_test_data_vector.collect().foreach(println)
    val input_prediction = predict_fatality_NB_model.predict(assembled_test_data_vector)
    val input_prediction_prob = predict_fatality_NB_model.predictProbabilities(assembled_test_data_vector)
//    println("predictions completed!")
//    input_prediction.toDF().show()
//    input_prediction.take(10).foreach(x => println(x + " "))
//    input_prediction.collect().foreach(println)
//    println("input_prediction: ",input_prediction.collect().foreach(println))

//    println("input_prediction_prob: ",input_prediction_prob.collect().foreach(println))


  }


  def naiveBayesModel(labeled:RDD[LabeledPoint]) :NaiveBayesModel = {

    // Split data into training (60%) and test (40%).
    val Array(training, test) = labeled.randomSplit(Array(0.66, 0.34))
//    print("sample test data ")
//    test.take(10).foreach(x => println(x + " "))
    // train the model
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    //get the predicted values
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
//    println("prediction and label")
//    predictionAndLabel.take(10).foreach(x => println(x + " "))
    //get the predicted probabilities
    val probAndLabel = test.map(p => (model.predictProbabilities(p.features), p.label))
//    println("probability and label")
//    probAndLabel
//      .filter {case (_, v) => v == 1.0}
//      .take(10)
//      .foreach(x => println(x + " "))
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
//    println("accuracy of Naive Bayes: ", accuracy)

//    ////////////////convert index back to string///////////////////
//    val converter = new IndexToString()
//      .setInputCol("categoryIndex")
//      .setOutputCol("originalCategory")
//
//    val converted = converter.transform(test)
//
//    println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
//      s"column '${converter.getOutputCol}' using labels in metadata")
//    converted.select("id", "categoryIndex", "originalCategory").show()
//
//    ///////////////////////////////////


    //////////////Classification metrics//////////////////////////////////////////////////
    // Compute raw scores on the test set
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    // Instantiate metrics object
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    // Precision-Recall Curve
    val PRC = metrics.pr

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
    /////////////////////////////////////////////////////////////////////////////////////


//    // Save and load model
//    model.save(sc, "target/tmp/myNaiveBayesModel")
//    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
    // $example off$
    model
  }

  // Method to split vehicle column and extract detail of first Vehicle
  def split_columns(df: sql.DataFrame, column_name: String) :sql.DataFrame = {
    var dataframe = df.na.fill("V1:()", Array(column_name)) // Replacing NA with V1:()
    dataframe = dataframe.withColumn("Vehicle_One", split(col(column_name),"\\)").getItem(0)) // Extracting characters before first bracket
    dataframe = dataframe.withColumn("Vehicle_Config",regexp_replace(col("Vehicle_One"),"V1:\\(","")) // Replacing V1: and comma(,) with blank
//    dataframe.show()
    dataframe
  }
  
  // Method to split datetime column into year, month, date and hour of the day
  def split_date_time(df: sql.DataFrame, column_name: String) :sql.DataFrame = {
    var dataframe = df.withColumn("CRASH_MONTH", split(col(column_name),"[\\/]").getItem(0))  // Extracting month from column
      .withColumn("CRASH_DAY", split(col(column_name),"[\\/]").getItem(1))  // Extracting day from column
      .withColumn("CRASH_YEAR_TIME", split(col(column_name),"[\\/]").getItem(2)) // Extracting year and time from column
    dataframe = dataframe.withColumn("CRASH_YEAR", split(col("CRASH_YEAR_TIME"),"\\ ").getItem(0)) // Extracting year from year/time column
    dataframe = dataframe.withColumn("TIME", split(col("CRASH_YEAR_TIME"),"\\ ").getItem(1)).drop("CRASH_YEAR_TIME") // Extracting time component from year/time column
    dataframe = dataframe.withColumn("CRASH_HOUR", split(col("TIME"),"\\:").getItem(0)).drop("TIME") // Extracting hour from column
//    dataframe.show()
    dataframe
  }

  // Method to split driver column and extract details of first driver
  def split_columns_driver(df: sql.DataFrame, column_name: String) :sql.DataFrame = {
    var dataframe = df.na.fill("D1:()", Array(column_name)) // Replacing NA with D1:()
    dataframe = dataframe.withColumn("Driver_One", split(col(column_name),"[\\)]*[\\/]").getItem(0)) // Extracting details of first driver
    // Cleaning the details of first driver
    dataframe = dataframe.withColumn("Driver_Control",regexp_replace(col("Driver_One"),"[D1:]*[D2:]*[D3:]*[D4:]","")).drop("Driver_One")
    dataframe = dataframe.withColumn("First",regexp_replace(col("Driver_Control"),"[\\(]*[\\)]","")).drop("Driver_Control")
    dataframe = dataframe.withColumn("First_" + column_name,regexp_replace(col("First"),"[\\(]","")).drop("First")
    //dataframe.coalesce(1).write.option("header", "true").csv("src/splitDriver.csv")
//    dataframe.show()
    dataframe
  }


}


