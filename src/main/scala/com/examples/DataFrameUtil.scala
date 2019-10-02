package com.examples


import java.sql.Timestamp
import java.text.DecimalFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

/**
  * DataFrameUtil - reusable functions across this project
  *
  * @author Ram Ghadiyaram
  */
object DataFrameUtil {
  val logger = Logger.getLogger(this.getClass.getName)
  Logger.getLogger("org").setLevel(Level.WARN)
  //  def main(args: Array[String]): Unit = {//    val conf = new SparkConf()//    conf.setAppName("Datasets Test")//    conf.setMaster("local[2]")//    val sc = new SparkContext(conf)//    logger.info(sc)//  }
  val decimalformat = new DecimalFormat("###.###")

  /**
    * replicateOneRowManytimes : Replicates same row many ines so that test data is created
    *
    * @param n  Int
    * @param df DataFrame
    * @return DataFrame
    */
  def replicateOneRowManytimes(n: Int, df: DataFrame): DataFrame = df.sqlContext.createDataFrame(
    df.sparkSession.sparkContext.parallelize(List.fill(n)(df.take(1)(0)).toSeq),
    df.schema)

  /**
    * createStruct.
    *
    * @param myObject
    * @return
    */
  def createStruct(myObject: String): DataType = {

    myObject match {
      case t if t.toUpperCase.contains("STRING") => StringType
      case t if t.toUpperCase.contains("LONG") => LongType
      case t if t.toUpperCase.contains("INT") => IntegerType
      case t if t.toUpperCase.contains("FLOAT") => FloatType
      case t if t.toUpperCase.contains("DOUBLE") => DoubleType
      case t if t.toUpperCase.contains("TIMESTAMP") => TimestampType
      case t if t.toUpperCase.contains("JSON") => StringType
      case t if t.toUpperCase.contains("SHORT") => ShortType
      case t if t.toUpperCase.contains("BYTE") => ByteType
      case t if t.toUpperCase.contains("BOOLEAN") => BooleanType
      case t if t.toUpperCase.contains("BINARY") => BinaryType
      case t if t.toUpperCase.contains("CALENDAR") => CalendarIntervalType
      case t if t.toUpperCase.contains("DECIMAL") => DecimalType.SYSTEM_DEFAULT
      case _ => StringType
    }
  }

  /**
    * createTestData.
    *
    * @param myObject
    * @return
    */
  def createTestData(myObject: String, length: Int): Any = {
    val r = scala.util.Random
    logger.info("myObject" + myObject)
    myObject.toUpperCase() match {
      case t if t.contains("STRING") => {
        randomString(length)
      }
      case t if t.toUpperCase.contains("LONG") => r.nextLong()
      case t if t.toUpperCase.contains("INT") => {
        val full = r.nextInt(Integer.MAX_VALUE).toString
        val len = (if (full.length > length) length else full.length).toInt
        logger.info(s"full$full $length")
        val int = full.substring(0, len).toInt;
        logger.info("next int value -->>" + int)
        int
      }
      case t if t.toUpperCase.contains("FLOAT") => r.nextFloat()
      case t if t.toUpperCase.contains("DOUBLE") => r.nextDouble()
      case t if t.contains("TIMESTAMP") => Timestamp.valueOf("2017-12-02 03:04:00")
      case t if t.contains("JSON") => "{}"
      case t if t.toUpperCase.contains("SHORT") => r.nextInt(length).toShort
      case t if t.toUpperCase.contains("BYTE") => r.nextLong().toByte
      case t if t.toUpperCase.contains("BOOLEAN") => r.nextBoolean()
      case t if t.toUpperCase.contains("BINARY") => 4.toBinaryString
      case t if t.toUpperCase.contains("CALENDAR") => null
    }
  }

  /**
    * randomString - generates new RandomString
    *
    * @param length
    * @return
    */
  def randomString(length: Int) = {
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(r.nextPrintableChar)
    }
    sb.toString
  }

  /**
    * Add Column Index to dataframe
    *
    * @param spark      SparkSession
    * @param df         DataFrame
    * @param columnName String
    * @return
    */
  def addColumnIndex(df: DataFrame, columnName: String): sql.DataFrame = {
    logger.info("original df")
    df.show
    val finaldf = df.sparkSession.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField(columnName, LongType, false)))
    logger.info("after transformation ")
    finaldf.show(false)
    finaldf
  }

  /**
    * randomString - generates new RandomString
    *
    * @param length
    * @return
    */
  def randomInt(length: Int) = {
    logger.info("length is " + length)
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val value = r.nextInt(length)
      if (value > 0)
        sb.append(value)
    }
    logger.info(sb)
    sb.toString.substring(0, length).toInt
  }

  /**
    * moveColumnToFirstPos : This function can be used after withColumn applied to a data frame
    * which will add column to the right most position
    * if you want to move column to first position in the dataframe selection and drop duplicate as well
    *
    * @param dataframecolumnOrderChanged
    */
  def moveColumnToFirstPos(dataframecolumnOrderChanged: DataFrame, colname: String) = {
    val xs = Array(colname) ++ dataframecolumnOrderChanged.columns.dropRight(1)
    val fistposColDF = dataframecolumnOrderChanged.selectExpr(xs: _*)
    fistposColDF.show(false)
    fistposColDF
  }

  /**
    * splitDF - splits in to 2 dataframe one is training and another one is test data
    *
    * @param dfReplicated
    * @param percent1
    * @param percent2
    */
  def splitDF(dfReplicated: DataFrame, percent1: Double, percent2: Double): Tuple2[DataFrame, DataFrame] = {
    dfReplicated.randomSplit(Array[Double](percent1, percent2: Double)) match {
      case Array(training, test) => (training, test)
    }
  }

  /**
    * writeToFileFormats.
    *
    * @param format
    * @param dfToWrite
    */
  def writeToFileFormats(format: String, dfToWrite: DataFrameWriter[Row]) = {
    format match {
      case "AVRO" => dfToWrite.format("com.databricks.spark.avro").save(s"src/test/resources/testdatasetgen/test.${format.toString.toLowerCase}")
      case "CSV" => dfToWrite.csv(s"src/test/resources/testdatasetgen/test.${format.toString.toLowerCase}")
      case "JSON" => dfToWrite.json(s"src/test/resources/testdatasetgen/test.${format.toString.toLowerCase}")
      case "XML" => dfToWrite.format("com.databricks.spark.xml").save(s"src/test/resources/testdatasetgen/test.${format.toString.toLowerCase}")
      case _ | "PARQUET" => dfToWrite.parquet(s"src/test/resources/testdatasetgen/test.${format.toString.toLowerCase}")
    }
  }

  /** Lazily instantiated singleton instance of SparkSession */

  object SparkSessionSingleton extends Logging {
    val logger = Logger.getLogger(this.getClass.getName)
    @transient private var instance: SparkSession = _

    def getInstance(app: Option[String]): SparkSession = {
      logDebug(" instance " + instance)
      if (instance == null) {
        instance = SparkSession
          .builder
          .config("spark.master", "local") //.config("spark.eventLog.enabled", "true")
          .appName("AppDroolsPlayGroundWithSpark")
          .getOrCreate()
      }
      instance
    }
  }

}