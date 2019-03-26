package com.examples


import com.examples.DataFrameUtil._

import com.examples.SampleDataSet._
import com.google.gson.JsonParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, _}

import scala.util.{Random, Success, Try}


/**
  *
  * @author : Ram.Ghadiyaram
  *
  */

object DataSetGenerator {


  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)

  val intArray = (1 to 999)

  val longArray = (1L to 999L)


  val rangelist = intArray.mkString(":")

  val myRandomStringArray = Array("Long Live     " + rangelist

    , "Long Live Testing 1 " + rangelist
    , "Long Live Testing 2 " + rangelist, null)
  /** *
    * <pre>
    * </pre>
    *
    * @param args
    *
    */

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    require(args.length <= 4,
      s"""
         | USAGE : <positive percent>  <Num Rows to be generated> <fileformat[s]> <number of Files to be generated
         | Current values are : ${args.foreach(println)}
   """.stripMargin)
    val positiveRecordpercent = if (args.length > 0) args(0).toDouble else .75
    require(positiveRecordpercent <= 1,
      s"""
         |

    | If its 75% then 0.75 need to be passed remaining 25% i.e. 0.25 will be negetive cases.
     | Current value passed : $positiveRecordpercent

   """.stripMargin)


    val numberOfRowsToBeGenerated = if (args.length > 0) args(1).toInt else 10
    val format = if (args.length > 0) args(2).toString.toUpperCase else "ALL"
    val numberOfPartFilesToBeGenerated = if (args.length > 0) args(3).toInt else 1

    val sc = spark.sparkContext
logger.info("json is -->> \n\n" + testdatasets.toString)
    val o = new JsonParser().parse(testdatasets).getAsJsonObject

    val myjsonasString = o.getAsJsonArray("columns").toString
import spark.implicits._
    logger.info(myjsonasString)

    /** If we are using > =spark 2.2 **/

    val dataFrameFromJson = spark.read.json(Seq(myjsonasString).toDS())

    /** If we are using < spark 2.2 *  val rdd = sc.parallelize(Seq(b))  val df = spark.read.json(rdd) **/


    val dataFrameOfRequiredColumns = dataFrameFromJson.selectExpr("businessColumnName", "columnOrder", "type", "length")

    dataFrameOfRequiredColumns.show

    dataFrameOfRequiredColumns.printSchema()


    val keyedBy = dataFrameOfRequiredColumns.rdd.keyBy(_.getAs[Long]("columnOrder"))
    keyedBy.foreach(x => logger.info("using keyBy-->>" + x))
    val mySortedMapByColOrder = scala.collection.immutable.TreeMap(keyedBy.collectAsMap().toArray: _*)
    val list = scala.collection.mutable.ListBuffer.empty[StructField]
    for ((key, value) <- mySortedMapByColOrder) {
      list += StructField(value.get(0).toString,
        createStruct(value.get(2).toString), false)
    }
    logger.debug("list is " + list.toList)
    val schema = StructType(list.toList)

    logger.info("schema as treeString -----" + schema.treeString)

    //val replicatedDf1 = replicateDf(100, testDf)
    // logger.info("-----" + schema.treeString)
    // creating data from sample values
    val mytestdataList = scala.collection.mutable.ListBuffer.empty[Any]
    //val schema1 = list1.toList
    for ((key1, value1) <- mySortedMapByColOrder) {
      logger.info(value1)
      val value = if (value1.get(2) == null) "" else value1
      logger.info(value)
      logger.info("value1.getAs[Long](\"length\")" + value1.getAs[Long]("length"))
      val valueOfLength: Int = Try(value1.getAs[Long]("length")) match {
        case Success(s) => s.toString.toInt
        case _ => logger.info("error :value1.getAs[Long](\"length\") is null"); 0

      }
      mytestdataList += createTestData(value.toString, valueOfLength)
    }

    var row = Row.fromSeq(mytestdataList.toSeq)
    row = Row.fromSeq(mytestdataList.toSeq)
    logger.info("list is " + mytestdataList)
    val testdatadf: RDD[Row] = spark.sparkContext.parallelize(Seq(row))
    logger.info("printing each row ")
    testdatadf.foreach(logger.info)
    val testdf: DataFrame = spark.createDataFrame(testdatadf, schema)
    logger.info("row is " + row.getClass.getName + "-----> " + row)
    testdf.show
    testdf.printSchema()
    logger.info(":::::::Experiment to replicate data!!!")
    val dfReplicated = replicateOneRowManytimes(numberOfRowsToBeGenerated, testdf)
    //val df2 = arrayOfReplicated(1).withColumn("","");
    val ided = addColumnIndex(dfReplicated, "index")

    moveColumnToFirstPos(ided, "index")

    val (training, test1) = splitDF(dfReplicated, positiveRecordpercent, 1 - positiveRecordpercent)

    val mycols = test1.schema.fields

    var test = test1;

    mycols.foreach(x => {

      test =
        x.dataType match {
          case FloatType => test.withColumn(x.name, lit(Random.nextFloat()))
          case DoubleType => test.withColumn(x.name, lit(Random.nextDouble()))
          case IntegerType => test.withColumn(x.name, lit(intArray(Random.nextInt(intArray.size))))
          case StringType => test.withColumn(x.name, lit(myRandomStringArray(Random.nextInt(myRandomStringArray.size))))
          case LongType => test.withColumn(x.name, lit(Random.nextLong()))

          //case TimestampType => test //.withColumn(x.name, Timestamp.valueOf("2017-12-02 03:04:00"))
          case _ => test
        }

    })

    val finaldf = training.union(test)

    logger.info(finaldf.count())

    test.show(false)

    finaldf.printSchema()

    finaldf.show(false)


    val dfToWrite = finaldf.coalesce(numberOfPartFilesToBeGenerated).write.mode(SaveMode.Overwrite)

    val allFormats = Array("PARQUET", "AVRO", "CSV", "JSON", "XML")

    format match {

      case "ALL" =>
        logger.info(s"Writing all formats ${allFormats.mkString("\n")} ");
        allFormats.foreach(formatType => writeToFileFormats(formatType, dfToWrite));
      case _ =>
        logger.info(s"Writing $format format } ");
        writeToFileFormats(format, dfToWrite)
    } //end of main

  }

}