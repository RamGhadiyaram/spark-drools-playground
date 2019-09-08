package com.examples.reports

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object CloudReportDriver extends App with Logging{
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder.
    master("local[*]")
    .appName(this.getClass.getName)
    .getOrCreate()
  JobProcessor.trigger(parameters = Map.empty[String, String], spark: SparkSession)


//  import spark.implicits._
//
//  val df1 = Seq(
//    ("2019-01-01 00:00:00", "7056589658"),
//    ("2019-02-02 00:00:00", "7778965896")
//  ).toDF("DATE_TIME", "PHONE_NUMBER")
//
//  df1.show()
//
//  val df2 = Seq(
//    ("2019-01-01 01:00:00", "194.67.45.126"),
//    ("2019-02-02 00:00:00", "102.85.62.100"),
//    ("2019-03-03 03:00:00", "102.85.62.100")
//  ).toDF("DATE_TIME", "IP")
//
//

  // #Read Parameters from Job_parameters and spark-submit input parameters

  // # convert them in to map

  // # Create a reader class which has read def in in and accepts map and returns map

  // create a processor class which will do transformation logic for the input dataframe to create
  // a derived dataframe.

  // create Writer class which will write in to reports like pdf excel  html csv or
  // write in to any other kafka topic etc ....


}
