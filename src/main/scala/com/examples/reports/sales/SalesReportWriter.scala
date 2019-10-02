package com.examples.reports.sales

import com.examples.reports.Writer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Level
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
case object SalesReportWriter extends Writer with Logging {

  def setup(a: Map[String, Object], sparksession: SparkSession): Map[String, Object] = {
    logInfo("in setup ")
    a
  }

  def write(a: Map[String, Object], sparkSession: SparkSession, seq: Seq[DataFrame]): Boolean = { // write to report and do any thing you want
    logInfo("In write ... here based on job parameters we can customize the logic like write in to KAFKA or S3 SNOWFLAKE HDFS EXCEL PDF CSV TDE etc....")

    seq.head.show
    seq(0).coalesce(1).write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'RollupCityYear'!A1:F35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .option("useHeader", "true")
      .option("addColorColumns", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .mode("append")
      .save("./src/main/salesReport/SalesReport.xlsx")
    logInfo("Using .groupBy(\"city\", \"year\")")
    seq(1).coalesce(1).write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'GroupByCityYearSheet'!A1:F35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .option("addColorColumns", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .mode("append")
      .save("./src/main/salesReport/SalesReport.xlsx")
    seq(2).coalesce(1).write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'GroupByCitySheet'!A1:F35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .option("treatEmptyValuesAsNulls", "false")
      .mode("append")
      .save("./src/main/salesReport/SalesReport.xlsx")

    seq(3).coalesce(1).write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'groupBySheet'!A1:F35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .option("treatEmptyValuesAsNulls", "false")
      .mode("append")
      .save("./src/main/salesReport/SalesReport.xlsx")


    seq(4).coalesce(1).write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'unionSheet'!A1:F35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .option("treatEmptyValuesAsNulls", "false")
      .mode("append")
      .save("./src/main/salesReport/SalesReport.xlsx")
    true
  }

  def close(a: Map[String, Object], sparkSession: SparkSession): Unit = { // post
    // anything is there to close or post operations...
  }

}
