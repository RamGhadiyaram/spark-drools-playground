package com.examples.reports.sales

import com.examples.reports.Writer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

case object SalesReportWriter extends Writer with Logging {

  def setup(a: Map[String, String], sparkSession: SparkSession): Unit = { // pre
    logInfo("in setup ")

  }

  def write(a: Map[String, String], sparkSession: SparkSession, seq: Seq[DataFrame]): Boolean = { // write to report and do any thing you want
    logInfo("In write ... here based on job parameters we can customize the logic like write in to KAFKA or S3 SNOWFLAKE HDFS EXCEL PDF CSV TDE etc....")
    seq.head.coalesce(1).write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'My Sheet1'!B3:C35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode("append")
      .save(".\\src\\main\\resources\\testexcel.xlsx")

    seq(1).coalesce(1).write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'My Sheet2'!B3:C35")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode("append")
      .save(".\\src\\main\\resources\\testexcel.xlsx")
    true
  }

  def close(a: Map[String, String], sparkSession: SparkSession): Unit = { // post
    // anything is there to close or post operations...
  }

}
