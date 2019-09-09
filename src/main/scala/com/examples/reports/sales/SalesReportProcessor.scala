package com.examples.reports.sales

import com.examples.reports.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

case object SalesReportProcessor extends Processor with Logging {

  def setup(a: Map[String, Object], sparksession: SparkSession): Map[String, Object] = {
    logInfo("in setup ")
    a
  }

  def process(a: Map[String, Object], sparkSession: SparkSession, seq: Seq[DataFrame]): Seq[DataFrame] = {
    logInfo("in process " + this.getClass.getSimpleName)
    seq
  }

  def close(a: Map[String, Object], sparkSession: SparkSession): Unit = {
    logInfo("in close ")

  }

}
