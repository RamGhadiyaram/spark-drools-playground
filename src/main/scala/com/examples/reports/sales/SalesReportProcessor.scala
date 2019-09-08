package com.examples.reports.sales

import com.examples.reports.Processor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

case object SalesReportProcessor  extends Processor with Logging{

  def setup(a: Map[String, String], sparkSession: SparkSession): Unit = {
    logInfo("in setup " + this.getClass.getSimpleName)

  }

  def process(a: Map[String, String], sparkSession: SparkSession,seq:Seq[DataFrame]): Seq[DataFrame] = {
    logInfo("in process " + this.getClass.getSimpleName)
seq
  }

  def close(a: Map[String, String], sparkSession: SparkSession): Unit = {

  }

}
