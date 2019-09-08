package com.examples.reports

import com.examples.reports.sales.{SalesReportProcessor, SalesReportReader, SalesReportWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object JobProcessor extends Logging {

  def trigger(parameters: Map[String, String], spark: SparkSession) {
    logInfo("in trigger of Job Processor " + this.getClass.getSimpleName)
var batchnumber = 0
    while (true) {
      batchnumber = batchnumber +1
      logInfo("executing trigger of Job Processor batch " + batchnumber + "  If you dont want to execute then define batch counter to come out of this loop " )
      process(parameters: Map[String, String], spark: SparkSession)
      Thread.sleep(10000)
    }
  }

  // read job parameters from a table or property file

  def process(parameters: Map[String, String], spark: SparkSession):Unit = {
    logInfo("in process of Job Processor " + this.getClass.getSimpleName)

    // instantiate correspodning reader
    SalesReportReader.setup(parameters, spark) // pre operations like any presql
    val seqr: Seq[DataFrame] = SalesReportReader.read(parameters, spark)
    SalesReportReader.close(parameters, spark) // post operations like any post sqls


    // instantiate correspodning reader
    SalesReportProcessor.setup(parameters, spark) // pre operations like any presql
    val seqp: Seq[DataFrame] = SalesReportProcessor.process(parameters, spark, seqr)
    SalesReportProcessor.close(parameters, spark) // post operations like any post sqls


    // instantiate correspodning reader
    SalesReportWriter.setup(parameters, spark) // pre operations like any presql
    val jobSuccess = SalesReportWriter.write(parameters, spark, seqp)
    SalesReportWriter.close(parameters, spark) // post operations like any post sqls
    jobSuccess match {
      case  true => logInfo("Job executed successfully")
      case false | _ => logInfo("Job failed some where logically")
    }
    //call setup read close in sequence

    //from read capture input and pass in to process method of specific processor

    // from processors process def get the input and call setup write close
    // which will write in to report or kafka queue or s3 bucket etc...

  }
}
