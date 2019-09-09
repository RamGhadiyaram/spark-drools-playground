package com.examples.reports

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe

/**
  * responsible for processing the jobs defined.
  */
object JobProcessor extends Logging {
  /**
    *
    * @param parameters
    * @param spark
    */
  def trigger(parameters: Map[String, Object], spark: SparkSession) {
    logInfo("in trigger of Job Processor " + this.getClass.getSimpleName)
    var batchnumber = 0
    while (true) {
      batchnumber = batchnumber + 1
      logInfo("executing trigger of Job Processor batch " + batchnumber + "  If you dont want to execute then define batch counter to come out of this loop ")
      process(parameters: Map[String, Object], spark: SparkSession)
      Thread.sleep(10000) // this number can be read from job parameters
    }
  }

  /**
    * process.
    *
    * @param parameters
    * @param spark
    */
  def process(parameters: Map[String, Object], spark: SparkSession): Unit = {
    logInfo("in process of Job Processor " + this.getClass.getSimpleName)
    // val c: Class[_] = Class.forName("com.examples.reports.sales.SalesReportReader").asInstanceOf[com.examples.reports.sales.SalesReportReader]
    //newify[com.examples.reports.sales.SalesReportReader]("com.examples.reports.sales.SalesReportReader")
    //   println("" + c.newInstance().getClass.getName)
    //    println(c.getClass.getName())
    // instantiate correspodning reader
    // these string will be read from database job_parameter property
    val frameworkClzReader = "com.examples.reports.sales.SalesReportReader"
    val frameworkClzProcessor = "com.examples.reports.sales.SalesReportProcessor"
    val frameworkClzWriter = "com.examples.reports.sales.SalesReportWriter"

    val reader: Reader = getFrameworkClasses(frameworkClzReader).instance.asInstanceOf[Reader]

    val job_params = reader.setup(parameters, spark) // pre operations like any presql
    val seqr: Seq[DataFrame] = reader.read(job_params, spark)
    reader.close(job_params, spark) // post operations like any post sqls


    val processor: Processor = getFrameworkClasses(frameworkClzProcessor).instance.asInstanceOf[Processor]
    // instantiate correspodning reader
   val job_paramsProcessor =  processor.setup(job_params, spark) // pre operations like any presql
    val seqp: Seq[DataFrame] = processor.process(job_paramsProcessor, spark, seqr)
    processor.close(job_paramsProcessor, spark) // post operations like any post sqls

    val writer: Writer = getFrameworkClasses(frameworkClzWriter).instance.asInstanceOf[Writer]
    // instantiate correspodning reader
  val job_paramsWriter =  writer.setup(job_paramsProcessor, spark) // pre operations like any presql
    val jobSuccess = writer.write(job_paramsWriter, spark, seqp)
    writer.close(job_paramsWriter, spark) // post operations like any post sqls
    jobSuccess match {
      case true => logInfo("Job executed successfully")
      case false | _ => logInfo("Job failed some where logically")
    }
    //call setup read close in sequence

    //from read capture input and pass in to process method of specific processor

    // from processors process def get the input and call setup write close
    // which will write in to report or kafka queue or s3 bucket etc...
  }

  private def getFrameworkClasses(frameworkClzReader: String): universe.ModuleMirror = {
    import scala.reflect.runtime.universe

    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(frameworkClzReader)
    val obj: universe.ModuleMirror = runtimeMirror.reflectModule(module)
    obj
  }

  // read job parameters from a table or property file
  def newify[T](className: String): T = Class.forName(className).newInstance.asInstanceOf[T]
}
