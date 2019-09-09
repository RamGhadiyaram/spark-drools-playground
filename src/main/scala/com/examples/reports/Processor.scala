package com.examples.reports


import org.apache.spark.sql.{DataFrame, SparkSession}

trait Processor extends  Serializable {

  def setup(a: Map[String, Object], sparksession: SparkSession): Map[String, Object]

  def process(a: Map[String, Object], sparkSession: SparkSession, seq:Seq[DataFrame]):Seq[DataFrame]

  def close(a: Map[String, Object], sparkSession: SparkSession)

}
