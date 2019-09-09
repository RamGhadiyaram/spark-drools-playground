package com.examples.reports

import org.apache.spark.sql.{DataFrame, SparkSession}

trait  Reader  extends  Serializable {

  def setup(a: Map[String, Object], sparksession: SparkSession): Map[String, Object]

  def read(a:Map[String,Object], spark:SparkSession) : Seq[DataFrame]

  def close(a:Map[String,Object] , sparkSession: SparkSession)

}
