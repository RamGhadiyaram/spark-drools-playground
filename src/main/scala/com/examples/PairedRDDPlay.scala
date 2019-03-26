package com.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** *
  * collectAsMap is only applicable to pairedrdd if you want to do a map then you can do a rdd key by and proceed
  *
  * @author : Ram Ghadiyaram
  */
object PairedRDDPlay extends Logging {
  Logger.getLogger("org").setLevel(Level.OFF)
  // Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val appName = if (args.length > 0) args(0) else this.getClass.getName
    val spark: SparkSession = SparkSession.builder
      .config("spark.master", "local") //.config("spark.eventLog.enabled", "true")
      .appName(appName)
      .getOrCreate()

    pairRDDCollectAsMapExample(spark)
    multipleGroupByExample(spark)
  }

  private def multipleGroupByExample(spark: SparkSession) = {
    import spark.implicits._

    val client: DataFrame = Seq((1, "A", 10), (2, "A", 5), (3, "B", 56)).toDF("ID", "Categ", "Amnt")
    client.groupBy("Categ").agg(sum("Amnt"), count("ID")).show()
  }

  private def pairRDDCollectAsMapExample(spark: SparkSession) = {
    import spark.implicits._
    val pairs = spark.sparkContext.parallelize(Array((1, 1, 3), (1, 2, 3), (1, 3, 3), (1, 1, 3), (2, 1, 3))).toDF("mycol1", "mycol2", "mycol3")
    pairs.show()

    val keyedBy = pairs.rdd.keyBy(_.getAs[Int]("mycol1"))
    keyedBy.foreach(x => println("using keyBy-->>" + x))
    val myMap = keyedBy.collectAsMap()
    println(myMap.toString())
    assert(myMap.size == 2)
  }
}