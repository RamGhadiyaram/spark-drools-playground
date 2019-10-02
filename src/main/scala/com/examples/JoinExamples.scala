package com.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Join Example and some basics demonstration using sample data.
  *
  * @author : Ram Ghadiyaram
  */
object JoinExamples extends Logging {
  // switch off  un necessary logs
  Logger.getLogger("org").setLevel(Level.OFF)
   val spark: SparkSession = SparkSession.builder.config("spark.master", "local").getOrCreate;
  case class Person(name: String, age: Int, personid: Int)

  case class Profile(name: String, personId: Int, profileDescription: String)

  /**
    * main
    *
    * @param args Array[String]
    */
  def main(args: Array[String]): Unit = {
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
    import spark.implicits._

    spark.sparkContext.getConf.getAllWithPrefix("spark.sql").foreach(x => logInfo(x.toString()))
    /**
      * create 2 dataframes here using case classes one is Person df1 and another one is profile df2
      */
    val df1 = spark.sqlContext.createDataFrame(
      spark.sparkContext.parallelize(
        Person("Sarath", 33, 2)
          :: Person("KangarooWest", 30, 2)
          :: Person("Ravikumar Ramasamy", 34, 5)
          :: Person("Ram Ghadiyaram", 42, 9)
          :: Person("Ravi chandra Kancharla", 43, 9)
          :: Nil))


    val df2 = spark.sqlContext.createDataFrame(
      Profile("Spark", 2, "SparkSQLMaster")
        :: Profile("Spark", 5, "SparkGuru")
        :: Profile("Spark", 9, "DevHunter")
        :: Nil
    )

    // you can do alias to refer column name with aliases to  increase readablity

    val df_asPerson = df1.as("dfperson")
    val df_asProfile = df2.as("dfprofile")
    /** *
      * Example displays how to join them in the dataframe level
      * next example demonstrates using sql with createOrReplaceTempView
      */
    val joined_df = df_asPerson.join(
      broadcast(df_asProfile)
      , col("dfperson.personid") === col("dfprofile.personid")
      , "fullouter")
    val joined = joined_df.select(
      col("dfperson.name")
      , col("dfperson.age")
      , col("dfprofile.name")
      , col("dfprofile.profileDescription"))
    joined.explain(false)
    joined.show

  }

}