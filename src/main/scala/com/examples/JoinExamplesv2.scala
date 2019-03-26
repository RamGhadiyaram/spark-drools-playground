package com.examples
import com.examples.DataFrameUtil.SparkSessionSingleton
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Join Example and some basics demonstration using sample data.
  *
  * @author : Ram Ghadiyaram
  */
object JoinExamplesv2 extends Logging {
  // switch off  un necessary logs
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  //  val spark: SparkSession = SparkSession.builder.config("spark.master", "local").getOrCreate;
  val spark: SparkSession = SparkSessionSingleton.getInstance(Option(this.getClass.getName))

  /**
    * main
    *
    * @param args Array[String]
    */
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    /**
      * create 2 dataframes here using case classes one is Person df1 and another one is profile df2
      */
    val df1 = spark.sqlContext.createDataFrame(
      spark.sparkContext.parallelize(
        Person("Sarath", 33, 2)
          :: Person("Vasudha Nanduri", 30, 2)
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

    val dfPerson1 = df1.as("dfperson1")
    val df_asProfile = df2.as("dfprofile")
    /** *
      * Example displays how to join them in the dataframe level
      * next example demonstrates using sql with createOrReplaceTempView
      */
    val joined_df = dfPerson1.join(
      df_asProfile
      , col("dfperson1.personid") === col("dfprofile.personid")
      , "inner")
    joined_df.select(
      col("dfperson1.name")
      , col("dfperson1.age")
      , col("dfprofile.name")
      , col("dfprofile.profileDescription"))
      .show

    /// example using sql statement after registering createOrReplaceTempView

    dfPerson1.createOrReplaceTempView("dfPerson1");
    df_asProfile.createOrReplaceTempView("dfprofile")
    // this is example of plain sql
    val dfJoin = spark.sqlContext.sql(
      """SELECT dfperson1.name, dfperson1.age, dfprofile.profileDescription
                          FROM  dfperson1 JOIN  dfprofile
                          ON dfperson1.personid == dfprofile.personid""")
    logInfo("Example using sql statement after registering createOrReplaceTempView ")
    dfJoin.show(false)

  }

  // models here

  case class Person(name: String, age: Int, personid: Int)

  case class Profile(name: String, personId: Int, profileDescription: String)

}