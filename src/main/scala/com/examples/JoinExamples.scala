package com.examples

import com.droolsplay.util.SparkSessionSingleton
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
  Logger.getLogger("akka").setLevel(Level.OFF)
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
          :: Person("Ravi", 34, 5)
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
      df_asProfile
      , col("dfperson.personid") === col("dfprofile.personid")
      , "inner")
    joined_df.select(
      col("dfperson.name")
      , col("dfperson.age")
      , col("dfprofile.name")
      , col("dfprofile.profileDescription"))
      .show

    /// example using sql statement after registering createOrReplaceTempView

    df_asPerson.createOrReplaceTempView("dfPerson");
    df_asProfile.createOrReplaceTempView("dfprofile")
    // this is example of plain sql
    val dfJoin = spark.sqlContext.sql(
      """SELECT dfperson.name, dfperson.age, dfprofile.profileDescription
                          FROM  dfperson JOIN  dfprofile
                          ON dfperson.personid == dfprofile.personid""")
    dfJoin.show(false)


    val seqEmpData: Seq[Employee] = Seq(Employee("Anto", 21, "Software Engineer", 2000, 56798)
      , Employee("Ram Ghadiyaram", 21, "Architect", 2000, 93798)
      , Employee("Sarath Mohan", 30, "Software Engineer", 2000, 28798)
      , Employee("Ravichandra K", 62, "CEO", 22000, 45798)
      , Employee("Nick", 74, "VP", 12000, 98798)
      , Employee("Steven", 45, "Development Lead", 8000, 98798)
      , Employee("Ravi", 21, "Sr.Software Engineer", 4000, 98798)
      , Employee("Ilker", 21, "Sr.Software Engineer", 4000, 98798)
      , Employee("Vasudha Nanduri", 21, "Sr.Software Engineer", 4000, 98798))
    spark.sparkContext.parallelize(seqEmpData).toDS().show(false)


    val caseClassDf = spark.sparkContext.parallelize(Seq(TestPerson("Andy", 32L)))
    caseClassDf.toDF.show(false)

    logInfo("primitiveSequenceValueDemo")
    val primitiveDS: Unit = Seq(1, 2, 3).toDS().show(false)
    // val Employee_DataFrame = spark.sqlContext.createDataFrame( EmployeesData,Employee.getClass)
    // Employee_DataFrame.show(false)
  }

  // models here

  case class Person(name: String, age: Int, personid: Int)

  case class Profile(name: String, personId: Int, profileDescription: String)

  case class TestPerson(name: String, age: Long)

  case class Employee(Name: String, Age: Int, Designation: String, Salary: Int, ZipCode: Int)

}