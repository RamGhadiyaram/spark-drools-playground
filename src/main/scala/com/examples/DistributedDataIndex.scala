package com.examples


import com.examples.DataFrameUtil.SparkSessionSingleton
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

/** *
  * Appending an Index Column to Distributed DataFrame based on another Column with Non-unique Entries
  *
  * @author : Ram Ghadiyaram
  */
object DistributedDataIndex extends Logging {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSessionSingleton.getInstance(Option(this.getClass.getName))
  var indexedItems: DataFrame = spark.sqlContext.emptyDataFrame
  var indexedUniqueCategories: DataFrame = spark.sqlContext.emptyDataFrame
  var indexedUniqueCategoriesMap: mutable.Map[String, Long] = mutable.Map[String, Long]()

  /**
    * main
    *
    * @param args Array[String]
    */

  def main(args: Array[String]): Unit = {
    /**
      * Prepare data
      * 1) create a listbuffer
      * 2) assign random number
      * 3) create a dataframe - sample data
      */

    val categoryListBuffer: ListBuffer[String] = new ListBuffer
    for (i <- 1 to 5) {
      categoryListBuffer += java.util.UUID.randomUUID.toString
    }
    val randomGenerator = scala.util.Random
    val itemListBuffer: ListBuffer[Item] = new ListBuffer
    for (i <- 1 to 25) {
      itemListBuffer += Item(categoryListBuffer(randomGenerator.nextInt(5)), f"Item$i", randomGenerator.nextDouble() * 100)
    }

    logInfo("partitions size of empty dataframe  " + indexedItems.rdd.partitions.length)
    logInfo("original dataframe")
    val items = spark.sqlContext.createDataFrame(itemListBuffer.toList)
    items.show(false)

    /** *
      * Running take requires moving data into the application's driver process, and doing so with
      * * a very large `n` can crash the driver process with OutOfMemoryError.
      */
    items.take(5).foreach(println)

    //("BroadCast local variable : ")
    timeElapsed {
      /** *
        * broadcast way
        */
      logInfo(
        """ Method 0 :  Using broad cast but you have size limit default local variable to be broadcased in 4mb "
          |broadcast way
          | 3 steps
          | 1) Collect the dataframe
          | 2) Assign index
          | 3) Broadcast local variable sc.broadCast
          |
          | Drawbacks:
          | collect is bad for large Dataset it will cause OOM if data is large
          | broadcast (torrent has limited size)
          | so rule out this....as this is for test programs, not production grade code.
          |
    """.stripMargin)
      //is a potential bottleneck at large data size
      val uniqueCategoriesList = items.select("category").distinct.collect //collect is bad for large dataset

      for (i <- 0 to uniqueCategoriesList.length - 1) {
        indexedUniqueCategoriesMap += (uniqueCategoriesList(i).get(0).toString -> i)
      }
      // using broad cast but you have size limit default local variable to be broadcased in 4mb
      spark.sparkContext.broadcast(indexedUniqueCategoriesMap)

      /** *
        * define udf  lookupIndex here...
        */

      val lookupIndex: String => Long = (categoryName: String) => {
        indexedUniqueCategoriesMap.getOrElse(categoryName, -1)
      }
      val lookupIndexFunction = udf(lookupIndex)
      indexedItems = items.withColumn("index", lookupIndexFunction(items("category")))
      indexedItems.toDF().show(false)
      indexedItems.rdd.partitions.length
    }
    //("Method 1 - Using JOIN of two DataFrames : ")
    timeElapsed {
      val uniqueCategories = items.select("category").distinct.withColumnRenamed("category", "uniquecategory")
      logInfo(" original data uniqueCategories")
      uniqueCategories.show(false)
      logInfo(" adding indexes now ")
      indexedUniqueCategories = spark.createDataFrame(uniqueCategories.rdd.zipWithIndex().map(
        r => Row.fromSeq(Seq(r._2) ++ r._1.toSeq)), StructType(
        Array(StructField("index", LongType, false)) ++ uniqueCategories.schema.fields))
      indexedUniqueCategories.show(false)

      /** *
        * using join
        * Method 1 - Using JOIN of two DataFrames
        */
      logInfo("Method 1 - Using JOIN of two DataFrames ")
      indexedItems = items.join(indexedUniqueCategories, items("category")
        === indexedUniqueCategories("uniquecategory")).drop("uniquecategory")
      indexedItems.show(false)
      indexedItems.rdd.partitions.size
    }

    //("Method 2 - Using DataFrame as lookup table in UDF")
    timeElapsed {
      /**
        * Method 2 - Using DataFrame as lookup table in UDF
        * create index using a scala function
        */
      logInfo(" Method 2 - Using DataFrame as lookup table in UDF ")
      /**
        * getIndex
        */
      val getIndex: String => Long = (categoryName: String) => {
        indexedUniqueCategories.filter(indexedUniqueCategories("uniquecategory")
          === categoryName).select("index").first().get(0).asInstanceOf[Long]
      }
      /**
        * getIndexFunction
        */
      val getIndexFunction = udf(getIndex)

      val reorderedIndexedItems = items.withColumn("index", getIndexFunction(items("category"))).select("index", "category", "name", "price")
      reorderedIndexedItems.show(false)
      reorderedIndexedItems.rdd.partitions.size
    }


  }

  /**
    * Executes some code block and prints to stdout the
    * time taken to execute   the block
    * for interactive testing and debugging.
    */
  def timeElapsed[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()

    logInfo(s"Time taken: ${(end - start) / 1000 / 1000} ms")

    ret
  }

  // create a case class with category name and price.
  case class Item(category: String, name: String, price: Double)

}