import javax.sql.rowset.RowSetFactory
import org.apache.spark.sql.Row

val insertString = "_a,b,c"
val sqlTableName = "table"

val tableHeader = "tableHeader"
//println(s"INSERT INTO $sqlTableName  VALUES  $tableHeader ${insertString} ON DUPLICATE KEY UPDATE yourprimarykeycolumn='${record.get("key").get}'")
val record: Row = org.apache.spark.sql.RowFactory.create("key","val")

val sql =   s"""
               | INSERT INTO $sqlTableName  VALUES
               | $tableHeader
               | ${insertString}
               | ON DUPLICATE KEY UPDATE
               | yourprimarykeycolumn='${record.getAs[String]("key")}'
""".stripMargin

println(sql)

// Basic map example in scala
val x = spark.sparkContext.parallelize(List("spark", "map", "example",  "sample", "example"), 3)
val y = x.map(x => (x, 1))
y.collect
// res0: Array[(String, Int)] =
//    Array((spark,1), (map,1), (example,1), (sample,1), (example,1))

// rdd y can be re writen with shorter syntax in scala as
val y = x.map((_, 1))
y.collect
// res1: Array[(String, Int)] =
//    Array((spark,1), (map,1), (example,1), (sample,1), (example,1))

// Another example of making tuple with string and it's length
val y = x.map(x => (x, x.length))
y.collect
// res3: Array[(String, Int)] =
//    Array((spark,5), (map,3), (example,7), (sample,6), (example,7))