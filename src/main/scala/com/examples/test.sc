import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val x = Map (
  0-> Row("APPN_DIM_ID",0,"Int")
  ,1-> Row("SRC_PRIM_KEY",1,"Long")
  ,3-> Row( "LEGL_AFIL_CD",3,"Long")
  ,2-> Row( "SOR_ID",2,"Long")
  ,5-> Row( "BUS_DT",5,"Long")
  ,4-> Row( "COAF_PUBN_ID",4,"Long")
)

val a = scala.collection.immutable.TreeMap(x.toArray:_*)

val list = scala.collection.mutable.ListBuffer.empty[StructField]

/**
  *
  * @param myObject
  * @return
  */
def createStruct(myObject: String): DataType = {

  myObject match {
    case t if t.contains( "String") => StringType
    case t if t.contains( "Long") => LongType
    case t if t.contains( "Int") => IntegerType
    case t if t.contains( "Float") => FloatType
    case t if t.contains( "Double") => DoubleType
    case t if t.contains( "Timestamp") => TimestampType
  }
}


for ((key, value) <- a) {
  list += StructField(value.get(0).toString, createStruct(value.get(2).toString), false)
}
println("list is " + list)
val schema = StructType(list.toList)
println("-----" + schema.treeString)