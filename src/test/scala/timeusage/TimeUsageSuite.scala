package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.junit.{Assert, Test}
import org.junit.Assert.assertEquals
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.Random

class TimeUsageSuite {

  import TimeUsage._

  @Test def `does row work correctly`: Unit = {
    val inputLine = List("Mark", "1", "2.0", "3.00")
    val testRow = row(inputLine)
    val expectedRow = Row("Mark", 1.toDouble, 2.toDouble, 3.toDouble)
    assertEquals(expectedRow, testRow)
  }

  @Test def `categorise works correctly`: Unit = {
//    val PrimCat = List("t01", "t03", "t11", "t1801", "t1803")
//    val WorkCat = List("t05", "t1805")
//    val OtherCat = List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")
//def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column])
    val testColumnNames: List[String] = List("t010101", "t0532423", "t02", "t083443", "t1803")
    val expectedGroups: (List[Column], List[Column] ,List[Column])= (List(col("t010101"), col("t1803")), List(col("t0532423")), List(col("t02"), col("t083443")))
    assertEquals(expectedGroups, classifiedColumns(testColumnNames))
  }

}
