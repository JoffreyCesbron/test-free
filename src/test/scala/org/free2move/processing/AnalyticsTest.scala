package org.free2move.processing

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.free2move.processing.Analytics.getStatsForAPeriod
import org.scalatest.FunSuite
import java.sql.Date

class AnalyticsTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  test("Should dataframe be filtered regarding a period") {

    //Given
    val someSchema = List(
      StructField("id", StringType, true),
      StructField("order_purchase_day", DateType, true)
    )

    val someDataInput = Seq(
      Row("1", Date.valueOf("2020-01-01")),
      Row("2", Date.valueOf("2020-01-02")),
      Row("3", Date.valueOf("2020-01-03")),
      Row("4", Date.valueOf("2020-01-04"))
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(someDataInput),
      StructType(someSchema)
    )

    val someDataExpected = Seq(
      Row("2", Date.valueOf("2020-01-02")),
      Row("3", Date.valueOf("2020-01-03")),
    )

    val dfExpected = spark.createDataFrame(
      spark.sparkContext.parallelize(someDataExpected),
      StructType(someSchema)
    )

    //When
    val dfActual = getStatsForAPeriod(df, "2020-01-02", "2020-01-03")

    //Then
    assertDataFrameDataEquals(dfActual, dfExpected)

  }

}
