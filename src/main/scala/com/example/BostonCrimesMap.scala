package com.example

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable

object Boston {
  // Median function
  def median(inList: List[Long]): Long = {
    val sortList = inList.sorted
    val cnt: Int = inList.size
    // for even values, take the average of the two central elements
    if (cnt % 2 == 0) {
      val first: Int = cnt/2 - 1
      val second: Int = first + 1
      (sortList(first) + sortList(second))/2
    } else sortList(cnt/2)
  }
  // UDF
  def medianUDF: UserDefinedFunction = udf((x: mutable.WrappedArray[Long]) => median(x.toList))


  case class FreqCrimeTypes(DISTRICT: String, crime_type: String, crimes: Long)

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage Boston-crimea: {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder} ")
      sys.exit(-1)
    }
    val crimeFilename: String = args(0)
    val offenseCodesFilename: String = args(1)
    val resultFolder: String = args(2)

    val appName: String = "Boston-crimea"
    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .config("spark.driver.memory", "5g")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.sqlContext.implicits._

    val dfOffenceCodes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(offenseCodesFilename)
      .withColumn("crime_type", rtrim(split($"NAME", "-")(0)))

    val dfOffenceCodesBr: Broadcast[DataFrame]  = spark.sparkContext.broadcast(dfOffenceCodes)

    val dfCrime = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(crimeFilename)

    val dfCrimeBoston = dfCrime
      .join(dfOffenceCodesBr.value, ltrim(dfCrime("OFFENSE_CODE"),"0") === dfOffenceCodesBr.value("CODE"), "inner")
      .select("INCIDENT_NUMBER", "DISTRICT", "MONTH", "Lat", "Long", "crime_type")
      .na.fill(0.0) // replace Null with 0 for Lat and Long
      .cache

    /* общее количество преступлений в этом районе*/
    val dfCrimesTotal = dfCrimeBoston.groupBy( $"DISTRICT")
      .agg(count($"INCIDENT_NUMBER").alias("crimes_total"))
    dfCrimesTotal.show()

    /* медиана числа преступлений в месяц в этом районе с использованием функции percentile_approx()
     (не точная - для четных значений берется первый элемент из середины отсортированного списка) */
    val dfGroupByDistrictMonth = dfCrimeBoston.groupBy($"DISTRICT", $"MONTH")
      .agg(count($"INCIDENT_NUMBER").alias("crimes"))
    val dfCrimesMonthly = dfGroupByDistrictMonth.groupBy($"DISTRICT")
      .agg(percentile_approx($"crimes", lit(0.5), lit(100)).alias("crimes_monthly"))
    println("с использованием percentile_approx()")
    dfCrimesMonthly.show()

    /* медиана числа преступлений в месяц в этом районе с использованием UDF
     (более точная - для четных значений берется среднее двух элементов из середины отсортированного списка) */
    val dfGroupByDistrictMonthUDF = dfCrimeBoston
      .groupBy($"DISTRICT", $"MONTH")
      .agg(count($"INCIDENT_NUMBER").alias("crimes"))
      // Group by district and calculate median
      .groupBy($"DISTRICT")
      .agg(collect_list($"crimes").alias("month_list"))
      .withColumn("crimes_monthly", medianUDF($"month_list"))
      .drop($"month_list")
    println("более точная с использованием UDF")
    dfGroupByDistrictMonthUDF.show()

    /* три самых частых crime_type за всю историю наблюдений в этом районе, объединенных через запятую
    с одним пробелом “, ” , расположенных в порядке убывания частоты
    (версия c groupByKey) */
    val dfFreqCrimeTypes = dfCrimeBoston.groupBy($"DISTRICT", $"crime_type")
      .agg(count($"INCIDENT_NUMBER").alias("crimes"))
      .as[FreqCrimeTypes]
      .groupByKey(x => x.DISTRICT)
      .flatMapGroups{
        case(districtKey, elements) => elements.toList.sortBy(x => - x.crimes).take(3)
      }
      .groupBy($"DISTRICT")
      .agg(collect_list($"crime_type").alias("crime_type_list"))
      .withColumn("frequent_crime_types", concat_ws(", ",col("crime_type_list")))
      .drop("crime_type_list")
    dfFreqCrimeTypes.show(false)

    /* три самых частых crime_type за всю историю наблюдений в этом районе, объединенных через запятую
    с одним пробелом “, ” , расположенных в порядке убывания частоты
    (версия c оконной функцией) */
    val window: WindowSpec = Window.partitionBy($"DISTRICT").orderBy($"crimes".desc)
    val dfFreqCrimeTypesWindow = dfCrimeBoston
      .groupBy($"DISTRICT", $"crime_type")
      .agg(count($"INCIDENT_NUMBER").alias("crimes"))
      .withColumn("rn", row_number().over(window))
      .filter($"rn" < 4)
      .drop($"rn")
      .groupBy($"DISTRICT")
      .agg(collect_list($"crime_type").alias("crime_type_list"))
      .withColumn("frequent_crime_types", concat_ws(", ",col("crime_type_list")))
      .drop($"crime_type_list")
    println("с использованием оконной функции:")
    dfFreqCrimeTypesWindow.show(false)

    /* широта координаты района, рассчитанная как среднее по всем широтам инцидентов,
       долгота координаты района, рассчитанная как среднее по всем долготам инцидентов */
    val dfLatLong = dfCrimeBoston.groupBy( $"DISTRICT")
      .agg(mean($"Lat").alias("lat"),
        mean($"Long").alias("long"))
    dfLatLong.show()

    val result: DataFrame = dfCrimesTotal.na.fill(" ")
      .join(dfCrimesMonthly.na.fill(" "), Seq("DISTRICT"))
      .join(dfFreqCrimeTypes.na.fill(" "), Seq("DISTRICT"))
      .join(dfLatLong.na.fill(" "), Seq("DISTRICT"))
    result.show(false)

    result.repartition(1)
      .write
      .mode("OVERWRITE")
      .parquet(resultFolder)

    // новое изменение
    result.printSchema()

    spark.stop()
    }
}
