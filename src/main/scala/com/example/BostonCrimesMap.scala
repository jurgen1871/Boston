package com.example

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{callUDF, lit}

object BostonCrimesMap extends App {
  // Check number of arguments
  if (args.length != 3) {
    println("Error: Incorrect numbers arguments")
    println("Usage: /path/to/jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}")
    sys.exit(-1)
  }

  // parse arguments
  val crimeFile: String = args(0)
  val offenseCodesFile: String = args(1)
  val outFolder: String = args(2)

  // Create Spark Session
  val spark = SparkSession.builder()
    //.master("local")
    .getOrCreate()

  import spark.implicits._

  // Read crime dataset (DISTRICT is null ignored)
  val crimeDF: DataFrame = spark
    .read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv(crimeFile)
    .filter($"DISTRICT".isNotNull)

  // Read offense_codes dataset
  val offenseDF: DataFrame = spark
    .read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv(offenseCodesFile)
    .withColumn("CRIME_TYPE", trim(split($"NAME", "-")(0)))

  // broadcast offense_codes
  val offenseBC: Broadcast[DataFrame] = spark.sparkContext.broadcast(offenseDF)

  // Join datafame crime with offense_codes
  val crimeOffence: DataFrame = crimeDF
    .join(offenseBC.value, crimeDF("OFFENSE_CODE") === offenseBC.value("CODE"))
    // import only uses columns
    .select("INCIDENT_NUMBER", "DISTRICT", "YEAR", "MONTH", "Lat", "Long", "CRIME_TYPE")
    // fill 0 for Lat and Long is null
    .na.fill(0.0)
    .cache

  // Calculate crimes_total
  val crimes_total = crimeOffence
    .groupBy($"DISTRICT")
    .agg(count($"INCIDENT_NUMBER").alias("crimes_total"))

  // Calculate median crimes_monthly
  val crimes_monthly = crimeOffence
    .groupBy($"DISTRICT", $"YEAR", $"MONTH")
    .agg(count($"INCIDENT_NUMBER").alias("group_cnt"))
    .groupBy($"DISTRICT")
    .agg(callUDF("percentile_approx", $"group_cnt", lit(0.5)).as("crimes_monthly"))

  // Calculate frequent_crime_types
  val frequent_crime_types = crimeOffence
    .groupBy($"DISTRICT", $"CRIME_TYPE")
    .agg(count($"INCIDENT_NUMBER").alias("group_cnt"))
    .withColumn("numb", row_number().over(Window.partitionBy($"DISTRICT").orderBy($"group_cnt".desc)))
    .filter($"numb" < 4)
    .drop($"numb")
    .groupBy($"DISTRICT")
    .agg(collect_list($"CRIME_TYPE").alias("crime_list"))
    .withColumn("frequent_crime_types", concat_ws(", ", $"crime_list"))
    .drop($"crime_list")

  // Calculate average Lat and Long
  val lat_long = crimeOffence
    .groupBy($"DISTRICT")
    .agg(mean($"Lat").alias("lat"), mean($"Long").alias("lng"))

  // all together
  val result: DataFrame = crimes_total
    .join(crimes_monthly, Seq("DISTRICT"))
    .join(frequent_crime_types, Seq("DISTRICT"))
    .join(lat_long, Seq("DISTRICT"))

  result.repartition(1)
    .write
    .mode("OVERWRITE")
    .parquet(outFolder)

  spark.stop()

}