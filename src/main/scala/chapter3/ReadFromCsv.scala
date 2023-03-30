package chapter3

import spark.Spark
import org.apache.spark.sql.types._
import config.Config._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}

object ReadFromCsv extends Spark {
  import spark.implicits._

  val fireSchema: StructType = StructType(Array(
    StructField("CallNumber", IntegerType, nullable = true),
    StructField("UnitID", StringType, nullable = true),
    StructField("IncidentNumber", IntegerType, nullable = true),
    StructField("CallType", StringType, nullable = true),
    StructField("Location", StringType, nullable = true),
    StructField("CallDate", StringType, nullable = true),
    StructField("WatchDate", StringType, nullable = true),
    StructField("CallFinalDisposition", StringType, nullable = true),
    StructField("AvailableDtTm", StringType, nullable = true),
    StructField("Address", StringType, nullable = true),
    StructField("City", StringType, nullable = true),
    StructField("Zipcode", StringType, nullable = true),
    StructField("Battalion", StringType, nullable = true),
    StructField("StationArea", StringType, nullable = true),
    StructField("Box", StringType, nullable = true),
    StructField("OriginalPriority", StringType, nullable = true),
    StructField("Priority", StringType, nullable = true),
    StructField("FinalPriority", IntegerType, nullable = true),
    StructField("ALSUnit", BooleanType, nullable = true),
    StructField("CallTypeGroup", StringType, nullable = true),
    StructField("NumAlarms", IntegerType, nullable = true),
    StructField("UnitType", StringType, nullable = true),
    StructField("UnitSequenceInCallDispatch", IntegerType, nullable = true),
    StructField("FirePreventionDistrict", StringType, nullable = true),
    StructField("SupervisorDistrict", StringType, nullable = true),
    StructField("Neighborhood", StringType, nullable = true),
    StructField("RowID", StringType, nullable = true),
    StructField("Delay", FloatType, nullable = true)))

  val fireDF: DataFrame = spark.read.schema(fireSchema).option("header", "true").csv(sfFireDataset)
//  val parquetPath = "/Users/ian/learning/fire-data"
//  val parquetTable = "/Users/ian/learning/fire-table"

//  fireDF.write.format("parquet").save(parquetPath)
//  fireDF.write.format("parquet").save(parquetTable)

  val fewFireDF: Dataset[Row] = fireDF
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") =!= "Medical Incident")

  fewFireDF.show(5, truncate = false)

  fireDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .agg(count_distinct(col("CallType")).as("DistinctCallTypes"))
    .show()

  fireDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .distinct()
    .show(10, truncate = false)

  // Rename columns
  val newFireDF = fireDF
    .withColumnRenamed("Delay", "ResponseDelayedinMins")
    .select("ResponseDelayedinMins")
    .where(col("ResponseDelayedinMins") > 5)
//    .show(5, truncate = true)

  val dateFmt = "MM/dd/yyyy"
  // Convert types
  val fireTsDF = fireDF
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), dateFmt))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), dateFmt))
    .drop("WatchDate")
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), s"$dateFmt hh:mm:ss a"))
    .drop("AvailableDtTm")
    .withColumnRenamed("Delay", "ResponseDelayedinMins")
//    .where(col("OnWatchDate").isNotNull && col("AvailableDtTS").isNotNull)

  fireTsDF
    .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
    .show(5, truncate = false)

  fireDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .groupBy(col("CallType"))
    .count()
    .orderBy(desc("count"))
    .show(10, truncate = false)

  fireTsDF
    .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
      F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))

  val fireDF2018 = fireDF
    .filter(
      col("CallType").isNotNull &&
        year(to_timestamp(col("CallDate"), "MM/dd/yyyy")) === "2018"
    )

  val callTypesIn2018 =
    fireDF2018
      .select("CallType")
      .distinct()

//  callTypesIn2018.show(truncate = false)

  val monthsWithTheHighestCalls =
    fireDF2018
      .withColumn("Month", month(to_timestamp(col("CallDate"), "MM/dd/yyyy")))
      .groupBy("Month")
      .agg(count(col("CallType")).as("NumberOfCalls"))
      .orderBy(desc("NumberOfCalls"))
      .show(truncate = false)


  val neighborhoodsWithTheMostFireCalls =
    fireDF2018
      .filter(col("Neighborhood").isNotNull)
      .groupBy("Neighborhood")
      .agg(count(col("CallType")).as("NumberOfCalls"))
      .orderBy(desc("NumberOfCalls"))

  neighborhoodsWithTheMostFireCalls.show(10) //take(1).foreach(println)//.show(1)

  val worstResponseTimesByNeighborhoods=
    fireDF2018.filter(col("Neighborhood").isNotNull).groupBy("Neighborhood")
      .agg(max("Delay").alias("MaxResponseDelayInMins"))
      .orderBy(desc("MaxResponseDelayInMins"))

  worstResponseTimesByNeighborhoods.show(false)

  val weekWithTheMostFireCalls =
    fireDF2018
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .groupBy(weekofyear(col("IncidentDate")).as("WeekOfTheYear"))
      .count()
      .orderBy(desc("count"))

  weekWithTheMostFireCalls.show(10,truncate = false)

  val neighborhoodsWithTheMostFireCalls2 =
    fireDF2018
      .filter(col("Neighborhood").isNotNull)
      .groupBy("Neighborhood")
      .count()
      .orderBy(desc("count"))

  neighborhoodsWithTheMostFireCalls2.show(1) //take(1).foreach(println)//.show(1)

}
