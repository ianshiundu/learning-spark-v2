package chapter2

import config.Config._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import spark.Spark

object MnMCountWithDataFrames extends Spark {

  val mnmDF: DataFrame = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(mnmDataset)

  val countMnMDF: Dataset[Row] =
    mnmDF
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(col("Total").desc)

  val CAMnMCount: Dataset[Row] = countMnMDF.filter("State == 'CA'").orderBy(desc("Total"))

}
