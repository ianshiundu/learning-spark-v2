package spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait Spark {
  val spark: SparkSession = SparkSession.builder()
    .appName("LearningSparkV2")
    .config("spark.master", "local")
//    .config("spark.executor.memory", "4g")
//    .config("spark.driver.memory", "2g")
//    .config("spark.executor.cores", "4")
    .getOrCreate()
}

object Spark extends Spark {
  implicit val sc: SparkContext = spark.sparkContext
}
