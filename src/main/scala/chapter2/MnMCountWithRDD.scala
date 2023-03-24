package chapter2

import spark.Spark
import config.Config._
import org.apache.spark.rdd.RDD

object MnMCountWithRDD {
  val rdd: RDD[String] = Spark.sc.textFile(mnmDataset)
  val header: String = rdd.first()
  val rows: RDD[String] = rdd.filter(_ != header) // remove header

  val mnmData: RDD[(String, String, String)] =
    rows.map {
      row =>
        val fields = row.split(",")
        (fields(0), fields(1), fields(2))
    }

  val countMnMRdd: RDD[(String, String, Long)] =
    mnmData
      .map(row => ((row._1, row._2), 1L))
      .reduceByKey(_ + _)
      .sortBy{ case (_, count) => -count }
      .map{ row => (row._1._1, row._1._2, row._2)}

  val CAMnMCount: RDD[(String, String, Long)] = countMnMRdd.filter(_._1 == "CA")

}
