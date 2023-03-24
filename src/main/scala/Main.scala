import chapter2.{MnMCountWithDataFrames, MnMCountWithRDD}
import chapter3.ReadJsonFile

object Main {
  def main(args: Array[String]): Unit = {

//    val mnmCountDF = MnMCountWithDataFrames
//    val mnmCountRDD = MnMCountWithRDD
    // chapter 3
    val readJsonDF = ReadJsonFile

//    mnmCountDF.countMnMDF.show(60, truncate = false)
//    println(s"Total rows = ${mnmCountDF.countMnMDF.count()}")
//
//    mnmCountDF.CAMnMCount.show(10)
//
//    mnmCountRDD.countMnMRdd.take(60).foreach(println)
//
//    mnmCountRDD.CAMnMCount.take(10).foreach(println)

//    readJsonDF.blogsDF.show(false)
//    println(readJsonDF.blogsDF.printSchema)
//    println(readJsonDF.blogsDF.schema)

    readJsonDF.authorsId.show(4)
  }
}
