package chapter3

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.Spark
import config.Config._
import org.apache.spark.sql._

object ReadJsonFile extends Spark {
  import spark.implicits._

  val schema = StructType(
    Array(
      StructField("Id", IntegerType, nullable = false),
      StructField("First", StringType, nullable = false),
      StructField("Last", StringType, nullable = false),
      StructField("Url", StringType, nullable = false),
      StructField("Published", StringType, nullable = false),
      StructField("Hits", IntegerType, nullable = false),
      StructField("Campaigns", ArrayType(StringType), nullable = false)
    )
  )

  val blogsDF: DataFrame = spark
    .read
    .schema(schema)
    .json(jsonDataset)

  blogsDF.select(expr("Hits * 2")).show(2)

  blogsDF.select(col("Hits") * 2).show(2)

  blogsDF.withColumn("Big Hitters", expr("Hits > 10000")).show()

  val authorsId =
    blogsDF
      .withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))
      .select("AuthorsId")

  blogsDF.select(expr("Hits")).show(2)
  blogsDF.select(col("Hits")).show(2)
  blogsDF.select("Hits").show(2)

  // Sort by column Id in descending order
  blogsDF.sort(col("Id").desc).show()

  // Rows
  val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
  val authorsDF = rows.toDF("Author", "State")
  authorsDF.show()


}
