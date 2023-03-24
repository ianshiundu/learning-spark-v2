package chapter3

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SchemaDefinition {
  val schema = StructType(
    Array(
      StructField("author", StringType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("pages", IntegerType, nullable = false)
    )
  )

  // Defining the schema using Data Definition Language DDL
  val schema2 = "author STRING, title STRING, pages INT"

}
