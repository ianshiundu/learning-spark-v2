ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

run / fork := true

lazy val sparkVersion = "3.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "learning-spark-v2",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe" % "config" % "1.4.1",
      "org.scalatest" %% "scalatest" % "3.1.1" % Test,
    ),
  )
