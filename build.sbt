name := "data_engineer_case"

version := "0.1"

scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.0" % Test,
  "org.scalatest" %% "scalatest-funsuite" % "3.2.0" % Test
)