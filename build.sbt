name := "Boston"

version := "1.0"

scalaVersion := "2.12.10"

lazy val sparkVersion = "3.1.1"


libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-sql" % sparkVersion % "provided"
)
