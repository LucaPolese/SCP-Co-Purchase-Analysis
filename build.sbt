ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"
val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang" % "scala-library" % "2.12.18"
)