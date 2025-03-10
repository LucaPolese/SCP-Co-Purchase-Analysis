ThisBuild / version := "0.2.0"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "Co-Purchase-Analysis",
    idePackagePrefix := Some("it.lucapolese.unibo")
  )

val sparkVersion = "3.5.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)