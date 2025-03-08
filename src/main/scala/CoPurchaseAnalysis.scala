package it.lucapolese.unibo
import org.apache.spark.sql.SparkSession

object CoPurchaseAnalysis {
  def main(args: Array[String]): Unit = {

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .getOrCreate()

    val context = spark.sparkContext

    // Stop Spark Session
    spark.stop()
  }
}