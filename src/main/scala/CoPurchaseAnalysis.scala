package it.lucapolese.unibo

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object CoPurchaseAnalysis {
  def main(args: Array[String]): Unit = {
    // Start timing the execution
    val startTime = System.currentTimeMillis()

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .config("spark.executor.cores", "8") // Number of cores per executor
      .config("spark.executor.instances", "2") // Number of executor instances
      .getOrCreate()

    // Read dataset from CSV file
    val inputPath = "data/order_products.csv"
    val outputPath = "output/co_purchase_results"
    val rawData: RDD[String] = spark.sparkContext.textFile(inputPath)

    // Determine number of partitions based on dataset size
    val cores = spark.conf.get("spark.executor.cores").toInt
    val instances = spark.conf.get("spark.executor.instances").toInt
    val numPartitions = cores * instances * 3

    println(s"numPartitions = ${numPartitions}")

    // Parse dataset (order_id, product_id)
    val orderProductPairs: RDD[(Int, Int)] = rawData.map(line => {
      val cols = line.split(",")
      (cols(0).toInt, cols(1).toInt)
    }).partitionBy(new HashPartitioner(numPartitions))

    // Group by order_id to get products in each order
    val orderToProducts: RDD[(Int, Iterable[Int])] = orderProductPairs.groupByKey()

    // Generate co-purchase pairs (unordered unique pairs per order)
    val coPurchasePairs: RDD[((Int, Int), Int)] = orderToProducts
      .flatMap { case (_, products) =>
        val productList = products.toList
        productList.combinations(2).map { combination =>
          // Ensure products are sorted to create consistent pairs
          val sortedPair = if (combination.head <= combination(1))
            (combination.head, combination(1))
          else
            (combination(1), combination.head)
          (sortedPair, 1)
        }
      }

    // Reduce by key to count occurrences of each co-purchase pair
    val coPurchaseCounts: RDD[((Int, Int), Int)] = coPurchasePairs
      // Even distribution of keys across partitions before the reduce operation
      .partitionBy(new HashPartitioner(numPartitions))
      .reduceByKey(_ + _)

    // Transform the RDD[((Int, Int), Int)] to RDD[String] before saving
    val resultRDD: RDD[String] = coPurchaseCounts.map {
      case ((p1, p2), count) => s"$p1,$p2,$count"
    }

    // Create a header
    val header = spark.sparkContext.parallelize(Seq("product1,product2,count"))

    // Save results - use coalesce(1) to write to a single file without sorting
    // Combine header with data
    header.union(resultRDD).coalesce(1).saveAsTextFile(outputPath)

    // Log execution details
    println(s"Number of partitions: $numPartitions")
    println(s"Total records processed: ${rawData.count()}")

    // Calculate and log execution time
    val endTime = System.currentTimeMillis()
    val executionTimeInSeconds = (endTime - startTime) / 1000.0
    println(s"Total execution time: $executionTimeInSeconds seconds")

    // Stop Spark Session
    spark.stop()
  }
}