import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object CoPurchaseAnalysis {
  def main(args: Array[String]): Unit = {
    // Start timing the execution
    val startTime = System.currentTimeMillis()

    // Get GCP bucket from command line arguments
    val gcpBucket = if (args.length > 0) args(0)
    else throw new IllegalArgumentException("GCP_BUCKET must be provided as a command line argument")

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Co-Purchase Analysis")
      .config("spark.executor.cores", "4")
      .config("spark.driver.memory", "6g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    // Determine number of partitions based on dataset size
    val cores = spark.conf.get("spark.executor.cores").toInt
    val instances = spark.conf.get("spark.executor.instances").toInt
    val numPartitions = cores * instances * 3  // Slightly more partitions for better parallelism

    try {
      // Read dataset from CSV file using environment variable
      val inputPath = s"gs://$gcpBucket/order_products.csv"
      val outputPath = s"gs://$gcpBucket/output/$instances/co_purchase_results/"

      // Read with more partitions for better parallelism
      val rawData: RDD[String] = spark.sparkContext.textFile(inputPath, numPartitions)

      // Parse dataset (order_id, product_id)
      val orderProductPairs: RDD[(Int, Int)] = rawData
        .map(line => {
          val cols = line.split(",")
          (cols(0).toInt, cols(1).toInt)
        })
        .partitionBy(new HashPartitioner(numPartitions))

      // Group by order_id to get products in each order
      val orderToProducts: RDD[(Int, Iterable[Int])] = orderProductPairs.groupByKey()

      // Generate co-purchase pairs with better memory management
      val coPurchasePairs: RDD[((Int, Int), Int)] = orderToProducts.flatMap { case (_, products) =>
        val productList = products.toList
        for {
          i <- productList.indices
          j <- (i + 1) until productList.size
        } yield ((productList(i), productList(j)), 1)
      }

      // Reduce by key to count occurrences of each co-purchase pair
      val coPurchaseCounts: RDD[((Int, Int), Int)] = coPurchasePairs
        .partitionBy(new HashPartitioner(numPartitions))
        .reduceByKey(_ + _)

      // All results to partitioned files
      coPurchaseCounts.map{
        case ((p1, p2), count) => s"$p1,$p2,$count"
      }.saveAsTextFile(outputPath)

      // Calculate and log execution time
      val endTime = System.currentTimeMillis()
      val executionTimeInSeconds = (endTime - startTime) / 1000.0
      // Log execution details
      println(s"Number of partitions: $numPartitions")
      println(s"Total execution time: $executionTimeInSeconds seconds")
    } catch {
      case e: Exception =>
        println(s"Error in co-purchase analysis: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Stop Spark Session
      spark.stop()
    }
  }
}