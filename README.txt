# Co-Purchase Analysis Scaling Test on GCP Dataproc

## Project Overview

### Technical Architecture
This project implements a distributed co-purchase analysis using:
- Apache Spark for big data processing
- Google Cloud Dataproc  (on Google Cloud Platform) for managed Spark clusters
- Scala as the primary programming language
The script performs performance testing across single-node and multi-node Spark cluster configurations.


### Co-Purchase Analysis Algorithm
The core algorithm performs these key steps:
1. Read and parse order-product pairs from CSV
2. Group products by order
3. Generate all unique product pair combinations within each order
4. Count co-purchase frequencies
5. Output results as product pairs with their co-purchase count

### Performance Optimization Techniques
- Dynamic partitioning based on cluster configuration
- HashPartitioner for even data distribution
- Minimized data shuffling
- Configurable executor and memory settings

## Prerequisites

1. Google Cloud Platform (GCP) Account
2. Google Cloud SDK installed
3. `gcloud` CLI configured
4. SBT (Scala Build Tool)
5. Bash shell

### System Dependencies
- Java 17 JDK
- Scala 2.12.18
- SBT 1.10.x
- Google Cloud SDK

## Environment Setup

### 1. Create a `.env` File

Create a `.env` file in the project root with the following environment variables:

```bash
GCP_BUCKET=your-gcp-bucket-name
REGION=your-gcp-region     # e.g., us-central1
ZONE=your-gcp-zone         # e.g., us-central1-a
CLUSTER_NAME=dataproc-scaling-test
```

### 2. Prepare Input Data - Uploading Dataset to GCP Bucket

- Upload your `order_products.csv` to the specified GCP bucket
- Ensure the CSV has two columns: order_id and product_id

#### Method 1: Using Google Cloud Console
1. Open Google Cloud Console
2. Navigate to Cloud Storage
3. Select or create your bucket
4. Click "Upload Files"
5. Select `order_products.csv`

#### Method 2: Using gcloud CLI
```bash
# Upload with specific metadata/access control
gcloud storage buckets create gs://your-bucket-name \
  --location=your-gcp-region \
  --project=your-project-id

gcloud storage cp order_products.csv gs://your-bucket-name/
```

## Repository Structure

- `gcp_cluster_scaling_test.sh`: Bash script for cluster management and Spark job execution
- `build.sbt`: SBT build configuration
- `src/main/scala/CoPurchaseAnalysis.scala`: Spark job implementation
- `.env`: Environment configuration (not tracked in version control)

## Deployment and Execution

### Key Bash Script Functions

1. `create_single_node_cluster()`: Creates a single-node Dataproc cluster
2. `create_multi_node_cluster()`: Creates a multi-node Dataproc cluster
3. `run_spark_job()`: Submits Spark job to the cluster
4. `update_cluster_workers()`: Dynamically updates cluster worker count
5. `drop_all_clusters()`: Removes all existing clusters in the specified region

### Detailed gcloud Commands Explained

#### 1. Cluster Creation
```bash
gcloud dataproc clusters create $CLUSTER_NAME
  --region $REGION                    # GCP region for deployment
  --zone $ZONE                        # Specific zone within the region
  --single-node/--num-workers         # Cluster type and worker count
  --master-machine-type n2-standard-4 # Master node machine type
  --worker-machine-type n2-standard-4 # Worker node machine type
  --image-version 2.2-debian12        # Dataproc image version
  --project $GCP_BUCKET               # GCP project ID
```

#### 2. Spark Job Submission
```bash
gcloud dataproc jobs submit spark
  --cluster=$CLUSTER_NAME             # Target cluster
  --region=$REGION                    # Cluster's region
  --class=CoPurchaseAnalysis          # Main Spark job class
  --jars="gs://$GCP_BUCKET/jar_file"  # JAR location in GCS
  --properties="spark.executor.instances=X" # Dynamic configuration
```

#### 3. Cluster Update
```bash
gcloud dataproc clusters update $CLUSTER_NAME
  --region $REGION                    # Cluster's region
  --num-workers X                     # New worker count
```

#### 4. Cluster Deletion
```bash
gcloud dataproc clusters delete $CLUSTER_NAME
  --region $REGION                    # Cluster's region
  --quiet                             # Suppress confirmation prompt
```

## Script Execution

```bash
# Make script executable
chmod +x gcp_cluster_scaling_test.sh

# Run the scaling test
./gcp_cluster_scaling_test.sh
```

## Performance Tuning

### Configurable Parameters
In `CoPurchaseAnalysis.scala`:
- Adjust `numPartitions` calculation
- Modify executor core count
- Uncomment/configure Spark optimizations

## Performance Testing Strategy

The script performs two primary tests:
1. Single-node cluster performance
2. Scaling test with 2-4 worker nodes

Each test:
- Creates/updates a Dataproc cluster
- Builds and uploads the Spark JAR
- Executes the co-purchase analysis
- Collects performance metrics
- Deletes the cluster

## Customization

Modify these files to adapt the test:
- `build.sbt`: Change Scala/Spark versions
- `CoPurchaseAnalysis.scala`: Adjust Spark configurations
- `gcp_cluster_scaling_test.sh`: Change worker counts, machine types

Modify `gcp_cluster_scaling_test.sh`:
- Adjust `--master-machine-type`
- Change `--worker-machine-type`
- Customize disk sizes

## Outputs

Results are stored in:
- `gs://$GCP_BUCKET/output/$instances/co_purchase_results/`
- Performance logs printed to console

## Troubleshooting

- Ensure all environment variables are set
- Verify GCP credentials and permissions
- Check input data format and location