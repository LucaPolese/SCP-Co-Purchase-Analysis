#!/bin/bash
# gcp_cluster_scaling_test.sh - Execute scaling tests on GCP Dataproc

set -e # Exit on error

# Define constants
IMAGE_VERSION="2.2-debian12"

# Get current version from build.sbt and prepare for increment
get_and_increment_version() {
  # Extract version components
  VERSION_LINE=$(grep "ThisBuild / version :=" build.sbt)
  MAJOR=$(echo $VERSION_LINE | sed -E 's/.*"([0-9]+)\.([0-9]+)\.([0-9]+)".*/\1/')
  MINOR=$(echo $VERSION_LINE | sed -E 's/.*"([0-9]+)\.([0-9]+)\.([0-9]+)".*/\2/')
  PATCH=$(echo $VERSION_LINE | sed -E 's/.*"([0-9]+)\.([0-9]+)\.([0-9]+)".*/\3/')

  # Increment patch version
  NEW_PATCH=$((PATCH + 1))

  # Create new version string
  NEW_VERSION="$MAJOR.$MINOR.$NEW_PATCH"

  # Update build.sbt
  sed -i "s/ThisBuild \/ version := \"$MAJOR\.$MINOR\.$PATCH\"/ThisBuild \/ version := \"$MAJOR\.$MINOR\.$NEW_PATCH\"/" build.sbt

  echo $NEW_VERSION
}

# Update Spark executor instances in Scala file
update_executor_instances() {
  local instances=$1
  sed -i "s/\.config(\"spark\.executor\.instances\", \"[0-9]\+\")/\.config(\"spark\.executor\.instances\", \"$instances\")/" src/main/scala/CoPurchaseAnalysis.scala
  echo "Updated Spark executor instances to $instances"
}

# Build and upload JAR to GCP bucket
build_and_upload() {
  local version=$1
  local scala_version=$(grep "scalaVersion :=" build.sbt | cut -d'"' -f2 | cut -d'.' -f1,2)
  local jar_file="co-purchase-analysis_$scala_version-$version.jar"

  echo "Building project with version $version..."
  sbt clean package

  # Check if build was successful
  local target_dir="target/scala-$scala_version"
  if [ ! -f "$target_dir/$jar_file" ]; then
    echo "Error: JAR file was not created. Build may have failed."
    exit 1
  fi

  # Upload JAR to GCP bucket
  echo "Uploading JAR to gs://$GCP_BUCKET/$jar_file"
  gsutil cp "$target_dir/$jar_file" "gs://$GCP_BUCKET/"

  echo "Successfully uploaded $jar_file to GCP bucket $GCP_BUCKET"
  return 0
}

# Run Spark job on GCP Dataproc cluster
run_spark_job() {
  local version=$1
  local executor_instances=$2
  local scala_version=$(grep "scalaVersion :=" build.sbt | cut -d'"' -f2 | cut -d'.' -f1,2)
  local jar_file="co-purchase-analysis_$scala_version-$version.jar"

  echo "Running Spark job on GCP cluster $CLUSTER_NAME"
  echo "Using JAR file: $jar_file with $executor_instances executor instances"

  gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --class=CoPurchaseAnalysis \
    --jars="gs://$GCP_BUCKET/$jar_file" \
    --properties="spark.executor.instances=$executor_instances" \
    -- $GCP_BUCKET   # Passing the bucket name as a command-line argument

  echo "Spark job completed"
}

# Create single-node cluster
create_single_node_cluster() {
  echo "Creating single-node cluster: $CLUSTER_NAME"

  gcloud dataproc clusters create $CLUSTER_NAME \
    --enable-component-gateway \
    --region $REGION \
    --zone $ZONE \
    --single-node \
    --master-machine-type n2-standard-4 \
    --master-boot-disk-type pd-balanced \
    --master-boot-disk-size 100 \
    --image-version $IMAGE_VERSION \
    --project $GCP_BUCKET

  echo "Single-node cluster created successfully"
}

# Create multi-node cluster
create_multi_node_cluster() {
  local worker_count=$1
  echo "Creating cluster with $worker_count workers: $CLUSTER_NAME"

  gcloud dataproc clusters create $CLUSTER_NAME \
    --enable-component-gateway \
    --region $REGION \
    --zone $ZONE \
    --master-machine-type n2-standard-2 \
    --master-boot-disk-type pd-balanced \
    --master-boot-disk-size 100 \
    --num-workers $worker_count \
    --worker-machine-type n2-standard-4 \
    --worker-boot-disk-type pd-balanced \
    --worker-boot-disk-size 100 \
    --image-version $IMAGE_VERSION \
    --project $GCP_BUCKET

  echo "Multi-node cluster with $worker_count workers created successfully"
}

# Update cluster worker count
update_cluster_workers() {
  local worker_count=$1
  echo "Updating cluster to $worker_count workers"

  gcloud dataproc clusters update $CLUSTER_NAME \
    --project $GCP_BUCKET \
    --region $REGION \
    --num-workers $worker_count

  echo "Cluster updated to $worker_count workers"
}

# Delete cluster
delete_cluster() {
  echo "Deleting cluster: $CLUSTER_NAME"

  gcloud dataproc clusters delete $CLUSTER_NAME \
    --region $REGION \
    --project $GCP_BUCKET \
    --quiet

  echo "Cluster deleted successfully"
}

# Check environment variables
check_environment() {
  echo "Checking environment variables..."

  # Source environment variables
  if [ -f .env ]; then
    source .env
    echo "Environment variables loaded from .env file"
  else
    echo "Error: No .env file found"
    exit 1
  fi

  # Validate required environment variables
  if [ -z "$GCP_BUCKET" ]; then
    echo "Error: GCP_BUCKET environment variable is not set in .env file"
    echo "Please add GCP_BUCKET=your-bucket-name to your .env file"
    exit 1
  fi

  if [ -z "$REGION" ]; then
    echo "Error: REGION environment variable is not set in .env file"
    echo "Please add REGION=your-gcp-region to your .env file"
    exit 1
  fi

  if [ -z "$ZONE" ]; then
    echo "Error: ZONE environment variable is not set in .env file"
    echo "Please add ZONE=your-gcp-zone to your .env file"
    exit 1
  fi

  if [ -z "$CLUSTER_NAME" ]; then
    echo "Error: CLUSTER_NAME environment variable is not set in .env file"
    echo "Please add CLUSTER_NAME=your-cluster-name to your .env file"
    exit 1
  fi

  echo "Environment check complete."
}

# Drop all running Dataproc clusters
drop_all_clusters() {
  echo "Checking for running Dataproc clusters..."

  # List all clusters and extract their names and regions
  local clusters=$(gcloud dataproc clusters list --project $GCP_BUCKET --region $REGION --format="csv[no-heading](name)")

  if [ -z "$clusters" ]; then
    echo "No running Dataproc clusters found."
    return 0
  fi

  echo "Found the following Dataproc clusters:"

  # Loop through each cluster and delete it
  for cluster_name in $clusters; do
    echo "Deleting cluster: $cluster_name in region $REGION"

    gcloud dataproc clusters delete $cluster_name \
      --region $REGION \
      --project $GCP_BUCKET \
      --quiet

    echo "Cluster $cluster_name deleted successfully"
  done

  echo "All Dataproc clusters have been deleted"
}

# Run single node test
run_single_node_test() {
  echo "========== STARTING SINGLE NODE TEST =========="

  # Create single-node cluster
  create_single_node_cluster

  # Build and upload
  local version=$(get_and_increment_version)
  build_and_upload $version

  # Run job with 1 executor
  run_spark_job $version 1

  # Delete cluster
  delete_cluster

  echo "========== SINGLE NODE TEST COMPLETED =========="
}

# Run scaling tests
run_scaling_tests() {
  echo "========== STARTING SCALING TESTS =========="

  # Define worker counts to test
  local worker_counts=(2 3 4)

  # Initial cluster with first worker count
  create_multi_node_cluster ${worker_counts[0]}

  # Loop through all worker counts
  for worker_count in "${worker_counts[@]}"; do
    echo "Testing with $worker_count workers"

    # Update cluster if not on first iteration
    if [ "$worker_count" != "${worker_counts[0]}" ]; then
      update_cluster_workers $worker_count
    fi

    # Build and upload
    local version=$(get_and_increment_version)
    build_and_upload $version

    # Run job with current worker count
    run_spark_job $version $worker_count
  done

  # Delete cluster
  delete_cluster

  echo "========== SCALING TESTS COMPLETED =========="
}

# Main execution flow
main() {
  echo "Starting GCP Dataproc scaling test workflow"

  # Check environment
  check_environment

  # First, drop any running clusters
  drop_all_clusters

  # Run single node test
  run_single_node_test

  # Run scaling tests
  run_scaling_tests

  echo "All tests completed successfully"
}

# Execute the script
main