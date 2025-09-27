#!/bin/bash

# Dataproc Serverless Spark Job Submission Script
#
# Usage:
#   ./submit_job.sh --project-id YOUR_PROJECT_ID [options]
#
# Example:
#   ./submit_job.sh --project-id my-project --total-records 1000000

set -e

# Default values
PROJECT_ID=""
DATASET_ID="mdm_demo"
TABLE_SUFFIX="_scale"
TOTAL_RECORDS=1000000
UNIQUE_CUSTOMERS=""  # Will be auto-calculated if not specified
WRITE_MODE="overwrite"
PARTITIONS=1000
REGION="us-central1"
SUBNET=""
SERVICE_ACCOUNT=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --project-id)
      PROJECT_ID="$2"
      shift 2
      ;;
    --dataset-id)
      DATASET_ID="$2"
      shift 2
      ;;
    --table-suffix)
      TABLE_SUFFIX="$2"
      shift 2
      ;;
    --total-records)
      TOTAL_RECORDS="$2"
      shift 2
      ;;
    --unique-customers)
      UNIQUE_CUSTOMERS="$2"
      shift 2
      ;;
    --write-mode)
      WRITE_MODE="$2"
      shift 2
      ;;
    --partitions)
      PARTITIONS="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --subnet)
      SUBNET="$2"
      shift 2
      ;;
    --service-account)
      SERVICE_ACCOUNT="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 --project-id PROJECT_ID [options]"
      echo ""
      echo "Required:"
      echo "  --project-id         Google Cloud Project ID"
      echo ""
      echo "Optional:"
      echo "  --dataset-id         BigQuery dataset (default: mdm_demo)"
      echo "  --table-suffix       Table name suffix (default: _scale)"
      echo "  --total-records      Total records to generate (default: 100M)"
      echo "  --unique-customers   Unique customers (default: auto-calculated as 25% of total)"
      echo "  --write-mode         overwrite|append (default: overwrite)"
      echo "  --partitions         Spark partitions (default: 1000)"
      echo "  --region             GCP region (default: us-central1)"
      echo "  --subnet             VPC subnet (optional)"
      echo "  --service-account    Service account email (optional)"
      echo ""
      echo "Examples:"
      echo "  # Basic 100M record generation (25M unique customers auto-calculated)"
      echo "  $0 --project-id my-project"
      echo ""
      echo "  # Small test run (250K unique customers auto-calculated)"
      echo "  $0 --project-id my-project --total-records 1000000"
      echo ""
      echo "  # Custom ratios - explicit unique customers"
      echo "  $0 --project-id my-project --total-records 1000000 --unique-customers 200000"
      echo ""
      echo "  # Large scale with custom settings"
      echo "  $0 --project-id my-project --total-records 1000000000 --partitions 5000"
      echo ""
      echo "Note: In MDM scenarios, unique customers should be less than total records"
      echo "      as each customer appears in multiple systems with variations."
      exit 0
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

# Validate required parameters
if [[ -z "$PROJECT_ID" ]]; then
  echo "❌ Error: --project-id is required"
  echo "Use --help for usage information"
  exit 1
fi

# Auto-calculate unique customers if not specified
# Rule: unique customers should be about 25% of total records for realistic MDM scenario
if [[ -z "$UNIQUE_CUSTOMERS" ]]; then
  UNIQUE_CUSTOMERS=$(echo "scale=0; $TOTAL_RECORDS / 4" | bc)
  echo "ℹ️  Auto-calculated unique customers: $(printf "%'d" $UNIQUE_CUSTOMERS) (25% of total records)"
fi

# Validate that unique customers doesn't exceed total records
if [[ $UNIQUE_CUSTOMERS -gt $TOTAL_RECORDS ]]; then
  echo "❌ Error: Unique customers ($UNIQUE_CUSTOMERS) cannot exceed total records ($TOTAL_RECORDS)"
  echo "In MDM scenarios, each unique customer generates multiple records across different systems."
  echo "Suggested fix: Either increase --total-records or decrease --unique-customers"
  exit 1
fi

# Warn if ratio seems unrealistic
RATIO=$(echo "scale=2; $UNIQUE_CUSTOMERS * 100 / $TOTAL_RECORDS" | bc)
RATIO_INT=$(echo "scale=0; $RATIO / 1" | bc)
if [[ $RATIO_INT -gt 50 ]]; then
  echo "⚠️  Warning: High unique customer ratio (${RATIO_INT}% of total records)"
  echo "   This may not represent realistic MDM duplication patterns."
fi

# Calculate expected record counts for display
EXPECTED_CRM=$(echo "scale=0; $TOTAL_RECORDS * 0.8 * 1.15 / 1" | bc)
EXPECTED_ERP=$(echo "scale=0; $TOTAL_RECORDS * 0.7 / 1" | bc)
EXPECTED_ECOMMERCE=$(echo "scale=0; $TOTAL_RECORDS * 0.6 * 1.3 / 1" | bc)

echo "🚀 Starting PySpark MDM Data Generation"
echo "========================================="
echo "Project:           $PROJECT_ID"
echo "Dataset:           $DATASET_ID"
echo "Region:            $REGION"
echo "Total Records:     $(printf "%'d" $TOTAL_RECORDS)"
echo "Unique Customers:  $(printf "%'d" $UNIQUE_CUSTOMERS)"
echo "Partitions:        $PARTITIONS"
echo "Write Mode:        $WRITE_MODE"
echo ""
echo "Expected Output:"
echo "  CRM:             ~$(printf "%'d" $EXPECTED_CRM) records"
echo "  ERP:             ~$(printf "%'d" $EXPECTED_ERP) records"
echo "  E-commerce:      ~$(printf "%'d" $EXPECTED_ECOMMERCE) records"
echo "========================================="

# Check if gcloud is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
  echo "❌ Error: No active gcloud authentication found"
  echo "Please run: gcloud auth application-default login"
  exit 1
fi

# Check if required APIs are enabled
echo "🔍 Checking required APIs..."
REQUIRED_APIS=("dataproc.googleapis.com" "bigquery.googleapis.com" "compute.googleapis.com")
for api in "${REQUIRED_APIS[@]}"; do
  if ! gcloud services list --enabled --filter="name:$api" --format="value(name)" | grep -q "$api"; then
    echo "❌ Error: $api is not enabled"
    echo "Enable it with: gcloud services enable $api"
    exit 1
  fi
done

# Create temporary GCS bucket if it doesn't exist
TEMP_BUCKET="${PROJECT_ID}-dataproc-temp"
if ! gsutil ls -b gs://$TEMP_BUCKET >/dev/null 2>&1; then
  echo "📦 Creating temporary GCS bucket: $TEMP_BUCKET"
  gsutil mb -p $PROJECT_ID gs://$TEMP_BUCKET
fi

# Upload the PySpark script to GCS
SCRIPT_GCS_PATH="gs://$TEMP_BUCKET/spark-mdm/spark_data_generator.py"
echo "📤 Uploading PySpark script to $SCRIPT_GCS_PATH"
gsutil cp spark_data_generator.py $SCRIPT_GCS_PATH

# Auto-create dependencies if missing
DEPENDENCIES_GCS_PATH="gs://$TEMP_BUCKET/spark-mdm/dependencies.zip"
echo "📦 Uploading Python dependencies to $DEPENDENCIES_GCS_PATH"

if [[ ! -f "dependencies.zip" ]]; then
  echo "📦 Creating Python dependencies from requirements.txt..."

  if [[ ! -f "requirements.txt" ]]; then
    echo "❌ Error: requirements.txt not found"
    echo "Please create requirements.txt with your Python dependencies"
    exit 1
  fi

  mkdir -p dependencies
  pip3 install -r requirements.txt --target=dependencies/ --quiet
  cd dependencies && zip -r ../dependencies.zip . --quiet
  cd ..
  echo "✓ Dependencies created successfully from requirements.txt"
fi

gsutil cp dependencies.zip $DEPENDENCIES_GCS_PATH
echo "✓ Dependencies zip uploaded successfully"

# Build the gcloud dataproc batches submit pyspark command
BATCH_ID="mdm-data-gen-$(date +%Y%m%d-%H%M%S)"

echo "📦 Using pre-built Python dependencies via --py-files..."

# ============================================
# SCALING LIMITS (Cost Control & Performance)
# ============================================
# These limits prevent runaway costs while maintaining excellent performance
#
# WITHOUT LIMITS: 1000 partitions could spawn 1000+ executors = $1000+/hour
# WITH LIMITS:    Max 150 executors with controlled resources = $50-100/hour
#
# For 100M records with 1000 partitions, these settings provide optimal balance:
# - Fast processing through parallelism (150 executors)
# - Cost control through resource limits
# - BigQuery write optimization (repartition to 200 in code)

# Executor Scaling Limits
MAX_EXECUTORS=150           # Maximum executors (prevents cost explosion)
MIN_EXECUTORS=10            # Minimum executors (faster startup)
INITIAL_EXECUTORS=20        # Starting executors (balanced initialization)

# Resource Per Executor (Cost vs Performance)
EXECUTOR_CORES=4            # 4 cores per executor (minimum allowed for Serverless)
EXECUTOR_MEMORY="8g"        # 8GB per executor (vs default 15GB = 47% cost reduction)

# Driver Resources (Minimal - driver doesn't do heavy lifting)
DRIVER_CORES=4              # 4 cores for driver (minimum allowed for Serverless)
DRIVER_MEMORY="4g"          # Driver memory

# Cleanup Settings (Note: Dataproc Serverless auto-cleans up when job completes)
# Manual cleanup available via: gcloud dataproc batches cancel BATCH_ID

# ============================================
# ADVANCED: Override scaling limits if needed
# ============================================
# Uncomment and modify these lines to customize for specific needs:
#
# For smaller jobs (10M records):
# MAX_EXECUTORS=50
# EXECUTOR_MEMORY="8g"
#
# For high-performance requirements:
# MAX_EXECUTORS=300
# EXECUTOR_MEMORY="16g"
# ============================================

# Build properties string with scaling limits and performance optimizations
SPARK_PROPERTIES="spark.sql.adaptive.enabled=true"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.sql.adaptive.coalescePartitions.enabled=true"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.dynamicAllocation.enabled=true"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS}"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS}"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.dynamicAllocation.initialExecutors=${INITIAL_EXECUTORS}"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.executor.cores=${EXECUTOR_CORES}"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.executor.memory=${EXECUTOR_MEMORY}"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.driver.cores=${DRIVER_CORES}"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.driver.memory=${DRIVER_MEMORY}"

# Performance optimizations for Dataproc Serverless
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.serializer=org.apache.spark.serializer.KryoSerializer"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.speculation=true"
SPARK_PROPERTIES="${SPARK_PROPERTIES},spark.kryoserializer.buffer.max=512m"

GCLOUD_CMD="gcloud dataproc batches submit pyspark $SCRIPT_GCS_PATH \
  --batch=$BATCH_ID \
  --project=$PROJECT_ID \
  --region=$REGION \
  --py-files=$DEPENDENCIES_GCS_PATH \
  --properties=${SPARK_PROPERTIES} \
  --async"

# Add optional network configuration
if [[ -n "$SUBNET" ]]; then
  GCLOUD_CMD="$GCLOUD_CMD --subnet=$SUBNET"
fi

if [[ -n "$SERVICE_ACCOUNT" ]]; then
  GCLOUD_CMD="$GCLOUD_CMD --service-account=$SERVICE_ACCOUNT"
fi

# Add application arguments
GCLOUD_CMD="$GCLOUD_CMD -- \
  --project-id $PROJECT_ID \
  --dataset-id $DATASET_ID \
  --table-suffix $TABLE_SUFFIX \
  --total-records $TOTAL_RECORDS \
  --unique-customers $UNIQUE_CUSTOMERS \
  --write-mode $WRITE_MODE \
  --partitions $PARTITIONS"

echo ""
echo "🔄 Submitting Dataproc Serverless batch job..."
echo "Batch ID: $BATCH_ID"
echo ""

# Execute the command
eval $GCLOUD_CMD

echo ""
echo "✅ Job submitted successfully!"
echo ""
echo "Monitor progress:"
echo "  gcloud dataproc batches describe $BATCH_ID --project=$PROJECT_ID --region=$REGION"
echo ""
echo "View logs:"
echo "  gcloud dataproc batches describe $BATCH_ID --project=$PROJECT_ID --region=$REGION --format='value(runtimeInfo.outputUri)'"
echo ""
echo "Cancel if needed:"
echo "  gcloud dataproc batches cancel $BATCH_ID --project=$PROJECT_ID --region=$REGION"
