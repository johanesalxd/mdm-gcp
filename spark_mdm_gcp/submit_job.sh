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
TOTAL_RECORDS=100000000
UNIQUE_CUSTOMERS=25000000
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
      echo "  --unique-customers   Unique customers (default: 25M)"
      echo "  --write-mode         overwrite|append (default: overwrite)"
      echo "  --partitions         Spark partitions (default: 1000)"
      echo "  --region             GCP region (default: us-central1)"
      echo "  --subnet             VPC subnet (optional)"
      echo "  --service-account    Service account email (optional)"
      echo ""
      echo "Examples:"
      echo "  # Basic 100M record generation"
      echo "  $0 --project-id my-project"
      echo ""
      echo "  # Small test run"
      echo "  $0 --project-id my-project --total-records 1000000 --unique-customers 250000"
      echo ""
      echo "  # Large scale with custom settings"
      echo "  $0 --project-id my-project --total-records 1000000000 --partitions 5000"
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

# Calculate expected record counts for display
EXPECTED_CRM=$(echo "scale=0; $TOTAL_RECORDS * 0.8 * 1.15" | bc)
EXPECTED_ERP=$(echo "scale=0; $TOTAL_RECORDS * 0.7" | bc)
EXPECTED_ECOMMERCE=$(echo "scale=0; $TOTAL_RECORDS * 0.6 * 1.3" | bc)

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

# Build the gcloud dataproc batches submit pyspark command
BATCH_ID="mdm-data-gen-$(date +%Y%m%d-%H%M%S)"

GCLOUD_CMD="gcloud dataproc batches submit pyspark $SCRIPT_GCS_PATH \
  --batch=$BATCH_ID \
  --project=$PROJECT_ID \
  --region=$REGION \
  --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar \
  --properties=spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true"

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
