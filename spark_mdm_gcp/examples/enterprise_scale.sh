#!/bin/bash

# Enterprise Scale MDM Data Generation Example
# Generates 1B records for production-scale testing

# Set your project ID here
PROJECT_ID="your-project-id"

# Check if project ID is set
if [[ "$PROJECT_ID" == "your-project-id" ]]; then
  echo "❌ Error: Please set your PROJECT_ID in this script"
  echo "Edit line 6 and replace 'your-project-id' with your actual project ID"
  exit 1
fi

echo "🚀 Enterprise Scale MDM Data Generation"
echo "Generating 1B records for production-scale testing..."
echo ""
echo "⚠️  WARNING: This will generate a VERY large dataset!"
echo "   Estimated processing time: 2-4 hours"
echo "   Estimated cost: $50-200 (depending on region/cluster size)"
echo ""
read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Cancelled."
  exit 1
fi

cd "$(dirname "$0")/.."

./submit_job.sh \
  --project-id "$PROJECT_ID" \
  --dataset-id "mdm_demo" \
  --total-records 1000000000 \
  --unique-customers 250000000 \
  --partitions 5000 \
  --table-suffix "_enterprise"

echo ""
echo "✅ Enterprise-scale job submitted!"
echo ""
echo "This will generate approximately:"
echo "  • CRM: ~920M records"
echo "  • ERP: ~700M records"
echo "  • E-commerce: ~780M records"
echo ""
echo "Monitor progress with:"
echo "  gcloud dataproc batches list --project=$PROJECT_ID --region=us-central1"
