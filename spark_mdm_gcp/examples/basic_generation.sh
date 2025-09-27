#!/bin/bash

# Basic MDM Data Generation Example
# Generates 1M records for testing and development

# Set your project ID here
PROJECT_ID="your-project-id"

# Check if project ID is set
if [[ "$PROJECT_ID" == "your-project-id" ]]; then
  echo "❌ Error: Please set your PROJECT_ID in this script"
  echo "Edit line 6 and replace 'your-project-id' with your actual project ID"
  exit 1
fi

echo "🚀 Basic MDM Data Generation Example"
echo "Generating 1M records for development and testing..."

cd "$(dirname "$0")/.."

./submit_job.sh \
  --project-id "$PROJECT_ID" \
  --dataset-id "mdm_demo" \
  --total-records 1000000 \
  --unique-customers 250000 \
  --partitions 100

echo ""
echo "✅ Job submitted! This will generate approximately:"
echo "  • CRM: ~920K records"
echo "  • ERP: ~700K records"
echo "  • E-commerce: ~780K records"
echo ""
echo "Perfect for testing the notebook with realistic scale!"
