# PySpark MDM Data Generator

Enterprise-scale MDM data generation using PySpark and Dataproc Serverless. Solves multiprocessing limitations while preserving sophisticated data generation logic.

## Prerequisites

```bash
gcloud services enable dataproc.googleapis.com bigquery.googleapis.com compute.googleapis.com
gcloud auth application-default login
```

## Quick Start

### Basic Usage
```bash
cd batch_mdm_gcp/spark_data_generator

# Test run (1M records)
./submit_job.sh --project-id YOUR_PROJECT_ID --total-records 1000000

# Standard scale (100M records)
./submit_job.sh --project-id YOUR_PROJECT_ID --total-records 100000000

# Enterprise scale (1B records)
./submit_job.sh --project-id YOUR_PROJECT_ID --total-records 1000000000 --partitions 5000
```

### Features
- **Zero setup**: Auto-creates dependencies from `requirements.txt`
- **Smart defaults**: Auto-calculates 25% customer ratio if not specified
- **Async execution**: Script exits immediately after job submission
- **Clean schemas**: Separate optimized tables per source system

## Command Reference

### Core Parameters
```bash
--project-id PROJECT_ID          # Required: GCP project
--dataset-id DATASET            # BigQuery dataset (default: mdm_demo)
--total-records COUNT           # Total records to generate
--unique-customers COUNT        # Unique customers (auto-calculated: total ÷ 4)
--partitions COUNT              # Spark partitions (default: 1000)
--write-mode overwrite|append   # Table write behavior
--table-suffix SUFFIX           # Table name suffix (default: _scale)
```

### Advanced Options
```bash
--region REGION                 # GCP region (default: us-central1)
--subnet SUBNET_PATH           # VPC subnet (optional)
--service-account EMAIL        # Service account (optional)
```

## Technical Implementation

### Architecture
The generator uses a two-stage distributed processing approach:

1. **Customer Pool Generation**: Parallel generation of base customer data across Spark partitions
2. **Source Record Generation**: Apply source-specific coverage, duplication, and field variations

### Repartitioning Solution
To prevent BigQuery Storage Write API concurrency issues, the generator automatically repartitions data before writes:

```python
source_df.repartition(200).write.format("bigquery")...
```

This limits concurrent write streams from 1000 (default partitions) to 200, staying within API limits while maintaining performance.

### Performance Optimizations
Built-in Dataproc Serverless optimizations for reliability and speed:

- **Kryo Serialization**: Fast, efficient object serialization (`spark.serializer=org.apache.spark.serializer.KryoSerializer`)
- **Speculative Execution**: Automatic straggler task detection and backup execution (`spark.speculation=true`)
- **Buffer Management**: Increased Kryo buffer to 512MB to prevent overflow errors (`spark.kryoserializer.buffer.max=512m`)
- **Adaptive Query Execution**: Dynamic optimization of query plans and partition coalescing

### Schema Design
Each source system has a dedicated schema to avoid BigQuery compatibility issues:

- **CRM**: Base fields + `lead_source`, `sales_rep`, `deal_stage`
- **ERP**: Base fields + `account_number`, `credit_limit`, `payment_terms`, `account_status`
- **E-commerce**: Base fields + `username`, `total_orders`, `total_spent`, `preferred_category`, `marketing_opt_in`

### Data Generation Logic
Preserves all original sophisticated variations:
- **Coverage**: CRM (80%), ERP (70%), E-commerce (60%)
- **Duplication**: CRM (15% chance), ERP (none), E-commerce (up to 3 records)
- **Variations**: 10 name variations, 7 address formats, 5 phone formats
- **Data quality issues**: Typos (10%), missing data (15%), domain changes (20%)

### Scaling Control & Cost Management

The generator includes built-in scaling limits to prevent runaway costs while maintaining excellent performance:

#### **Default Scaling Limits**
- **Max Executors**: 150 (prevents cost explosion)
- **Executor Resources**: 4 cores, 8GB RAM each (4 cores is minimum for Dataproc Serverless)
- **Dynamic Allocation**: Auto-scales from 10 to 150 executors
- **Auto-cleanup**: Automatic when job completes

#### **Custom Scaling**
Modify these variables in `submit_job.sh` for different requirements:

```bash
# Cost-sensitive (small jobs)
MAX_EXECUTORS=50
EXECUTOR_MEMORY="8g"

# High-performance (large jobs)
MAX_EXECUTORS=300
EXECUTOR_MEMORY="16g"
```

### Performance Characteristics
| Records | Partitions | Max Executors | Approximate Time | Cost Estimate |
|---------|------------|---------------|------------------|---------------|
| 1M | 100 | 50 | 5-10 min | $2-5 |
| 100M | 1000 | 150 | 30-60 min | $20-50 |
| 1B | 5000 | 300 | 2-4 hours | $100-300 |

## Job Monitoring

```bash
# List jobs
gcloud dataproc batches list --project=PROJECT_ID --region=us-central1

# Monitor specific job
gcloud dataproc batches describe BATCH_ID --project=PROJECT_ID --region=us-central1

# Cancel job
gcloud dataproc batches cancel BATCH_ID --project=PROJECT_ID --region=us-central1
```

## Integration with Notebook

The generator creates tables compatible with the batch processing notebook:

### Generated Tables
- `raw_crm_customers_scale`
- `raw_erp_customers_scale`
- `raw_ecommerce_customers_scale`

### Notebook Configuration
1. Run the generator to populate tables
2. Update notebook configuration:
   ```python
   PROJECT_ID = "your-project-id"
   DATASET_ID = "mdm_demo"
   ```
3. Skip to Section 4 and update table references:
   ```python
   combine_sql = generate_union_sql(bq_helper.dataset_ref, table_suffix="_scale")
   ```

## Related Documentation

- [Batch Processing Guide](../MDM_BATCH_PROCESSING.md)
- [Batch Processing Notebook](../mdm_batch_processing.ipynb)
- [Streaming MDM Guide](../../streaming_mdm_gcp/MDM_STREAMING_PROCESSING.md)
- [Main Project README](../../README.md)
