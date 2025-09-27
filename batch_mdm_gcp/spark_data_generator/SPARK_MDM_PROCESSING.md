# PySpark MDM Data Generator

Enterprise-scale MDM data generation using PySpark and Dataproc Serverless, solving multiprocessing limitations while preserving all sophisticated data generation logic.

## 🎯 Why PySpark for MDM Data Generation?

The original multiprocessing approach encountered limitations at scale:
- **Worker process crashes** due to memory exhaustion
- **IPC deadlocks** when passing large data between processes
- **Limited scalability** constrained by single machine resources
- **Complex error handling** for distributed failures

The PySpark solution provides:
- ✅ **Automatic scaling** via Dataproc Serverless
- ✅ **Fault tolerance** with automatic task retry
- ✅ **Memory management** handled by Spark
- ✅ **Enterprise scale** supporting billions of records
- ✅ **Cost efficiency** with pay-per-use serverless model

## 🚀 Quick Start

### Prerequisites

1. **Google Cloud Project** with billing enabled
2. **Required APIs enabled**:
   ```bash
   gcloud services enable dataproc.googleapis.com
   gcloud services enable bigquery.googleapis.com
   gcloud services enable compute.googleapis.com
   ```
3. **Authentication** configured:
   ```bash
   gcloud auth application-default login
   ```

### Zero-Setup Usage ✨

The Spark MDM generator is **completely self-contained** - no manual dependency setup required!

#### 1. **Test Run (1M records, 250K customers auto-calculated)**
```bash
cd batch_mdm_gcp/spark_data_generator
./submit_job.sh --project-id YOUR_PROJECT_ID --total-records 1000000
```
**Features**: Script exits immediately, auto-creates dependencies, validates configuration

#### 2. **Standard Scale (100M records, 25M customers auto-calculated)**
```bash
./submit_job.sh --project-id YOUR_PROJECT_ID --total-records 100000000
```

#### 3. **Enterprise Scale (1B records with custom settings)**
```bash
./submit_job.sh \
    --project-id YOUR_PROJECT_ID \
    --total-records 1000000000 \
    --partitions 5000
```

### What Happens Automatically

✅ **Dependencies**: Auto-created from `requirements.txt` if missing
✅ **Configuration**: Smart defaults and validation (25% customer ratio)
✅ **Immediate Return**: Script exits after submission (--async mode)
✅ **Clean Schemas**: Separate optimized tables per source system
✅ **Error Prevention**: Validates impossible ratios and configurations

## 📊 Generated Data Structure

### Source System Coverage (Matches Original Logic)
- **CRM**: 80% of customers, 15% chance of duplicate records
- **ERP**: 70% of customers, always 1 record per customer
- **E-commerce**: 60% of customers, up to 3 records per customer

### Sophisticated Data Variations (Preserved Exactly)
- **10 name variations** (John→Jon, Michael→Mike, etc.)
- **7 address variations** (Street→St, Avenue→Ave, etc.)
- **5 phone formats** (original, dots, spaces, parentheses, etc.)
- **Email domain changes** (20% chance)
- **Realistic typos** (10% chance)
- **Missing data simulation** (15% chance)

### Source-Specific Fields
| Source | Fields Added |
|--------|-------------|
| **CRM** | `lead_source`, `sales_rep`, `deal_stage` |
| **ERP** | `account_number`, `credit_limit`, `payment_terms`, `account_status` |
| **E-commerce** | `username`, `total_orders`, `total_spent`, `preferred_category`, `marketing_opt_in` |

## 🏗️ Architecture Overview

### Two-Stage Parallel Processing

#### **Stage 1: Distributed Customer Pool Generation**
```
Spark Driver → RDD Partitions → Parallel Customer Generation → Cached DataFrame
     ↓              ↓                        ↓                      ↓
   Coordinate    Partition 1            Generate 25K            Consolidated
   workers      Partition 2         customers each             Customer Pool
                Partition N
```

#### **Stage 2: Source-Specific Record Generation**
```
Customer Pool → RDD Transformation → Source Records → Clean Schemas → BigQuery Tables
      ↓               ↓                     ↓             ↓              ↓
  Distributed    Apply Coverage       Add Variations  Separate      Direct Write
  Processing     & Duplication       & Source Fields  Schemas      (3 tables)
```

### Clean Schema Architecture ✨

**Problem Solved**: Original unified schema caused BigQuery compatibility issues

**Solution**: Separate optimized schemas per source system:

| Source | Schema Fields | Benefits |
|--------|--------------|----------|
| **CRM** | Base + `lead_source`, `sales_rep`, `deal_stage` | ✅ Clean, no null padding |
| **ERP** | Base + `account_number`, `credit_limit`, `payment_terms`, `account_status` | ✅ Source-specific only |
| **E-commerce** | Base + `username`, `total_orders`, `total_spent`, `preferred_category`, `marketing_opt_in` | ✅ Optimal performance |

**Result**: Zero BigQuery schema conflicts, faster writes, cleaner data structure

### Performance Advantages

| Aspect | Multiprocessing | PySpark |
|--------|----------------|---------|
| **Scale Limit** | ~10M records | 1B+ records |
| **Memory Usage** | High per worker | Distributed |
| **Fault Tolerance** | Manual retry | Automatic |
| **Scalability** | Single machine | Auto-scaling |
| **Resource Management** | Manual tuning | Spark-managed |

## 📋 Command Reference

### Core Arguments
```bash
--project-id PROJECT_ID          # Required: Your GCP project
--dataset-id DATASET            # BigQuery dataset (default: mdm_demo)
--total-records COUNT           # Total records to generate
--unique-customers COUNT        # Number of unique customers (auto-calculated if omitted)
--partitions COUNT              # Spark partitions for parallelism
--write-mode overwrite|append   # Table write behavior
```

### Smart Configuration Features ✨

**Auto-Calculation**: When `--unique-customers` is omitted, the script automatically calculates:
```bash
unique_customers = total_records ÷ 4    # 25% ratio for realistic MDM scenarios
```

**Intelligent Validation**:
- ✅ Prevents unique customers > total records (impossible scenario)
- ⚠️  Warns when ratio > 50% (unrealistic for MDM)
- 📊 Shows expected output volumes

**Immediate Return**: Script exits after successful submission (--async mode)

### Performance Tuning
```bash
--partitions 1000              # Default for 100M records
--partitions 5000              # Recommended for 1B+ records
--partitions 100               # Lower for small datasets
```

### Advanced Configuration
```bash
--region us-central1           # GCP region for processing
--subnet projects/.../subnets/... # VPC subnet (optional)
--service-account EMAIL       # Custom service account
```

## 📦 Dependency Management

### Requirements.txt Approach

**Professional Standard**: Uses `requirements.txt` for scalable dependency management

```txt
# batch_mdm_gcp/spark_data_generator/requirements.txt
faker==19.12.0
# Add future dependencies here:
# pandas==2.1.3
# numpy==1.24.3
```

### Automatic Setup ✨

**No Manual Steps Required**:
- Script auto-detects missing `dependencies.zip`
- Reads `requirements.txt` and installs packages
- Creates and uploads dependency bundle
- Zero configuration for end users

**Benefits**:
- ✅ **Version Control**: Pin exact dependency versions
- ✅ **Scalability**: Easy to add new libraries
- ✅ **Reproducibility**: Consistent builds across environments
- ✅ **Professional**: Industry-standard Python practices

## 🔧 Configuration Examples

### Small Development Dataset (Auto-calculated customers)
```bash
./submit_job.sh \
    --project-id my-project \
    --total-records 100000 \
    --partitions 10
# Auto-calculates: 25,000 unique customers (25% ratio)
```

### Production Testing (Explicit customers)
```bash
./submit_job.sh \
    --project-id my-project \
    --total-records 500000000 \
    --unique-customers 125000000 \
    --partitions 2500 \
    --write-mode overwrite
```

### Quick Test with Smart Defaults
```bash
./submit_job.sh --project-id my-project --total-records 1000000
# Result: 1M records, 250K customers, immediate return, auto-dependencies
```

### Incremental Data Addition
```bash
./submit_job.sh \
    --project-id my-project \
    --total-records 100000000 \
    --write-mode append \
    --table-suffix "_batch2"
```

## 📈 Expected Performance

### Processing Times (Approximate)
| Records | Unique Customers | Partitions | Time | Cost Estimate |
|---------|------------------|------------|------|---------------|
| 1M | 250K | 100 | 5-10 min | $2-5 |
| 100M | 25M | 1000 | 30-60 min | $20-50 |
| 1B | 250M | 5000 | 2-4 hours | $100-300 |

### Output Volume
For 100M total records:
- **CRM**: ~92M records (80% × 1.15 duplication factor)
- **ERP**: ~70M records (70% × 1.0 duplication factor)
- **E-commerce**: ~78M records (60% × 1.3 duplication factor)

## 🔍 Monitoring and Troubleshooting

### Monitor Job Progress
```bash
# List all batch jobs
gcloud dataproc batches list --project=PROJECT_ID --region=us-central1

# Get specific job details
gcloud dataproc batches describe BATCH_ID --project=PROJECT_ID --region=us-central1

# View job logs
gcloud dataproc batches describe BATCH_ID \
    --project=PROJECT_ID \
    --region=us-central1 \
    --format='value(runtimeInfo.outputUri)'
```

### Cancel Running Job
```bash
gcloud dataproc batches cancel BATCH_ID --project=PROJECT_ID --region=us-central1
```

### Common Issues & Solutions ✨

#### **"Insufficient quota" errors**
```bash
ERROR: Insufficient 'CPUS' quota. Requested 12.0, available 0.0
```
**Solutions**:
- Check Compute Engine quotas in GCP Console
- Request quota increases for CPUs, IPs, or disks
- Try a different region with available quota
- Use fewer partitions: `--partitions 100`

#### **Configuration validation errors**
```bash
❌ Error: Unique customers (25000000) cannot exceed total records (1000000)
```
**Solutions**:
- **Auto-fix**: Remove `--unique-customers` to use auto-calculation
- **Manual fix**: Increase `--total-records` or decrease `--unique-customers`
- **Smart defaults**: Let the script calculate realistic ratios

#### **Dependency creation failures**
```bash
❌ Error: requirements.txt not found
```
**Solutions**:
- File exists: Check current directory has `requirements.txt`
- Missing file: Create with `echo "faker==19.12.0" > requirements.txt`
- Path issues: Run script from `spark_mdm_gcp/` directory

#### **BigQuery schema conflicts (SOLVED)**
```bash
BigQueryConnectorException$InvalidSchemaException: schema not compatible
```
**Status**: ✅ **Permanently Fixed** with clean schema architecture
- No longer occurs with separate source schemas
- Original unified schema approach eliminated

#### **Job monitoring after --async**
```bash
# Job submitted but how to monitor?
```
**Solutions**:
- **List jobs**: `gcloud dataproc batches list --project=PROJECT_ID --region=us-central1`
- **Get status**: Use batch ID from submission output
- **View logs**: Script provides monitoring commands after submission

#### **Transient BigQuery write errors**
```bash
BigQueryConnectorException: Could not create write-stream after multiple retries
```
**Status**: ✅ **Normal behavior** - not a code issue
- **Cause**: High concurrency (1000 partitions) hitting BigQuery rate limits
- **Outcome**: Spark automatically retries failed tasks
- **Action**: None required - job completes successfully
- **Reduce frequency**: Use `--partitions 500` for fewer concurrent writes

#### **Job timeout or slow performance**
- Increase partition count for better parallelism
- Check for data skew in customer distribution
- Consider using fewer unique customers for testing
- Monitor BigQuery quotas and slot usage

## 🔗 Integration with Notebook

The PySpark generator creates tables compatible with the batch processing notebook:

### Table Names Generated
- `raw_crm_customers_scale`
- `raw_erp_customers_scale`
- `raw_ecommerce_customers_scale`

### Notebook Integration
1. **Run the PySpark generator** to populate tables
2. **Update notebook configuration**:
   ```python
   PROJECT_ID = "your-project-id"
   DATASET_ID = "mdm_demo"  # Match generator dataset
   ```
3. **Skip to Section 4** in the notebook:
   ```python
   # Update to use _scale tables
   combine_sql = generate_union_sql(bq_helper.dataset_ref, table_suffix="_scale")
   ```

## 💡 Best Practices

### Development Workflow
1. **Start small**: Test with 1M records first
2. **Validate output**: Check data quality and table structure
3. **Scale gradually**: 1M → 10M → 100M → 1B
4. **Monitor costs**: Set up billing alerts

### Production Usage
1. **Use custom subnets** for network security
2. **Set service accounts** for precise permissions
3. **Enable audit logging** for compliance
4. **Schedule regular runs** for data refresh

### Cost Optimization
1. **Right-size partitions**: More partitions = more parallelism but higher overhead
2. **Choose regions wisely**: Some regions have lower Dataproc pricing
3. **Use preemptible instances**: For non-urgent batch jobs
4. **Clean up temp buckets**: Automatic cleanup after jobs

## 🆚 Comparison with Original

| Feature | Original Multiprocessing | PySpark Implementation |
|---------|-------------------------|----------------------|
| **Data Logic** | ✅ Sophisticated variations | ✅ **Identical** logic preserved |
| **Scalability** | ❌ Limited to single machine | ✅ **Unlimited** via Dataproc |
| **Reliability** | ❌ Process crashes | ✅ **Fault-tolerant** with retries |
| **Performance** | ❌ Memory bottlenecks | ✅ **Distributed** processing |
| **Cost** | ❌ Always-on infrastructure | ✅ **Pay-per-use** serverless |
| **Maintenance** | ❌ Manual scaling/tuning | ✅ **Fully managed** by GCP |

## 🔗 Related Resources

### **Batch Processing Documentation**
- **[Original Batch Processing Guide](../MDM_BATCH_PROCESSING.md)** - Multiprocessing implementation
- **[Batch Processing Notebook](../mdm_batch_processing.ipynb)** - Interactive MDM pipeline
- **[Demo Results & Analysis](../MDM_BATCH_RESULTS.md)** - Comprehensive results

### **Streaming Processing**
- **[Streaming MDM Guide](../../streaming_mdm_gcp/MDM_STREAMING_PROCESSING.md)** - Real-time processing
- **[Streaming Notebook](../../streaming_mdm_gcp/streaming_mdm_processing.ipynb)** - Interactive streaming

### **Architecture & Design**
- **[Main Project README](../../README.md)** - Overall MDM architecture
- **[Unified Implementation](../../mdm_unified_implementation.md)** - Cross-platform strategy

### **External Documentation**
- [Dataproc Serverless Documentation](https://cloud.google.com/dataproc-serverless)
- [BigQuery Spark Connector](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example)
- [PySpark Programming Guide](https://spark.apache.org/docs/latest/api/python/)

---

**Ready for Enterprise-Scale MDM! 🎯**
