# MDM Batch Processing Notebook

A comprehensive Jupyter notebook demonstrating end-to-end Master Data Management using BigQuery's native capabilities.

## 🎯 What This Notebook Demonstrates

This notebook implements the **Batch Processing Path** from the unified MDM architecture, showcasing:

- **100% BigQuery-Native Implementation**: All processing happens in BigQuery for maximum scalability
- **Latest AI Models**: Uses `gemini-embedding-001` for state-of-the-art embeddings
- **5-Strategy AI-Powered Matching**: Combines exact, fuzzy, vector, business rules, and AI natural language matching (uses `gemini-2.5-pro`)
- **Automated Decision Making**: Confidence-based workflows with auto-merge and human review
- **Production-Ready Patterns**: Scalable SQL patterns suitable for enterprise deployment

## 📊 Sample Data

The notebook generates **200+ realistic customer records** representing:
- **120 unique individuals** with intentional duplicates and variations
- **3 data sources**: CRM, ERP, and E-commerce systems
- **Realistic data quality issues**: typos, format differences, missing fields, name variations

## 🚀 Quick Start

### Prerequisites

1. **Google Cloud Project** with billing enabled
2. **APIs Enabled**:
   ```bash
   gcloud services enable bigquery.googleapis.com
   gcloud services enable aiplatform.googleapis.com
   ```
3. **Authentication** set up:
   ```bash
   gcloud auth application-default login
   ```
4. **Python 3.12** and **uv** package manager installed

## 🔧 Large Scale Data Generation (100M+ Records)

For enterprise-scale MDM demonstrations with 100M+ records, use the scalable data generator instead of the notebook's built-in data generation.

### When to Use Scalable Generator

**Use the notebook (Section 2) for:**
- ✅ Quick demos and learning (120 unique customers, ~284 records)
- ✅ Algorithm testing and development
- ✅ Small-scale proof of concepts

**Use the scalable generator for:**
- ✅ Enterprise-scale demos (25M+ customers, 100M+ records)
- ✅ Performance testing at scale
- ✅ Production workload simulation
- ✅ Realistic data complexity with cross-temporal relationships

### Scalable Generator Options

**Option 1: Traditional Batch Generator (Multi-processing)**
- **🚀 Fully-Parallel Architecture**: CPU-based parallelization
- **🧠 Memory-Efficient**: Processes data in chunks
- **Available in**: `batch_mdm_gcp/scalable_data_generator.py`

**Option 2: Spark Generator (Distributed)**
- **☁️ Serverless Scaling**: Auto-scaling via Dataproc Serverless
- **🔧 Enterprise Scale**: Supports billions of records
- **📦 Zero Setup**: Auto-dependency management
- **Available in**: `batch_mdm_gcp/spark_data_generator/`

Both generators are **notebook-compatible** and create the same table structure.

### Basic Usage

#### 1. **Standard Generation (First Run)**
This command generates ~100 million records, split across three tables, using all available CPU cores. It will **overwrite** the tables on the first run.

```bash
uv run python batch_mdm_gcp/scalable_data_generator.py \
    --project-id YOUR_PROJECT_ID \
    --dataset-id mdm_demo \
    --table-suffix _scale \
    --total-records 100000000
```

#### 2. **Appending a Second Batch (e.g., for 200M total)**
To add another 100 million records, use the `--write-disposition WRITE_APPEND` flag. This will append data to the existing `_scale` tables.

```bash
uv run python batch_mdm_gcp/scalable_data_generator.py \
    --project-id YOUR_PROJECT_ID \
    --dataset-id mdm_demo \
    --table-suffix _scale \
    --total-records 100000000 \
    --write-disposition WRITE_APPEND
```

#### 3. **Custom Scale & Performance Tuning**
You can tune the generator for your specific machine and needs. For a powerful machine, a larger chunk size is often more efficient as it reduces the number of BigQuery load jobs.

```bash
# Generate 50M records using 16 workers and a larger chunk size
uv run python batch_mdm_gcp/scalable_data_generator.py \
    --project-id YOUR_PROJECT_ID \
    --dataset-id mdm_demo \
    --table-suffix _scale \
    --total-records 50000000 \
    --unique-customers 12500000 \
    --chunk-size 100000 \
    --num-workers 16
```

### Integration with Notebook Workflow

The new generator is designed to work seamlessly with the notebook.

1.  **Run the Generator**: Use the commands above to populate your `_scale` tables in BigQuery.

2.  **Update Notebook Configuration**: In the notebook's first setup cell, ensure the `PROJECT_ID` and `DATASET_ID` match what you used in the generator.

3.  **Modify the `generate_union_sql` call in Section 4**: This is the only change needed in the notebook. Tell the function to look for the tables with the `_scale` suffix.
    ```python
    # In Section 4 of the notebook
    combine_sql = generate_union_sql(bq_helper.dataset_ref, table_suffix="_scale")
    ```

4.  **Run the Rest of the Notebook**: The notebook will now correctly find and `UNION` the large-scale tables, and the entire pipeline will run on your 100M+ record dataset.

### Integration with Notebook Workflow

After running the scalable generator, **skip to Section 4** of the notebook:

#### **Option A: Align Generator with Notebook (Recommended)**

1. **Run generator with notebook-compatible parameters:**
   ```bash
   uv run python batch_mdm_gcp/scalable_data_generator.py \
       --project-id YOUR_PROJECT_ID \
       --dataset-id mdm_demo \
       --total-records 100000000
   ```

2. **Update notebook configuration (Section 1):**
   ```python
   PROJECT_ID = "YOUR_PROJECT_ID"  # Your actual project ID
   DATASET_ID = "mdm_demo"        # Matches generator
   ```

3. **Skip to Section 4** and update table references:
   ```python
   # In Section 4, update table names to include _scale suffix
   combine_sql = generate_union_sql(bq_helper.dataset_ref, table_suffix="_scale")
   ```

#### **Option B: Use Generator Defaults**

1. **Run generator with defaults:**
   ```bash
   uv run python batch_mdm_gcp/scalable_data_generator.py \
       --project-id YOUR_PROJECT_ID \
       --total-records 100000000
   ```

2. **Update notebook configuration:**
   ```python
   PROJECT_ID = "YOUR_PROJECT_ID"
   DATASET_ID = "mdm_demo_scale"  # Matches generator default
   ```

3. **Skip to Section 4** (table names will match: `raw_*_customers_scale`)

### Generated Table Structure

The scalable generator creates these tables automatically:

| Table Name | Description | Records (100M total) |
|------------|-------------|---------------------|
| `raw_crm_customers_scale` | CRM system data | ~80M (80% of 100M) |
| `raw_erp_customers_scale` | ERP system data | ~70M (70% of 100M) |
| `raw_ecommerce_customers_scale` | E-commerce data | ~60M (60% of 100M) |

**Note**: Overlap between systems creates realistic multi-source scenarios.

### Performance Optimization

The scalable generator uses **multi-processing** for significant performance improvements:

#### **Multi-Core Utilization**
- **Parallel Processing**: Utilizes all available CPU cores for both customer pool generation and final data chunk creation
- **Performance gain**: **4-16x faster** depending on system cores
- **Memory-efficient**: Processes data in manageable chunks to keep RAM usage low

#### **Available Command-Line Options**

```bash
uv run python batch_mdm_gcp/scalable_data_generator.py --help
```

**Key Parameters:**
- `--total-records`: Total number of records to generate (default: 100M)
- `--unique-customers`: Number of unique customers (default: 25M)
- `--chunk-size`: Records per processing chunk (default: 10K)
- `--num-workers`: Parallel worker processes (default: CPU count)
- `--write-disposition`: WRITE_TRUNCATE or WRITE_APPEND

#### **Performance Tuning**

For optimal performance:

```bash
# Use larger chunks on powerful systems
uv run python batch_mdm_gcp/scalable_data_generator.py \
    --project-id YOUR_PROJECT_ID \
    --chunk-size 100000 \
    --num-workers 16

# Reduce load on smaller systems
uv run python batch_mdm_gcp/scalable_data_generator.py \
    --project-id YOUR_PROJECT_ID \
    --chunk-size 10000 \
    --num-workers 4
```

### Troubleshooting

#### **Memory Issues**
- Reduce `--chunk-size` to lower memory usage per worker
- Reduce `--num-workers` if system becomes unresponsive
- Ensure sufficient disk space in `/tmp` for temporary files

#### **Performance Issues**
- Too many workers can cause CPU contention - try reducing `--num-workers`
- Large chunk sizes improve BigQuery loading efficiency but use more memory
- Monitor system resources during generation

#### **BigQuery Issues**
- Check BigQuery quotas and limits in GCP Console
- Verify dataset and table permissions
- Ensure BigQuery API is enabled in your project

### Setup Steps

1. **Install dependencies** (from project root):
   ```bash
   uv sync
   ```

2. **Configure the notebook**:
   - Open `mdm_batch_processing.ipynb`
   - Update these variables in the configuration cell:
     ```python
     PROJECT_ID = "your-gcp-project-id"  # Your GCP project ID
     ```

3. **Run the notebook**:
   ```bash
   # From the project root
   jupyter lab batch_mdm_gcp/mdm_batch_processing.ipynb
   ```

## 📋 Pipeline Steps

The notebook walks through each step of the MDM pipeline:

### 1. **Setup & Configuration**
- Import libraries and configure GCP connection
- Initialize BigQuery helper classes

### 2. **Sample Data Generation**
- Generate realistic customer data with duplicates
- Create variations (typos, format differences, missing data)
- Load data from 3 different source systems

### 3. **Data Ingestion to BigQuery**
- Create BigQuery dataset and tables
- Load raw data from each source system
- Verify data loading and table statistics

### 4. **Data Standardization**
- Clean and normalize data using SQL
- Standardize names, emails, phones, addresses
- Apply consistent formatting rules

### 5. **Embedding Generation**
- Create BigQuery ML model using `gemini-embedding-001`
- Generate semantic embeddings for entity matching
- Combine multiple fields into composite embeddings

### 6. **Vector Index Creation**
- Create vector indexes for fast similarity search
- Optimize for cosine distance calculations
- Enable efficient large-scale matching

### 7. **Entity Matching**
- **Exact Matching**: Direct field comparison (email, phone, ID)
- **Fuzzy Matching**: String similarity (edit distance, soundex, tokens)
- **Vector Matching**: Semantic similarity using embeddings
- **Business Rules**: Domain-specific logic (company, location, age)
- **AI Natural Language**: Direct AI comparison using Gemini 2.5 Pro

### 7.5. **AI Model Setup**
- Create Gemini 2.5 Pro model for natural language matching
- Configure BigQuery ML remote model with Vertex AI connection
- Implement AI-powered entity comparison with explanations
- Optimize API calls with LIMIT 500 for cost control

### 8. **Combined Scoring**
- Weighted combination of all matching strategies
- Confidence score calculation
- Automated decision making (auto-merge vs. human review)

### 9. **Golden Record Creation**
- **Entity Clustering**: Use transitive closure to group all connected matching records
- **Survivorship Rules**: Apply data quality rules to select best values within each cluster
- **Master Entity Creation**: Generate one golden record per unique entity (not per source record)
- **Source Lineage**: Track all contributing source records and systems

### 10. **Analysis & Visualization**
- Pipeline performance metrics
- Data quality assessments
- Matching strategy effectiveness analysis
- Interactive visualizations with Plotly

## 🎛️ Configuration Options

### Matching Strategy Weights

Adjust the importance of each matching strategy:

```python
weights = {
    'exact': 0.30,     # Exact field matches (highest priority)
    'fuzzy': 0.25,     # String similarity
    'vector': 0.20,    # Semantic similarity
    'business': 0.15,  # Domain-specific rules
    'ai': 0.10         # AI natural language reasoning
}
```

### Confidence Thresholds

Control automated decision making:

```python
high_confidence_threshold = 0.9    # Auto-merge threshold
medium_confidence_threshold = 0.7  # Human review threshold
```

### Embedding Configuration

Choose between performance and accuracy:

```sql
-- High performance (768 dimensions)
768 AS output_dimensionality

-- Maximum accuracy (3072 dimensions)
3072 AS output_dimensionality
```

## 🔧 Customization Guide

### Adding New Data Sources

1. **Extend the data generator**:
   ```python
   # In batch_mdm_gcp/data_generator.py
   def generate_new_source_data(self) -> pd.DataFrame:
       # Add your custom data generation logic
       pass
   ```

2. **Update the ingestion process**:
   ```python
   # Add to the notebook
   new_source_df = generator.generate_new_source_data()
   bq_helper.load_dataframe_to_table(new_source_df, "raw_new_source_customers")
   ```

### Custom Matching Rules

Add domain-specific business rules:

```python
def custom_business_rule(record1, record2):
    """Custom matching logic for your domain"""
    if record1.get('industry') == record2.get('industry'):
        return 0.2  # Industry match boost
    return 0.0

# Add to BusinessRulesMatcher
business_matcher.add_rule(custom_business_rule, weight=1.0)
```

### Survivorship Rules

Customize how golden records are created:

```sql
-- Modify the golden record SQL to change survivorship logic
-- Example: Prefer most recent data over most complete
ARRAY_AGG(email_clean ORDER BY processed_at DESC LIMIT 1)[OFFSET(0)] as master_email
```

## 📊 Expected Results

After running the complete notebook, you should see:

### Data Processing
- **284 raw records** loaded from 3 sources (CRM: 105, ERP: 84, E-commerce: 95)
- **284 standardized records** (100% data completeness achieved)
- **Multiple potential matches** identified across all strategies
- **~120 golden records** created (57.7% deduplication rate)

### Matching Effectiveness
- **Exact matches**: 86 matches (Email: 17, Phone: 69, ID: 0)
- **Fuzzy matches**: 73 matches (Name: 0.752, Address: 0.700 avg scores)
- **Vector matches**: 42 matches (Avg similarity: 0.740)
- **Business rules**: 76 matches (Location: 76, Company/Age/Income: 0)
- **AI Natural Language**: 22 matches (Avg score: 0.677, Confidence: 0.816)

### Performance Metrics
- **Data completeness**: 100% across key fields (email, phone, address)
- **Auto-merge rate**: ~35% of identified matches
- **Human review rate**: ~28% of identified matches
- **Processing time**: 5-10 minutes for sample dataset
- **Deduplication effectiveness**: 57.7% reduction (284→120 records)

> 📊 **For detailed results, visualizations, and demo materials, see [`MDM_BATCH_RESULTS.md`](./MDM_BATCH_RESULTS.md)**

## 🔍 Troubleshooting

### Common Issues

#### 1. **Vertex AI Connection Errors**
```
Error: Connection not found or insufficient permissions
```

**Solutions**:
- Verify connection exists: `bq show --connection vertex-ai-connection`
- Check IAM permissions for the connection service account
- Ensure Vertex AI API is enabled

#### 2. **Embedding Generation Failures**
```
Error: Quota exceeded or model not available
```

**Solutions**:
- Check API quotas in GCP Console
- Verify model endpoint: `gemini-embedding-001`
- Reduce batch size if hitting rate limits

#### 3. **Vector Index Creation Issues**
```
Error: Vector indexes not supported in this region/edition
```

**Solutions**:
- Vector indexes require specific BigQuery editions
- Check regional availability
- The notebook will continue without indexes (slower performance)

#### 4. **Memory/Performance Issues**
```
Error: Query exceeded resource limits
```

**Solutions**:
- Reduce dataset size for testing
- Use `LIMIT` clauses during development
- Consider BigQuery slot reservations for large datasets

### Performance Optimization

#### For Large Datasets (>100K records)

1. **Use approximate functions**:
   ```sql
   -- Replace exact counts with approximations
   APPROX_COUNT_DISTINCT(email) instead of COUNT(DISTINCT email)
   ```

2. **Implement incremental processing**:
   ```sql
   -- Process only new/changed records
   WHERE processed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
   ```

3. **Partition tables**:
   ```sql
   -- Partition by date for better performance
   PARTITION BY DATE(processed_at)
   ```

4. **Use clustering**:
   ```sql
   -- Cluster on matching keys
   CLUSTER BY source_system, email_clean, phone_clean
   ```

## 📈 Scaling to Production

### Infrastructure Considerations

1. **BigQuery Slots**:
   - Consider reserved slots for predictable workloads
   - Monitor slot usage during peak processing

2. **Data Governance**:
   - Implement row-level security for sensitive data
   - Set up audit logging for compliance

3. **Monitoring**:
   - Create dashboards for pipeline health
   - Set up alerts for data quality issues

### Operational Patterns

1. **Incremental Processing**:
   - Process only changed records
   - Maintain change data capture (CDC) from source systems

2. **Human Review Workflow**:
   - Build approval interfaces for medium-confidence matches
   - Implement feedback loops to improve matching accuracy

3. **Data Quality Monitoring**:
   - Track completeness and accuracy metrics over time
   - Alert on significant quality degradation

## 🔗 Related Resources

### **Streaming Processing Documentation**
- **[MDM Streaming Processing Guide](../streaming_mdm_gcp/MDM_STREAMING_PROCESSING.md)** - Real-time 4-way matching with Spanner
- **[Streaming Processing Notebook](../streaming_mdm_gcp/streaming_mdm_processing.ipynb)** - Interactive streaming implementation
- **[Streaming MDM Utilities](../streaming_mdm_gcp/spanner_utils.py)** - Optimized Spanner helper functions

### **Batch Processing Resources**
- **Demo Results & Visualizations**: [`MDM_BATCH_RESULTS.md`](./MDM_BATCH_RESULTS.md) - Comprehensive results analysis with Mermaid diagrams and demo scripts
- **[Batch Processing Notebook](./mdm_batch_processing.ipynb)** - Interactive 5-way matching implementation
- **Source Code**: `batch_mdm_gcp/` - Reusable utility modules

### **Architecture & Design**
- **Main Project README**: `../README.md` - Overall MDM architecture and unified approach
- **Architecture Diagrams**: `../images/` - Visual architecture references
- **[Unified Implementation Guide](../mdm_unified_implementation.md)** - Cross-platform strategy

### External Documentation

- [BigQuery ML Documentation](https://cloud.google.com/bigquery-ml/docs)
- [Vertex AI Embeddings](https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings)
- [BigQuery Vector Search](https://cloud.google.com/bigquery/docs/vector-search-intro)
- [BigQuery Data Quality](https://cloud.google.com/bigquery/docs/data-quality-overview)
---

**Happy Data Mastering! 🎯**
