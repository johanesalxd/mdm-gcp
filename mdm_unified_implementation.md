# Unified MDM Implementation Guide: Batch + Streaming

This guide provides practical implementation examples for building a unified Master Data Management system that handles both batch and streaming data using GCP services.

## Architecture Overview

The unified MDM architecture supports two processing paths:
- **Batch Path**: Cost-effective, SQL-based processing using BigQuery
- **Stream Path**: Real-time processing using Kafka, Dataflow, and Vertex AI
- **Unified Matching**: Both paths converge at the matching layer

## 1. Generic Entity Schema Design

### Base Entity Table (Works for Any Domain)
```sql
CREATE TABLE `project.dataset.entities_raw` (
  source_system STRING NOT NULL,
  source_entity_id STRING NOT NULL,
  entity_type STRING NOT NULL, -- customer, product, supplier, etc.

  -- Core attributes (generic)
  entity_name STRING,
  entity_description TEXT,
  entity_category STRING,

  -- Contact information
  email STRING,
  phone STRING,
  address_line1 STRING,
  address_line2 STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  country STRING,

  -- Geographic coordinates
  latitude FLOAT64,
  longitude FLOAT64,

  -- Flexible attributes (JSON for domain-specific fields)
  attributes JSON,

  -- Metadata
  data_quality_score FLOAT64,
  last_updated TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (source_system, source_entity_id, entity_type) NOT ENFORCED
);
```

### Standardized Entity Table with Embeddings
```sql
CREATE TABLE `project.dataset.entities_embeddings` (
  entity_uuid STRING NOT NULL,
  source_system STRING NOT NULL,
  source_entity_id STRING NOT NULL,
  entity_type STRING NOT NULL,

  -- Standardized core fields
  entity_name_clean STRING,
  entity_description_combined STRING,
  entity_category_standardized STRING,

  -- Standardized contact
  email_normalized STRING,
  phone_normalized STRING,
  address_standardized STRING,
  coordinates GEOGRAPHY,

  -- Embeddings
  embedding VECTOR(768),

  -- Quality metrics
  completeness_score FLOAT64,
  confidence_score FLOAT64,

  -- Processing metadata
  processing_path STRING, -- 'batch' or 'stream'
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (entity_uuid) NOT ENFORCED
);
```

## 2. Batch Processing Implementation

### SQL-Based Data Standardization
```sql
-- Generic entity standardization pipeline
CREATE OR REPLACE TABLE `project.dataset.entities_standardized` AS
WITH cleaned_entities AS (
  SELECT
    GENERATE_UUID() as entity_uuid,
    source_system,
    source_entity_id,
    entity_type,

    -- Name standardization
    TRIM(UPPER(REGEXP_REPLACE(entity_name, r'[^\w\s]', ''))) as entity_name_clean,

    -- Email normalization
    LOWER(TRIM(email)) as email_normalized,

    -- Phone normalization (remove non-digits, format)
    REGEXP_REPLACE(phone, r'[^\d]', '') as phone_normalized,

    -- Address standardization
    CONCAT(
      COALESCE(address_line1, ''), ' ',
      COALESCE(address_line2, ''), ' ',
      COALESCE(city, ''), ' ',
      COALESCE(state, ''), ' ',
      COALESCE(postal_code, ''), ' ',
      COALESCE(country, '')
    ) as address_standardized,

    -- Geographic point
    CASE
      WHEN latitude IS NOT NULL AND longitude IS NOT NULL
      THEN ST_GEOGPOINT(longitude, latitude)
      ELSE NULL
    END as coordinates,

    -- Combined description for embedding
    CONCAT(
      COALESCE(entity_name, ''), ' ',
      COALESCE(entity_description, ''), ' ',
      COALESCE(entity_category, ''), ' ',
      COALESCE(address_line1, ''), ' ',
      COALESCE(city, '')
    ) as entity_description_combined,

    -- Completeness score
    (
      (IF(entity_name IS NOT NULL, 1, 0)) +
      (IF(email IS NOT NULL, 1, 0)) +
      (IF(phone IS NOT NULL, 1, 0)) +
      (IF(address_line1 IS NOT NULL, 1, 0)) +
      (IF(entity_description IS NOT NULL, 1, 0))
    ) / 5.0 as completeness_score,

    'batch' as processing_path

  FROM `project.dataset.entities_raw`
  WHERE entity_name IS NOT NULL
)
SELECT * FROM cleaned_entities;
```

### Python Batch Embedding Generation
```python
import pandas as pd
from google.cloud import bigquery
from vertexai.language_models import TextEmbeddingModel
import numpy as np
from typing import List, Dict, Any
import time
import logging

class UnifiedEntityEmbeddingGenerator:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko@003")

    def generate_embeddings_batch(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
        """Generate embeddings for a batch of texts"""
        embeddings = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            try:
                batch_embeddings = self.embedding_model.get_embeddings(batch)
                embeddings.extend([emb.values for emb in batch_embeddings])
                time.sleep(0.1)  # Rate limiting
                logging.info(f"Processed batch {i//batch_size + 1}")
            except Exception as e:
                logging.error(f"Error processing batch {i//batch_size}: {e}")
                # Add zero embeddings for failed batch
                embeddings.extend([[0.0] * 768 for _ in batch])

        return embeddings

    def process_entities_by_type(self, entity_type: str, limit: int = None):
        """Process entities of a specific type"""

        query = f"""
        SELECT
            entity_uuid,
            entity_description_combined,
            entity_type,
            source_system,
            source_entity_id,
            completeness_score
        FROM `{self.project_id}.{self.dataset_id}.entities_standardized`
        WHERE entity_type = '{entity_type}'
          AND entity_description_combined IS NOT NULL
        """

        if limit:
            query += f" LIMIT {limit}"

        df = self.client.query(query).to_dataframe()
        logging.info(f"Processing {len(df)} {entity_type} entities...")

        # Generate embeddings
        descriptions = df['entity_description_combined'].tolist()
        embeddings = self.generate_embeddings_batch(descriptions)

        # Prepare data for BigQuery
        rows_to_insert = []
        for idx, row in df.iterrows():
            rows_to_insert.append({
                'entity_uuid': row['entity_uuid'],
                'source_system': row['source_system'],
                'source_entity_id': row['source_entity_id'],
                'entity_type': row['entity_type'],
                'entity_description_combined': row['entity_description_combined'],
                'embedding': embeddings[idx],
                'completeness_score': row['completeness_score'],
                'processing_path': 'batch'
            })

        # Insert into BigQuery
        table_id = f"{self.project_id}.{self.dataset_id}.entities_embeddings"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )

        job = self.client.load_table_from_json(
            rows_to_insert, table_id, job_config=job_config
        )
        job.result()

        logging.info(f"Successfully inserted {len(rows_to_insert)} {entity_type} entities with embeddings")

# Usage examples for different domains
if __name__ == "__main__":
    generator = UnifiedEntityEmbeddingGenerator("your-project-id", "your-dataset-id")

    # Process different entity types
    generator.process_entities_by_type("customer", limit=1000)
    generator.process_entities_by_type("product", limit=1000)
    generator.process_entities_by_type("supplier", limit=1000)
```

## 3. Streaming Processing Implementation

### Kafka Consumer Setup
```python
from kafka import KafkaConsumer
import json
import logging
from typing import Dict, Any

class EntityKafkaConsumer:
    def __init__(self, kafka_servers: List[str], topic: str, group_id: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )

    def consume_entities(self) -> Dict[str, Any]:
        """Consume entity messages from Kafka"""
        for message in self.consumer:
            try:
                entity_data = message.value
                yield self.standardize_entity(entity_data)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                continue

    def standardize_entity(self, raw_entity: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize entity data in real-time"""
        return {
            'entity_uuid': raw_entity.get('id'),
            'source_system': raw_entity.get('source', 'unknown'),
            'entity_type': raw_entity.get('type', 'unknown'),
            'entity_name_clean': raw_entity.get('name', '').strip().upper(),
            'email_normalized': raw_entity.get('email', '').lower().strip(),
            'phone_normalized': ''.join(filter(str.isdigit, raw_entity.get('phone', ''))),
            'entity_description_combined': ' '.join([
                raw_entity.get('name', ''),
                raw_entity.get('description', ''),
                raw_entity.get('category', ''),
                raw_entity.get('address', '')
            ]).strip(),
            'processing_path': 'stream',
            'timestamp': message.timestamp
        }
```

### Apache Beam/Dataflow Pipeline
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from vertexai.language_models import TextEmbeddingModel
import json

class EntityStandardization(beam.DoFn):
    def process(self, element):
        """Standardize entity data"""
        # Parse JSON message
        entity = json.loads(element)

        # Standardization logic
        standardized = {
            'entity_uuid': entity.get('id'),
            'entity_name_clean': entity.get('name', '').strip().upper(),
            'email_normalized': entity.get('email', '').lower().strip(),
            'entity_description_combined': ' '.join([
                entity.get('name', ''),
                entity.get('description', ''),
                entity.get('category', '')
            ]).strip(),
            'processing_path': 'stream'
        }

        yield standardized

class GenerateEmbeddings(beam.DoFn):
    def setup(self):
        """Initialize embedding model"""
        self.embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko@003")

    def process(self, element):
        """Generate embeddings for entity"""
        try:
            description = element['entity_description_combined']
            if description:
                embeddings = self.embedding_model.get_embeddings([description])
                element['embedding'] = embeddings[0].values
            else:
                element['embedding'] = [0.0] * 768

            yield element
        except Exception as e:
            # Log error and yield element with zero embedding
            element['embedding'] = [0.0] * 768
            yield element

def run_streaming_pipeline():
    """Run the streaming Dataflow pipeline"""

    pipeline_options = PipelineOptions([
        '--project=your-project-id',
        '--region=us-central1',
        '--runner=DataflowRunner',
        '--streaming=true',
        '--temp_location=gs://your-bucket/temp',
        '--staging_location=gs://your-bucket/staging'
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from Kafka' >> beam.io.ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': 'your-kafka-servers',
                    'group.id': 'mdm-streaming-group'
                },
                topics=['entity-updates']
            )
            | 'Extract Values' >> beam.Map(lambda x: x[1])  # Get message value
            | 'Standardize Entities' >> beam.ParDo(EntityStandardization())
            | 'Generate Embeddings' >> beam.ParDo(GenerateEmbeddings())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table='your-project:your-dataset.entities_embeddings',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run_streaming_pipeline()
```

## 4. Unified Matching Implementation

### Generic Entity Matching Function
```sql
-- Generic entity matching function (works for any entity type)
CREATE OR REPLACE FUNCTION `project.dataset.find_similar_entities`(
  target_entity_uuid STRING,
  entity_type STRING,
  similarity_threshold FLOAT64 DEFAULT 0.85,
  max_results INT64 DEFAULT 10
)
RETURNS TABLE(
  entity_uuid STRING,
  source_system STRING,
  entity_name STRING,
  similarity_score FLOAT64,
  match_type STRING
)
AS (
  WITH target_entity AS (
    SELECT
      embedding,
      entity_name_clean,
      email_normalized,
      phone_normalized
    FROM `project.dataset.entities_embeddings`
    WHERE entity_uuid = target_entity_uuid
  )
  SELECT
    e.entity_uuid,
    e.source_system,
    e.entity_name_clean as entity_name,
    1 - COSINE_DISTANCE(e.embedding, t.embedding) as similarity_score,
    CASE
      WHEN e.email_normalized = t.email_normalized AND e.email_normalized IS NOT NULL THEN 'exact_email'
      WHEN e.phone_normalized = t.phone_normalized AND e.phone_normalized IS NOT NULL THEN 'exact_phone'
      WHEN 1 - COSINE_DISTANCE(e.embedding, t.embedding) >= similarity_threshold THEN 'vector_similarity'
      ELSE 'no_match'
    END as match_type
  FROM `project.dataset.entities_embeddings` e
  CROSS JOIN target_entity t
  WHERE e.entity_type = entity_type
    AND e.entity_uuid != target_entity_uuid
    AND (
      e.email_normalized = t.email_normalized
      OR e.phone_normalized = t.phone_normalized
      OR 1 - COSINE_DISTANCE(e.embedding, t.embedding) >= similarity_threshold
    )
  ORDER BY similarity_score DESC
  LIMIT max_results
);
```

### Comprehensive Matching with Business Rules
```sql
-- Multi-strategy entity matching
WITH entity_matches AS (
  SELECT
    e1.entity_uuid as entity1_uuid,
    e2.entity_uuid as entity2_uuid,
    e1.source_system as source1,
    e2.source_system as source2,
    e1.entity_type,

    -- Exact match scores
    CASE
      WHEN e1.email_normalized = e2.email_normalized AND e1.email_normalized IS NOT NULL THEN 1.0
      ELSE 0.0
    END as email_exact_score,

    CASE
      WHEN e1.phone_normalized = e2.phone_normalized AND e1.phone_normalized IS NOT NULL THEN 1.0
      ELSE 0.0
    END as phone_exact_score,

    -- Vector similarity score
    1 - COSINE_DISTANCE(e1.embedding, e2.embedding) as vector_similarity,

    -- Name similarity (using edit distance)
    1 - (EDIT_DISTANCE(e1.entity_name_clean, e2.entity_name_clean) /
         GREATEST(LENGTH(e1.entity_name_clean), LENGTH(e2.entity_name_clean), 1)) as name_similarity,

    -- Geographic proximity (if coordinates available)
    CASE
      WHEN e1.coordinates IS NOT NULL AND e2.coordinates IS NOT NULL THEN
        1 / (1 + ST_DISTANCE(e1.coordinates, e2.coordinates) / 1000) -- Distance in km
      ELSE 0.0
    END as geo_proximity_score

  FROM `project.dataset.entities_embeddings` e1
  JOIN `project.dataset.entities_embeddings` e2
    ON e1.entity_type = e2.entity_type
    AND e1.source_system != e2.source_system  -- Different sources
    AND e1.entity_uuid < e2.entity_uuid  -- Avoid duplicates
  WHERE 1 - COSINE_DISTANCE(e1.embedding, e2.embedding) >= 0.7  -- Pre-filter
),
scored_matches AS (
  SELECT
    *,
    -- Weighted composite score (adjust weights by domain)
    (
      email_exact_score * 0.3 +
      phone_exact_score * 0.25 +
      vector_similarity * 0.25 +
      name_similarity * 0.15 +
      geo_proximity_score * 0.05
    ) as composite_score
  FROM entity_matches
)
SELECT
  entity1_uuid,
  entity2_uuid,
  source1,
  source2,
  entity_type,
  email_exact_score,
  phone_exact_score,
  vector_similarity,
  name_similarity,
  composite_score,
  CASE
    WHEN composite_score >= 0.9 THEN 'HIGH'
    WHEN composite_score >= 0.7 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence_level
FROM scored_matches
WHERE composite_score >= 0.7
ORDER BY composite_score DESC;
```

## 5. Use Case Implementations

### Banking: Customer 360
```sql
-- Customer-specific matching rules
CREATE OR REPLACE TABLE `project.dataset.customer_matches` AS
WITH customer_specific_matches AS (
  SELECT
    e1.entity_uuid as customer1_uuid,
    e2.entity_uuid as customer2_uuid,

    -- Banking-specific exact matches
    CASE
      WHEN JSON_EXTRACT_SCALAR(e1.attributes, '$.ssn') =
           JSON_EXTRACT_SCALAR(e2.attributes, '$.ssn')
           AND JSON_EXTRACT_SCALAR(e1.attributes, '$.ssn') IS NOT NULL THEN 1.0
      ELSE 0.0
    END as ssn_match_score,

    CASE
      WHEN JSON_EXTRACT_SCALAR(e1.attributes, '$.account_number') =
           JSON_EXTRACT_SCALAR(e2.attributes, '$.account_number')
           AND JSON_EXTRACT_SCALAR(e1.attributes, '$.account_number') IS NOT NULL THEN 1.0
      ELSE 0.0
    END as account_match_score,

    -- Standard matching
    1 - COSINE_DISTANCE(e1.embedding, e2.embedding) as vector_similarity

  FROM `project.dataset.entities_embeddings` e1
  JOIN `project.dataset.entities_embeddings` e2
    ON e1.entity_type = 'customer'
    AND e2.entity_type = 'customer'
    AND e1.source_system != e2.source_system
    AND e1.entity_uuid < e2.entity_uuid
)
SELECT
  *,
  -- Banking-specific composite score
  (
    ssn_match_score * 0.4 +
    account_match_score * 0.3 +
    vector_similarity * 0.3
  ) as banking_composite_score
FROM customer_specific_matches
WHERE (ssn_match_score > 0 OR account_match_score > 0 OR vector_similarity >= 0.8);
```

### Retail: Product Catalog
```sql
-- Product-specific matching rules
CREATE OR REPLACE TABLE `project.dataset.product_matches` AS
WITH product_specific_matches AS (
  SELECT
    e1.entity_uuid as product1_uuid,
    e2.entity_uuid as product2_uuid,

    -- Product-specific matches
    CASE
      WHEN JSON_EXTRACT_SCALAR(e1.attributes, '$.sku') =
           JSON_EXTRACT_SCALAR(e2.attributes, '$.sku')
           AND JSON_EXTRACT_SCALAR(e1.attributes, '$.sku') IS NOT NULL THEN 1.0
      ELSE 0.0
    END as sku_match_score,

    CASE
      WHEN JSON_EXTRACT_SCALAR(e1.attributes, '$.upc') =
           JSON_EXTRACT_SCALAR(e2.attributes, '$.upc')
           AND JSON_EXTRACT_SCALAR(e1.attributes, '$.upc') IS NOT NULL THEN 1.0
      ELSE 0.0
    END as upc_match_score,

    -- Brand similarity
    CASE
      WHEN JSON_EXTRACT_SCALAR(e1.attributes, '$.brand') =
           JSON_EXTRACT_SCALAR(e2.attributes, '$.brand') THEN 1.0
      ELSE 0.0
    END as brand_match_score,

    -- Price similarity (within 10%)
    CASE
      WHEN ABS(CAST(JSON_EXTRACT_SCALAR(e1.attributes, '$.price') AS FLOAT64) -
               CAST(JSON_EXTRACT_SCALAR(e2.attributes, '$.price') AS FLOAT64)) /
           GREATEST(CAST(JSON_EXTRACT_SCALAR(e1.attributes, '$.price') AS FLOAT64),
                   CAST(JSON_EXTRACT_SCALAR(e2.attributes, '$.price') AS FLOAT64), 1) <= 0.1 THEN 1.0
      ELSE 0.0
    END as price_similarity_score,

    1 - COSINE_DISTANCE(e1.embedding, e2.embedding) as vector_similarity

  FROM `project.dataset.entities_embeddings` e1
  JOIN `project.dataset.entities_embeddings` e2
    ON e1.entity_type = 'product'
    AND e2.entity_type = 'product'
    AND e1.source_system != e2.source_system
    AND e1.entity_uuid < e2.entity_uuid
)
SELECT
  *,
  -- Product-specific composite score
  (
    sku_match_score * 0.3 +
    upc_match_score * 0.25 +
    brand_match_score * 0.2 +
    vector_similarity * 0.15 +
    price_similarity_score * 0.1
  ) as product_composite_score
FROM product_specific_matches
WHERE (sku_match_score > 0 OR upc_match_score > 0 OR vector_similarity >= 0.8);
```

## 6. Performance Optimization

### Batch vs Stream Performance Comparison
```sql
-- Performance metrics by processing path
WITH performance_metrics AS (
  SELECT
    processing_path,
    entity_type,
    COUNT(*) as total_entities,
    AVG(completeness_score) as avg_completeness,
    AVG(confidence_score) as avg_confidence,

    -- Processing time metrics (would need additional timestamp fields)
    AVG(TIMESTAMP_DIFF(processed_at, created_at, SECOND)) as avg_processing_time_seconds

  FROM `project.dataset.entities_embeddings`
  WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  GROUP BY processing_path, entity_type
)
SELECT
  processing_path,
  entity_type,
  total_entities,
  ROUND(avg_completeness, 3) as avg_completeness,
  ROUND(avg_confidence, 3) as avg_confidence,
  ROUND(avg_processing_time_seconds, 2) as avg_processing_time_seconds,

  -- Cost estimation (approximate)
  CASE
    WHEN processing_path = 'batch' THEN total_entities * 0.001  -- BigQuery cost
    WHEN processing_path = 'stream' THEN total_entities * 0.01  -- Dataflow + Vertex AI cost
  END as estimated_cost_usd

FROM performance_metrics
ORDER BY processing_path, entity_type;
```

### Monitoring and Alerting
```python
from google.cloud import monitoring_v3
import time

class MDMMonitoring:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = monitoring_v3.MetricServiceClient()

    def create_custom_metrics(self):
        """Create custom metrics for MDM monitoring"""

        # Entity processing rate
        descriptor = monitoring_v3.MetricDescriptor(
            type="custom.googleapis.com/mdm/entity_processing_rate",
            metric_kind=monitoring_v3.MetricDescriptor.MetricKind.GAUGE,
            value_type=monitoring_v3.MetricDescriptor.ValueType.DOUBLE,
            description="Rate of entity processing per minute"
        )

        project_name = f"projects/{self.project_id}"
        self.client.create_metric_descriptor(
            name=project_name,
            metric_descriptor=descriptor
        )

    def record_processing_metrics(self, entity_count: int, processing_path: str):
        """Record processing metrics"""

        series = monitoring_v3.TimeSeries()
        series.metric.type = "custom.googleapis.com/mdm/entity_processing_rate"
        series.metric.labels["processing_path"] = processing_path

        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": seconds, "nanos": nanos}}
        )
        point = monitoring_v3.Point(
            {"interval": interval, "value": {"double_value": entity_count}}
        )
        series.points = [point]

        project_name = f"projects/{self.project_id}"
        self.client.create_time_series(
            name=project_name,
            time_series=[series]
        )

# Usage
monitor = MDMMonitoring("your-project-id")
monitor.create_custom_metrics()
monitor.record_processing_metrics(1000, "batch")
```

This unified implementation guide provides a complete framework for building MDM systems that can handle both batch and streaming data, with practical examples for different industries and comprehensive monitoring capabilities.
