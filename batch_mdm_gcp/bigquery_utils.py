"""
BigQuery Utilities for MDM
Helper functions for BigQuery operations and SQL query generation
"""

from typing import Dict, Optional

from google.cloud import bigquery
import pandas as pd


class BigQueryMDMHelper:
    """Helper class for BigQuery MDM operations"""

    def __init__(self, project_id: str, dataset_id: str = "mdm"):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dataset_ref = f"{project_id}.{dataset_id}"

    def create_dataset(self) -> None:
        """Create the MDM dataset if it doesn't exist"""
        dataset = bigquery.Dataset(self.dataset_ref)
        dataset.location = "US"
        dataset.description = "Master Data Management dataset"

        try:
            self.client.create_dataset(dataset, exists_ok=True)
            print(f"Dataset {self.dataset_ref} created or already exists")
        except Exception as e:
            print(f"Error creating dataset: {e}")

    def execute_query(self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> pd.DataFrame:
        """Execute a BigQuery SQL query and return results as DataFrame"""
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            # Check if the query returns data (SELECT) or is a DDL/DML statement
            if results.total_rows is not None and results.total_rows > 0:
                return results.to_dataframe()
            elif query.strip().upper().startswith(('SELECT', 'WITH')):
                # SELECT query with no results - return empty DataFrame
                return results.to_dataframe()
            else:
                # DDL/DML statement (CREATE, INSERT, UPDATE, DELETE) - return empty DataFrame
                return pd.DataFrame()
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str,
                                write_disposition: str = "WRITE_TRUNCATE") -> None:
        """Load a pandas DataFrame to BigQuery table"""
        table_ref = f"{self.dataset_ref}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True
        )

        try:
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config)
            job.result()  # Wait for the job to complete
            print(f"Loaded {len(df)} rows to {table_ref}")
        except Exception as e:
            print(f"Error loading data to {table_ref}: {e}")
            raise

    def get_table_info(self, table_name: str) -> Dict:
        """Get information about a BigQuery table"""
        table_ref = f"{self.dataset_ref}.{table_name}"
        try:
            table = self.client.get_table(table_ref)
            return {
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "schema": [{"name": field.name, "type": field.field_type} for field in table.schema],
                "created": table.created,
                "modified": table.modified
            }
        except Exception as e:
            print(f"Error getting table info for {table_ref}: {e}")
            return {}


def generate_standardization_sql(source_table: str, target_table: str) -> str:
    """Generate SQL for data standardization"""
    return f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT
      record_id,
      source_system,
      source_id,
      customer_id,

      -- Standardize names
      TRIM(UPPER(REGEXP_REPLACE(full_name, r'[^a-zA-Z\\s]', ''))) AS full_name_clean,
      TRIM(UPPER(first_name)) AS first_name_clean,
      TRIM(UPPER(last_name)) AS last_name_clean,

      -- Standardize email
      LOWER(TRIM(email)) AS email_clean,

      -- Standardize phone (digits only)
      REGEXP_REPLACE(phone, r'[^0-9]', '') AS phone_clean,

      -- Standardize address
      TRIM(UPPER(REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(address, r'\\bSTREET\\b', 'ST'),
              r'\\bAVENUE\\b', 'AVE'
            ),
            r'\\bBOULEVARD\\b', 'BLVD'
          ),
          r'\\bROAD\\b', 'RD'
        ),
        r'\\bDRIVE\\b', 'DR'
      ))) AS address_clean,

      TRIM(UPPER(city)) AS city_clean,
      TRIM(UPPER(state)) AS state_clean,
      zip_code,

      -- Keep original fields
      full_name,
      first_name,
      last_name,
      email,
      phone,
      address,
      city,
      state,
      date_of_birth,
      company,
      job_title,
      annual_income,
      customer_segment,
      registration_date,
      last_activity_date,
      is_active,

      -- Add processing metadata
      CURRENT_TIMESTAMP() AS processed_at
    FROM `{source_table}`
    WHERE full_name IS NOT NULL
      AND (email IS NOT NULL OR phone IS NOT NULL)
    """


def generate_union_sql(dataset_ref: str, table_suffix: str = "") -> str:
    """Generate SQL to combine all raw data sources with consistent schema"""
    suffix = f"_{table_suffix}" if table_suffix else ""
    return f"""
    CREATE OR REPLACE TABLE `{dataset_ref}.raw_customers_combined{suffix}` AS
    -- CRM data with standardized columns
    SELECT
      record_id,
      source_system,
      source_id,
      customer_id,
      first_name,
      last_name,
      full_name,
      email,
      phone,
      address,
      city,
      state,
      zip_code,
      date_of_birth,
      company,
      job_title,
      annual_income,
      customer_segment,
      registration_date,
      last_activity_date,
      is_active
    FROM `{dataset_ref}.raw_crm_customers{suffix}`

    UNION ALL

    -- ERP data with standardized columns
    SELECT
      record_id,
      source_system,
      source_id,
      customer_id,
      first_name,
      last_name,
      full_name,
      email,
      phone,
      address,
      city,
      state,
      zip_code,
      date_of_birth,
      company,
      job_title,
      annual_income,
      customer_segment,
      registration_date,
      last_activity_date,
      is_active
    FROM `{dataset_ref}.raw_erp_customers{suffix}`

    UNION ALL

    -- E-commerce data with standardized columns
    SELECT
      record_id,
      source_system,
      source_id,
      customer_id,
      first_name,
      last_name,
      full_name,
      email,
      phone,
      address,
      city,
      state,
      zip_code,
      date_of_birth,
      company,
      job_title,
      annual_income,
      customer_segment,
      registration_date,
      last_activity_date,
      is_active
    FROM `{dataset_ref}.raw_ecommerce_customers{suffix}`
    """


def generate_embedding_sql(source_table: str, target_table: str, model_name: str) -> str:
    """Generate SQL for creating embeddings"""
    return f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT *
    FROM ML.GENERATE_EMBEDDING(
      MODEL `{model_name}`,
      (SELECT
        CONCAT(
          IFNULL(full_name_clean, ''), ' ',
          IFNULL(email_clean, ''), ' ',
          IFNULL(address_clean, ''), ' ',
          IFNULL(city_clean, ''), ' ',
          IFNULL(company, '')
        ) AS content,
        *
      FROM `{source_table}`),
      STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type)
    )
    """


def generate_exact_matching_sql(table_name: str) -> str:
    """Generate SQL for exact matching"""
    return f"""
    CREATE OR REPLACE TABLE `{table_name}_exact_matches` AS
    WITH exact_matches AS (
      SELECT
        a.record_id AS record1_id,
        b.record_id AS record2_id,
        a.source_system AS source1,
        b.source_system AS source2,

        -- Email exact match
        CASE
          WHEN a.email_clean = b.email_clean AND a.email_clean IS NOT NULL
          THEN 1.0 ELSE 0.0
        END AS email_exact_score,

        -- Phone exact match
        CASE
          WHEN a.phone_clean = b.phone_clean AND a.phone_clean IS NOT NULL
          THEN 1.0 ELSE 0.0
        END AS phone_exact_score,

        -- Customer ID exact match
        CASE
          WHEN a.customer_id = b.customer_id AND a.customer_id IS NOT NULL
          THEN 1.0 ELSE 0.0
        END AS id_exact_score

      FROM `{table_name}` a
      CROSS JOIN `{table_name}` b
      WHERE a.record_id < b.record_id  -- Avoid duplicates and self-matches
    )
    SELECT
      *,
      GREATEST(email_exact_score, phone_exact_score, id_exact_score) AS exact_overall_score
    FROM exact_matches
    WHERE GREATEST(email_exact_score, phone_exact_score, id_exact_score) > 0
    """


def generate_fuzzy_matching_sql(table_name: str) -> str:
    """Generate SQL for fuzzy matching"""
    return f"""
    CREATE OR REPLACE TABLE `{table_name}_fuzzy_matches` AS
    WITH fuzzy_matches AS (
      SELECT
        a.record_id AS record1_id,
        b.record_id AS record2_id,
        a.source_system AS source1,
        b.source_system AS source2,

        -- Name fuzzy matching
        CASE
          WHEN a.full_name_clean IS NOT NULL AND b.full_name_clean IS NOT NULL
          THEN 1.0 - (EDIT_DISTANCE(a.full_name_clean, b.full_name_clean) /
                      GREATEST(LENGTH(a.full_name_clean), LENGTH(b.full_name_clean)))
          ELSE 0.0
        END AS name_edit_distance_score,

        -- Soundex matching for names
        CASE
          WHEN SOUNDEX(a.full_name_clean) = SOUNDEX(b.full_name_clean)
               AND a.full_name_clean IS NOT NULL
          THEN 0.8 ELSE 0.0
        END AS name_soundex_score,

        -- Address fuzzy matching
        CASE
          WHEN a.address_clean IS NOT NULL AND b.address_clean IS NOT NULL
          THEN 1.0 - (EDIT_DISTANCE(a.address_clean, b.address_clean) /
                      GREATEST(LENGTH(a.address_clean), LENGTH(b.address_clean)))
          ELSE 0.0
        END AS address_edit_distance_score,

        -- Token-based name matching
        CASE
          WHEN a.full_name_clean IS NOT NULL AND b.full_name_clean IS NOT NULL
          THEN (
            SELECT COUNT(*)
            FROM UNNEST(SPLIT(a.full_name_clean, ' ')) AS token_a
            WHERE token_a IN UNNEST(SPLIT(b.full_name_clean, ' '))
          ) / GREATEST(
            ARRAY_LENGTH(SPLIT(a.full_name_clean, ' ')),
            ARRAY_LENGTH(SPLIT(b.full_name_clean, ' '))
          )
          ELSE 0.0
        END AS name_token_score

      FROM `{table_name}` a
      CROSS JOIN `{table_name}` b
      WHERE a.record_id < b.record_id
    )
    SELECT
      *,
      GREATEST(name_edit_distance_score, name_soundex_score, name_token_score) AS name_fuzzy_score,
      address_edit_distance_score AS address_fuzzy_score,
      (GREATEST(name_edit_distance_score, name_soundex_score, name_token_score) +
       address_edit_distance_score) / 2 AS fuzzy_overall_score
    FROM fuzzy_matches
    WHERE GREATEST(name_edit_distance_score, name_soundex_score, name_token_score,
                   address_edit_distance_score) > 0.5
    """


def generate_vector_matching_sql(table_name: str) -> str:
    """Generate SQL for vector similarity matching"""
    return f"""
    CREATE OR REPLACE TABLE `{table_name}_vector_matches` AS
    SELECT
      a.record_id AS record1_id,
      b.record_id AS record2_id,
      a.source_system AS source1,
      b.source_system AS source2,

      -- Cosine similarity (convert distance to similarity)
      1 - COSINE_DISTANCE(a.ml_generate_embedding_result, b.ml_generate_embedding_result) AS vector_similarity_score

    FROM `{table_name}` a
    CROSS JOIN `{table_name}` b
    WHERE a.record_id < b.record_id
      AND a.ml_generate_embedding_result IS NOT NULL
      AND b.ml_generate_embedding_result IS NOT NULL
      AND COSINE_DISTANCE(a.ml_generate_embedding_result, b.ml_generate_embedding_result) < 0.3  -- Only similar records
    """


def generate_business_rules_sql(table_name: str) -> str:
    """Generate SQL for business rules matching"""
    return f"""
    CREATE OR REPLACE TABLE `{table_name}_business_matches` AS
    SELECT
      a.record_id AS record1_id,
      b.record_id AS record2_id,
      a.source_system AS source1,
      b.source_system AS source2,

      -- Same company rule
      CASE
        WHEN a.company = b.company AND a.company IS NOT NULL
        THEN 0.3 ELSE 0.0
      END AS same_company_score,

      -- Same location rule
      CASE
        WHEN a.city_clean = b.city_clean AND a.state_clean = b.state_clean
             AND a.city_clean IS NOT NULL
        THEN 0.2 ELSE 0.0
      END AS same_location_score,

      -- Age compatibility rule
      CASE
        WHEN ABS(DATE_DIFF(a.date_of_birth, b.date_of_birth, DAY)) <= 365
             AND a.date_of_birth IS NOT NULL AND b.date_of_birth IS NOT NULL
        THEN 0.4
        WHEN ABS(DATE_DIFF(a.date_of_birth, b.date_of_birth, DAY)) <= 1825
             AND a.date_of_birth IS NOT NULL AND b.date_of_birth IS NOT NULL
        THEN 0.2
        ELSE 0.0
      END AS age_compatibility_score,

      -- Income compatibility rule
      CASE
        WHEN a.annual_income > 0 AND b.annual_income > 0
        THEN CASE
          WHEN LEAST(a.annual_income, b.annual_income) / GREATEST(a.annual_income, b.annual_income) >= 0.8
          THEN 0.1 ELSE 0.0
        END
        ELSE 0.0
      END AS income_compatibility_score

    FROM `{table_name}` a
    CROSS JOIN `{table_name}` b
    WHERE a.record_id < b.record_id
    """


def generate_ai_natural_language_matching_sql(table_name: str, model_name: str) -> str:
    """Generate SQL for AI natural language matching using Gemini 2.5 Pro"""
    return f"""
    CREATE OR REPLACE TABLE `{table_name}_ai_natural_language_matches` AS
    WITH customer_pairs AS (
      SELECT
        a.record_id as record1_id,
        b.record_id as record2_id,
        a.source_system as source1,
        b.source_system as source2,
        a.full_name_clean as name1,
        a.email_clean as email1,
        a.phone_clean as phone1,
        a.address_clean as address1,
        b.full_name_clean as name2,
        b.email_clean as email2,
        b.phone_clean as phone2,
        b.address_clean as address2
      FROM `{table_name}` a
      CROSS JOIN `{table_name}` b
      WHERE a.record_id < b.record_id
      LIMIT 500  -- Start with small batch for testing
    ),
    ai_matches AS (
      SELECT
        record1_id,
        record2_id,
        source1,
        source2,
        ROUND(SAFE_CAST(similarity_score AS FLOAT64), 6) as similarity_score,
        ROUND(SAFE_CAST(confidence AS FLOAT64), 6) as confidence,
        explanation
      FROM AI.GENERATE_TABLE(
        MODEL `{model_name}`,
        (
          SELECT
            CONCAT(
              'Compare the similarity between the following two customer records and provide: ',
              '1. A similarity score (0-1). ',
              '2. A confidence score (0-1) for your assessment. ',
              '3. A brief explanation for your confidence level. ',
              'Record 1: Name: ', COALESCE(name1, ''),
              ', Email: ', COALESCE(email1, ''),
              ', Phone: ', COALESCE(phone1, ''),
              ', Address: ', COALESCE(address1, ''), '. ',
              'Record 2: Name: ', COALESCE(name2, ''),
              ', Email: ', COALESCE(email2, ''),
              ', Phone: ', COALESCE(phone2, ''),
              ', Address: ', COALESCE(address2, '')
            ) AS prompt,
            record1_id,
            record2_id,
            source1,
            source2
          FROM customer_pairs
        ),
        STRUCT(
          "similarity_score FLOAT64, confidence FLOAT64, explanation STRING"
          AS output_schema
        )
      )
      WHERE SAFE_CAST(similarity_score AS FLOAT64) > 0.4  -- Balanced threshold for ensemble
        AND SAFE_CAST(confidence AS FLOAT64) > 0.6        -- Ensure AI is confident
    )
    SELECT
      record1_id,
      record2_id,
      source1,
      source2,
      LEAST(GREATEST(COALESCE(similarity_score, 0.0), 0.0), 1.0) as ai_score,
      LEAST(GREATEST(COALESCE(confidence, 0.0), 0.0), 1.0) as confidence,
      explanation,
      CURRENT_TIMESTAMP() as processed_at
    FROM ai_matches
    WHERE similarity_score IS NOT NULL
      AND confidence IS NOT NULL
      AND similarity_score BETWEEN 0.0 AND 1.0
      AND confidence BETWEEN 0.0 AND 1.0
    """


def generate_combined_scoring_sql(dataset_ref: str, table_name: str) -> str:
    """Generate SQL for combining all match scores including AI natural language matching (5 strategies)"""
    return f"""
    CREATE OR REPLACE TABLE `{dataset_ref}.{table_name}_combined_matches` AS
    WITH all_pairs AS (
      SELECT DISTINCT record1_id, record2_id, source1, source2
      FROM (
        SELECT record1_id, record2_id, source1, source2 FROM `{dataset_ref}.{table_name}_exact_matches`
        UNION DISTINCT
        SELECT record1_id, record2_id, source1, source2 FROM `{dataset_ref}.{table_name}_fuzzy_matches`
        UNION DISTINCT
        SELECT record1_id, record2_id, source1, source2 FROM `{dataset_ref}.{table_name}_vector_matches`
        UNION DISTINCT
        SELECT record1_id, record2_id, source1, source2 FROM `{dataset_ref}.{table_name}_business_matches`
        UNION DISTINCT
        SELECT record1_id, record2_id, source1, source2 FROM `{dataset_ref}.{table_name}_ai_natural_language_matches`
      )
    ),
    combined_scores AS (
      SELECT
        p.record1_id,
        p.record2_id,
        p.source1,
        p.source2,

        -- Get scores from each matching strategy (5 strategies)
        COALESCE(e.exact_overall_score, 0.0) AS exact_score,
        COALESCE(f.fuzzy_overall_score, 0.0) AS fuzzy_score,
        COALESCE(v.vector_similarity_score, 0.0) AS vector_score,
        COALESCE(
          b.same_company_score + b.same_location_score +
          b.age_compatibility_score + b.income_compatibility_score, 0.0
        ) AS business_score,
        COALESCE(ai.ai_score, 0.0) AS ai_score,
        ai.explanation as ai_explanation,

        -- Calculate weighted combined score (5-strategy ensemble)
        -- Weights: Exact 30%, Fuzzy 25%, Vector 20%, Business 15%, AI 10%
        (0.30 * COALESCE(e.exact_overall_score, 0.0) +
         0.25 * COALESCE(f.fuzzy_overall_score, 0.0) +
         0.20 * COALESCE(v.vector_similarity_score, 0.0) +
         0.15 * COALESCE(
           b.same_company_score + b.same_location_score +
           b.age_compatibility_score + b.income_compatibility_score, 0.0
         ) +
         0.10 * COALESCE(ai.ai_score, 0.0)) AS combined_score

      FROM all_pairs p
      LEFT JOIN `{dataset_ref}.{table_name}_exact_matches` e
        ON p.record1_id = e.record1_id AND p.record2_id = e.record2_id
      LEFT JOIN `{dataset_ref}.{table_name}_fuzzy_matches` f
        ON p.record1_id = f.record1_id AND p.record2_id = f.record2_id
      LEFT JOIN `{dataset_ref}.{table_name}_vector_matches` v
        ON p.record1_id = v.record1_id AND p.record2_id = v.record2_id
      LEFT JOIN `{dataset_ref}.{table_name}_business_matches` b
        ON p.record1_id = b.record1_id AND p.record2_id = b.record2_id
      LEFT JOIN `{dataset_ref}.{table_name}_ai_natural_language_matches` ai
        ON p.record1_id = ai.record1_id AND p.record2_id = ai.record2_id
    )
    SELECT
      *,
      -- Calculate confidence and decision with adjusted thresholds
      CASE
        WHEN combined_score >= 0.8 THEN 'auto_merge'
        WHEN combined_score >= 0.6 THEN 'human_review'
        ELSE 'no_match'
      END AS match_decision,

      CASE
        WHEN combined_score >= 0.8 THEN 'high'
        WHEN combined_score >= 0.6 THEN 'medium'
        ELSE 'low'
      END AS confidence_level

    FROM combined_scores
    WHERE combined_score > 0.3  -- Lower threshold to include more potential matches
    ORDER BY combined_score DESC
    """


def generate_golden_record_sql(dataset_ref: str, table_name: str) -> str:
    """Generate SQL for creating golden records with proper entity clustering and deterministic IDs"""
    return f"""
    CREATE OR REPLACE TABLE `{dataset_ref}.golden_records` AS
    WITH
    -- Step 1: Get all matching pairs above threshold for clustering
    match_pairs AS (
      SELECT
        record1_id,
        record2_id,
        combined_score
      FROM `{dataset_ref}.{table_name}_combined_matches`
      WHERE match_decision IN ('auto_merge', 'human_review')
        AND combined_score >= 0.6  -- Minimum threshold for clustering
    ),

    -- Step 2: Create bidirectional edges for graph traversal
    edges AS (
      SELECT record1_id as node1, record2_id as node2 FROM match_pairs
      UNION ALL
      SELECT record2_id as node1, record1_id as node2 FROM match_pairs
    ),

    -- Step 3: Find connected components using iterative approach
    -- Start with each node as its own cluster
    initial_clusters AS (
      SELECT DISTINCT
        node1 as record_id,
        node1 as cluster_id
      FROM edges

      UNION DISTINCT

      -- Include unmatched records as their own clusters
      SELECT
        record_id,
        record_id as cluster_id
      FROM `{dataset_ref}.{table_name}`
      WHERE record_id NOT IN (
        SELECT node1 FROM edges
        UNION DISTINCT
        SELECT node2 FROM edges
      )
    ),

    -- Step 4: Propagate cluster IDs through connected components
    -- This implements a simplified transitive closure
    propagated_clusters AS (
      SELECT DISTINCT
        c1.record_id,
        MIN(LEAST(c1.cluster_id, c2.cluster_id)) OVER (
          PARTITION BY c1.record_id
        ) as final_cluster_id
      FROM initial_clusters c1
      LEFT JOIN edges e ON c1.record_id = e.node1
      LEFT JOIN initial_clusters c2 ON e.node2 = c2.record_id
    ),

    -- Step 5: Ensure all connected records have the same cluster ID
    -- by taking the minimum cluster ID for each connected component
    final_clusters AS (
      SELECT DISTINCT
        pc1.record_id,
        MIN(pc2.final_cluster_id) as cluster_id
      FROM propagated_clusters pc1
      LEFT JOIN edges e ON pc1.record_id = e.node1
      LEFT JOIN propagated_clusters pc2 ON e.node2 = pc2.record_id
      GROUP BY pc1.record_id

      UNION DISTINCT

      -- Include records with no connections
      SELECT
        record_id,
        final_cluster_id as cluster_id
      FROM propagated_clusters
      WHERE record_id NOT IN (SELECT node1 FROM edges)
    ),

    -- Step 6: Apply survivorship rules within each cluster
    golden_records_raw AS (
      SELECT
        fc.cluster_id,
        ARRAY_AGG(c.record_id ORDER BY c.processed_at DESC) as source_record_ids,

        -- Name: Most complete (longest)
        ARRAY_AGG(c.full_name_clean IGNORE NULLS ORDER BY LENGTH(c.full_name_clean) DESC LIMIT 1)[OFFSET(0)] as master_name,

        -- Email: Most recent and complete
        ARRAY_AGG(c.email_clean IGNORE NULLS ORDER BY c.processed_at DESC LIMIT 1)[OFFSET(0)] as master_email,

        -- Phone: Most recent and complete
        ARRAY_AGG(c.phone_clean IGNORE NULLS ORDER BY c.processed_at DESC LIMIT 1)[OFFSET(0)] as master_phone,

        -- Address: Most complete
        ARRAY_AGG(c.address_clean IGNORE NULLS ORDER BY LENGTH(c.address_clean) DESC LIMIT 1)[OFFSET(0)] as master_address,
        ARRAY_AGG(c.city_clean IGNORE NULLS ORDER BY LENGTH(c.city_clean) DESC LIMIT 1)[OFFSET(0)] as master_city,
        ARRAY_AGG(c.state_clean IGNORE NULLS ORDER BY LENGTH(c.state_clean) DESC LIMIT 1)[OFFSET(0)] as master_state,

        -- Company: Most recent
        ARRAY_AGG(c.company IGNORE NULLS ORDER BY c.processed_at DESC LIMIT 1)[OFFSET(0)] as master_company,

        -- Income: Maximum (assuming most recent/accurate)
        MAX(c.annual_income) as master_income,

        -- Segment: Most recent
        ARRAY_AGG(c.customer_segment IGNORE NULLS ORDER BY c.processed_at DESC LIMIT 1)[OFFSET(0)] as master_segment,

        -- Metadata
        COUNT(DISTINCT c.record_id) as source_record_count,
        ARRAY_AGG(DISTINCT c.source_system IGNORE NULLS ORDER BY c.source_system) as source_systems,
        MIN(c.registration_date) as first_seen,
        MAX(c.last_activity_date) as last_activity,

        -- Quality metrics
        MAX(CASE WHEN c.email_clean IS NOT NULL THEN 1 ELSE 0 END) as has_email,
        MAX(CASE WHEN c.phone_clean IS NOT NULL THEN 1 ELSE 0 END) as has_phone,
        MAX(CASE WHEN c.address_clean IS NOT NULL THEN 1 ELSE 0 END) as has_address,

        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at

      FROM final_clusters fc
      JOIN `{dataset_ref}.{table_name}` c ON fc.record_id = c.record_id
      GROUP BY fc.cluster_id
    ),

    -- Step 7: Generate deterministic entity IDs (matching streaming logic)
    golden_records AS (
      SELECT
        -- Generate deterministic master_id based on best identifier
        CASE
          -- If has email, use hash of email (primary identifier)
          WHEN master_email IS NOT NULL THEN
            SUBSTR(TO_HEX(SHA256(CONCAT('email:', master_email))), 1, 36)
          -- If has phone, use hash of phone (secondary identifier)
          WHEN master_phone IS NOT NULL THEN
            SUBSTR(TO_HEX(SHA256(CONCAT('phone:', master_phone))), 1, 36)
          -- Otherwise, use the cluster_id as fallback
          ELSE
            cluster_id
        END as master_id,

        -- All other fields remain the same
        source_record_ids,
        master_name,
        master_email,
        master_phone,
        master_address,
        master_city,
        master_state,
        master_company,
        master_income,
        master_segment,
        source_record_count,
        source_systems,
        first_seen,
        last_activity,
        has_email,
        has_phone,
        has_address,
        created_at,
        updated_at

      FROM golden_records_raw
    )

    SELECT * FROM golden_records
    ORDER BY source_record_count DESC, master_name
    """
