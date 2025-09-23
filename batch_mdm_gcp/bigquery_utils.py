"""
BigQuery Utilities for MDM
Helper functions for BigQuery operations and SQL query generation
"""

from typing import Dict, List, Optional

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


def generate_union_sql(dataset_ref: str) -> str:
    """Generate SQL to combine all raw data sources with consistent schema"""
    return f"""
    CREATE OR REPLACE TABLE `{dataset_ref}.raw_customers_combined` AS
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
    FROM `{dataset_ref}.raw_crm_customers`

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
    FROM `{dataset_ref}.raw_erp_customers`

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
    FROM `{dataset_ref}.raw_ecommerce_customers`
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


def generate_combined_scoring_sql(dataset_ref: str, table_name: str) -> str:
    """Generate SQL for combining all match scores"""
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
      )
    ),
    combined_scores AS (
      SELECT
        p.record1_id,
        p.record2_id,
        p.source1,
        p.source2,

        -- Get scores from each matching strategy
        COALESCE(e.exact_overall_score, 0.0) AS exact_score,
        COALESCE(f.fuzzy_overall_score, 0.0) AS fuzzy_score,
        COALESCE(v.vector_similarity_score, 0.0) AS vector_score,
        COALESCE(
          b.same_company_score + b.same_location_score +
          b.age_compatibility_score + b.income_compatibility_score, 0.0
        ) AS business_score,

        -- Calculate weighted combined score
        (0.4 * COALESCE(e.exact_overall_score, 0.0) +
         0.3 * COALESCE(f.fuzzy_overall_score, 0.0) +
         0.2 * COALESCE(v.vector_similarity_score, 0.0) +
         0.1 * COALESCE(
           b.same_company_score + b.same_location_score +
           b.age_compatibility_score + b.income_compatibility_score, 0.0
         )) AS combined_score

      FROM all_pairs p
      LEFT JOIN `{dataset_ref}.{table_name}_exact_matches` e
        ON p.record1_id = e.record1_id AND p.record2_id = e.record2_id
      LEFT JOIN `{dataset_ref}.{table_name}_fuzzy_matches` f
        ON p.record1_id = f.record1_id AND p.record2_id = f.record2_id
      LEFT JOIN `{dataset_ref}.{table_name}_vector_matches` v
        ON p.record1_id = v.record1_id AND p.record2_id = v.record2_id
      LEFT JOIN `{dataset_ref}.{table_name}_business_matches` b
        ON p.record1_id = b.record1_id AND p.record2_id = b.record2_id
    )
    SELECT
      *,
      -- Calculate confidence and decision
      CASE
        WHEN combined_score >= 0.9 THEN 'auto_merge'
        WHEN combined_score >= 0.7 THEN 'human_review'
        ELSE 'no_match'
      END AS match_decision,

      CASE
        WHEN combined_score >= 0.9 THEN 'high'
        WHEN combined_score >= 0.7 THEN 'medium'
        ELSE 'low'
      END AS confidence_level

    FROM combined_scores
    WHERE combined_score > 0.5  -- Only keep meaningful matches
    ORDER BY combined_score DESC
    """
