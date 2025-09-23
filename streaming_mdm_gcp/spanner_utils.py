"""
Spanner utilities for streaming MDM processing
Provides helper functions for Spanner operations similar to BigQuery utils
"""

import logging
from typing import Dict

from google.cloud import spanner
from google.cloud.spanner_admin_instance_v1 import InstanceAdminClient
from google.cloud.spanner_admin_instance_v1.types import Instance
import pandas as pd


class SpannerMDMHelper:
    """Helper class for Spanner MDM operations"""

    def __init__(self, project_id: str, instance_id: str, database_id: str):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id

        # Initialize Spanner client
        self.client = spanner.Client(project=project_id)
        self.instance = self.client.instance(instance_id)
        self.database = self.instance.database(database_id)

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def create_instance_if_needed(self, processing_units: int = 100):
        """Create Spanner instance if it doesn't exist"""
        try:
            # Check if instance exists
            instance = self.client.instance(self.instance_id)
            if instance.exists():
                print(f"  ✅ Instance {self.instance_id} already exists")
                return

            # Create instance using the correct API
            print(f"  🔄 Creating Spanner instance: {self.instance_id}")
            config_name = f"projects/{self.project_id}/instanceConfigs/regional-us-central1"

            # Use the instance admin client directly
            instance_admin_client = InstanceAdminClient()

            # Create the instance object
            instance_obj = Instance(
                display_name="MDM Streaming Demo",
                config=config_name,
                processing_units=processing_units
            )

            # Create the instance
            operation = instance_admin_client.create_instance(
                parent=f"projects/{self.project_id}",
                instance_id=self.instance_id,
                instance=instance_obj
            )

            print(f"  ⏳ Waiting for instance creation...")
            operation.result(timeout=300)  # 5 minutes timeout
            print(f"  ✅ Instance {self.instance_id} created successfully")

        except Exception as e:
            print(f"  ❌ Error with instance: {e}")
            raise

    def create_database_if_needed(self):
        """Create database if it doesn't exist"""
        try:
            # Check if database exists
            if self.database.exists():
                print(f"  ✅ Database {self.database_id} already exists")
                return

            # Create database
            print(f"  🔄 Creating database: {self.database_id}")
            operation = self.database.create()
            operation.result(timeout=120)  # 2 minutes timeout
            print(f"  ✅ Database {self.database_id} created successfully")

        except Exception as e:
            print(f"  ❌ Error with database: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        try:
            query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ''
            AND table_name = @table_name
            """
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(
                    query,
                    params={'table_name': table_name},
                    param_types={'table_name': spanner.param_types.STRING}
                )
                return len(list(results)) > 0
        except:
            return False

    def drop_table_if_exists(self, table_name: str):
        """Drop a table if it exists with proper verification"""
        try:
            if self.table_exists(table_name):
                print(f"    🗑️ Dropping existing table: {table_name}")

                # Step 1: Drop known indexes first (ignore errors if they don't exist)
                if table_name == "golden_entities":
                    known_indexes = [
                        "idx_master_email", "idx_master_phone", "idx_master_name", "idx_master_company"]
                    for index in known_indexes:
                        try:
                            operation = self.database.update_ddl(
                                [f"DROP INDEX {index}"])
                            operation.result(timeout=30)
                            print(f"      ✅ Dropped index: {index}")
                        except:
                            pass  # Ignore if index doesn't exist

                # Step 2: Drop the table
                operation = self.database.update_ddl(
                    [f"DROP TABLE {table_name}"])
                operation.result(timeout=60)
                print(f"    ✅ Table {table_name} dropped successfully")
            else:
                print(
                    f"    ℹ️ Table {table_name} does not exist, skipping drop")
        except Exception as e:
            print(f"    ❌ Error dropping table {table_name}: {e}")
            raise

    def create_or_replace_schema(self):
        """Create or replace the MDM schema - aligned with BigQuery structure"""
        try:
            print("  🔄 Creating/updating schema...")

            # Step 1: Drop existing tables with verification
            print("  📋 Step 1: Dropping existing tables...")
            tables_to_drop = ["match_results", "golden_entities"]
            for table_name in tables_to_drop:
                self.drop_table_if_exists(table_name)

            # Step 2: Create tables
            print("  📋 Step 2: Creating tables...")
            create_statements = [
                # Create golden_entities table
                """CREATE TABLE golden_entities (
                    entity_id STRING(36) NOT NULL,
                    source_record_ids ARRAY<STRING(36)>,
                    source_record_count INT64,
                    source_systems ARRAY<STRING(50)>,
                    master_name STRING(200),
                    master_email STRING(200),
                    master_phone STRING(20),
                    master_address STRING(500),
                    master_city STRING(100),
                    master_state STRING(50),
                    master_company STRING(200),
                    master_income INT64,
                    master_segment STRING(50),
                    embedding ARRAY<FLOAT64>,
                    first_seen DATE,
                    last_activity DATE,
                    confidence_score FLOAT64,
                    processing_path STRING(20),
                    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                    updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
                ) PRIMARY KEY (entity_id)""",

                # Create match_results table
                """CREATE TABLE match_results (
                    match_id STRING(36) NOT NULL,
                    record1_id STRING(36) NOT NULL,
                    record2_id STRING(36) NOT NULL,
                    source1 STRING(50),
                    source2 STRING(50),
                    exact_score FLOAT64,
                    fuzzy_score FLOAT64,
                    vector_score FLOAT64,
                    business_score FLOAT64,
                    combined_score FLOAT64 NOT NULL,
                    confidence_level STRING(20),
                    match_decision STRING(20),
                    matched_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
                    processing_time_ms INT64
                ) PRIMARY KEY (match_id)"""
            ]

            for i, stmt in enumerate(create_statements):
                try:
                    table_name = "golden_entities" if i == 0 else "match_results"
                    print(f"    🔨 Creating table: {table_name}")
                    operation = self.database.update_ddl([stmt])
                    operation.result(timeout=60)
                    print(f"    ✅ Table {table_name} created successfully")
                except Exception as e:
                    print(f"    ❌ Error creating table: {e}")
                    raise

            # Step 3: Create indexes
            print("  📋 Step 3: Creating indexes...")
            index_statements = [
                "CREATE INDEX idx_master_email ON golden_entities(master_email)",
                "CREATE INDEX idx_master_phone ON golden_entities(master_phone)",
                "CREATE INDEX idx_master_name ON golden_entities(master_name)",
                "CREATE INDEX idx_master_company ON golden_entities(master_company)"
            ]

            for i, stmt in enumerate(index_statements):
                try:
                    index_name = stmt.split()[2]  # Extract index name
                    print(f"    📊 Creating index: {index_name}")
                    operation = self.database.update_ddl([stmt])
                    operation.result(timeout=60)
                    print(f"    ✅ Index {index_name} created successfully")
                except Exception as e:
                    print(f"    ❌ Error creating index: {e}")
                    # Don't fail on index errors, continue
                    pass

            print("  ✅ Schema created successfully (aligned with BigQuery)")

        except Exception as e:
            print(f"  ❌ Error creating schema: {e}")
            raise

    def clear_table(self, table_name: str):
        """Clear all data from a table"""
        try:
            def delete_all(transaction):
                transaction.execute_update(
                    f"DELETE FROM {table_name} WHERE 1=1")

            self.database.run_in_transaction(delete_all)
            print(f"  🗑️ Cleared table: {table_name}")

        except Exception as e:
            print(f"  ⚠️ Could not clear {table_name}: {e}")

    def execute_sql(self, query: str, params: Dict = None, param_types_dict: Dict = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            with self.database.snapshot() as snapshot:
                if params and param_types_dict:
                    results = snapshot.execute_sql(
                        query, params=params, param_types=param_types_dict)
                else:
                    results = snapshot.execute_sql(query)

                # Convert to DataFrame
                rows = list(results)
                if not rows:
                    return pd.DataFrame()

                # Get column names from first row
                columns = [f"col_{i}" for i in range(len(rows[0]))]
                return pd.DataFrame(rows, columns=columns)

        except Exception as e:
            print(f"  ❌ Error executing SQL: {e}")
            return pd.DataFrame()

    def load_golden_records_from_bigquery(self, bq_helper, limit: int = None):
        """Load golden records from BigQuery batch processing"""
        try:
            print("  🔄 Loading golden records from BigQuery...")

            # Query to get golden records from BigQuery
            query = f"""
            SELECT
                master_id,
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
                created_at
            FROM `{bq_helper.dataset_ref}.golden_records`
            """

            if limit:
                query += f" LIMIT {limit}"

            golden_df = bq_helper.execute_query(query)

            if golden_df.empty:
                print("  ⚠️ No golden records found in BigQuery")
                return 0

            # Clear existing data
            self.clear_table("golden_entities")

            # Insert golden records into Spanner
            count = 0
            with self.database.batch() as batch:
                for _, row in golden_df.iterrows():
                    # Helper function to convert BigQuery arrays to Spanner arrays
                    def to_array(value):
                        # Handle None/NaN
                        if pd.isna(value) or value is None:
                            return []

                        # Handle already converted lists
                        if isinstance(value, list):
                            return [str(item) if item is not None else None for item in value]

                        # Handle numpy arrays and other iterables (but not strings)
                        if hasattr(value, '__iter__') and not isinstance(value, (str, int, float)):
                            try:
                                result = list(value)
                                return [str(item) if item is not None else None for item in result]
                            except:
                                # If conversion fails, treat as single value
                                return [str(value)]

                        # Handle string representations of arrays
                        if isinstance(value, str):
                            if value.startswith('[') and value.endswith(']'):
                                try:
                                    import ast
                                    parsed = ast.literal_eval(value)
                                    if isinstance(parsed, list):
                                        return [str(item) for item in parsed]
                                    else:
                                        return [str(parsed)]
                                except:
                                    return [value]
                            return [value]

                        # Handle all other types (int, float, etc.) - wrap in list
                        return [str(value)]

                    # Debug: Print the values to see what's causing the issue
                    values_list = [
                        row['master_id'],
                        to_array(row['source_record_ids']),
                        int(row['source_record_count']) if pd.notna(
                            row['source_record_count']) else 1,
                        to_array(row['source_systems']),
                        row['master_name'] if pd.notna(
                            row['master_name']) else None,
                        row['master_email'] if pd.notna(
                            row['master_email']) else None,
                        row['master_phone'] if pd.notna(
                            row['master_phone']) else None,
                        row['master_address'] if pd.notna(
                            row['master_address']) else None,
                        row['master_city'] if pd.notna(
                            row['master_city']) else None,
                        row['master_state'] if pd.notna(
                            row['master_state']) else None,
                        row['master_company'] if pd.notna(
                            row['master_company']) else None,
                        int(row['master_income']) if pd.notna(
                            row['master_income']) else None,
                        row['master_segment'] if pd.notna(
                            row['master_segment']) else None,
                        row['first_seen'] if pd.notna(
                            row['first_seen']) else None,
                        row['last_activity'] if pd.notna(
                            row['last_activity']) else None,
                        0.95,  # High confidence for migrated records
                        'batch_migrated',
                        row['created_at'] if pd.notna(
                            row['created_at']) else spanner.COMMIT_TIMESTAMP,
                        spanner.COMMIT_TIMESTAMP
                    ]

                    batch.insert(
                        table="golden_entities",
                        columns=[
                            "entity_id", "source_record_ids", "source_record_count",
                            "source_systems", "master_name", "master_email",
                            "master_phone", "master_address", "master_city",
                            "master_state", "master_company", "master_income",
                            "master_segment", "first_seen", "last_activity",
                            "confidence_score", "processing_path", "created_at", "updated_at"
                        ],
                        # Wrap in list - each row should be a list
                        values=[values_list]
                    )
                    count += 1

            print(f"  ✅ Loaded {count} golden records from BigQuery")
            return count

        except Exception as e:
            print(f"  ❌ Error loading golden records: {e}")
            raise

    def get_table_count(self, table_name: str) -> int:
        """Get count of records in a table"""
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self.execute_sql(query)
            return int(result.iloc[0]['col_0']) if not result.empty else 0
        except:
            return 0
