"""
PySpark MDM Data Generator

Enterprise-scale MDM data generation using PySpark and Dataproc Serverless.
Preserves all sophisticated data generation logic from the original implementation
while solving multiprocessing limitations through distributed computing.

Usage:
    spark-submit \
        --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2 \
        spark_data_generator.py \
        --project-id YOUR_PROJECT_ID \
        --dataset-id mdm_demo \
        --total-records 100000000
"""

import argparse
import random
from typing import Any, Dict, List
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DateType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def create_spark_session(app_name: str = "MDM-Data-Generator") -> SparkSession:
    """Create optimized Spark session for BigQuery integration."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def get_customer_schema() -> StructType:
    """Define schema for base customer data."""
    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("company", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("annual_income", IntegerType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("registration_date", DateType(), True),
        StructField("last_activity_date", DateType(), True),
        StructField("is_active", BooleanType(), True),
    ])


def get_base_schema() -> StructType:
    """Define base schema for all customer records."""
    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("source_id", StringType(), True),
        StructField("source_system", StringType(), False),
        StructField("record_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("company", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("annual_income", IntegerType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("registration_date", DateType(), True),
        StructField("last_activity_date", DateType(), True),
        StructField("is_active", BooleanType(), True),
    ])


def get_crm_schema() -> StructType:
    """Define schema for CRM records."""
    base_fields = get_base_schema().fields
    crm_fields = [
        StructField("lead_source", StringType(), True),
        StructField("sales_rep", StringType(), True),
        StructField("deal_stage", StringType(), True),
    ]
    return StructType(base_fields + crm_fields)


def get_erp_schema() -> StructType:
    """Define schema for ERP records."""
    base_fields = get_base_schema().fields
    erp_fields = [
        StructField("account_number", StringType(), True),
        StructField("credit_limit", IntegerType(), True),
        StructField("payment_terms", StringType(), True),
        StructField("account_status", StringType(), True),
    ]
    return StructType(base_fields + erp_fields)


def get_ecommerce_schema() -> StructType:
    """Define schema for E-commerce records."""
    base_fields = get_base_schema().fields
    ecommerce_fields = [
        StructField("username", StringType(), True),
        StructField("total_orders", IntegerType(), True),
        StructField("total_spent", FloatType(), True),
        StructField("preferred_category", StringType(), True),
        StructField("marketing_opt_in", BooleanType(), True),
    ]
    return StructType(base_fields + ecommerce_fields)


def create_base_customer_data(partition_id: int, num_customers: int, seed_base: int):
    """Generate base customer data for a partition (preserves original logic)."""
    # Set partition-specific seed for reproducibility
    partition_seed = seed_base + partition_id
    random.seed(partition_seed)

    # Import Faker within the function for distributed execution
    from faker import Faker
    fake = Faker()
    Faker.seed(partition_seed)

    customers = []
    start_id = partition_id * num_customers

    for i in range(num_customers):
        customer_id = start_id + i
        first_name = fake.first_name()
        last_name = fake.last_name()

        customer = {
            # Matches original format
            'customer_id': f'CUST_{customer_id+1:05d}',
            'first_name': first_name,
            'last_name': last_name,
            'full_name': f'{first_name} {last_name}',
            'email': fake.email(),
            'phone': fake.phone_number()[:12],
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'company': fake.company(),
            'job_title': fake.job(),
            'annual_income': random.randint(30000, 200000),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
            'registration_date': fake.date_between(start_date='-5y', end_date='today'),
            'last_activity_date': fake.date_between(start_date='-1y', end_date='today'),
            # 75% active
            'is_active': random.choice([True, True, True, False]),
        }
        customers.append(customer)

    return customers


def apply_data_variations(customer_data: Dict[str, Any], source: str, partition_seed: int) -> Dict[str, Any]:
    """Apply sophisticated data variations (preserves all original logic) - CLEAN VERSION."""
    # Set seed for consistent variations within partition
    random.seed(partition_seed + hash(customer_data['customer_id']))

    # Import Faker for this partition
    from faker import Faker
    fake = Faker()
    Faker.seed(partition_seed + hash(customer_data['customer_id']))

    varied_customer = customer_data.copy()

    # Add source-specific ID (matches original)
    if source == 'crm':
        varied_customer['source_id'] = f'CRM_{random.randint(10000, 99999)}'
    elif source == 'erp':
        varied_customer['source_id'] = f'ERP_{random.randint(10000, 99999)}'
    elif source == 'ecommerce':
        varied_customer['source_id'] = f'EC_{random.randint(10000, 99999)}'

    varied_customer['source_system'] = source
    varied_customer['record_id'] = str(uuid.uuid4())

    # Apply variations with exact original probabilities
    variation_chance = 0.3  # 30% chance

    # Complete name variations (all 10 from original)
    name_variations = [
        lambda name: name.replace('John', 'Jon'),
        lambda name: name.replace('Michael', 'Mike'),
        lambda name: name.replace('William', 'Bill'),
        lambda name: name.replace('Robert', 'Bob'),
        lambda name: name.replace('James', 'Jim'),
        lambda name: name.replace('Christopher', 'Chris'),
        lambda name: name.replace('Matthew', 'Matt'),
        lambda name: name.replace('Anthony', 'Tony'),
        lambda name: name.replace('Elizabeth', 'Liz'),
        lambda name: name.replace('Jennifer', 'Jen'),
    ]

    # Complete address variations (all 7 from original)
    address_variations = [
        lambda addr: addr.replace('Street', 'St'),
        lambda addr: addr.replace('Avenue', 'Ave'),
        lambda addr: addr.replace('Boulevard', 'Blvd'),
        lambda addr: addr.replace('Road', 'Rd'),
        lambda addr: addr.replace('Drive', 'Dr'),
        lambda addr: addr.replace('Apartment', 'Apt'),
        lambda addr: addr.replace('Suite', 'Ste'),
    ]

    # Complete phone format variations (all 5 from original)
    phone_formats = [
        lambda phone: phone,  # Original format
        lambda phone: phone.replace('-', '.'),
        lambda phone: phone.replace('-', ' '),
        lambda phone: phone.replace('-', ''),
        lambda phone: f"({phone[:3]}) {phone[4:7]}-{phone[8:]}",
    ]

    # Apply name variations (matches original logic)
    if random.random() < variation_chance:
        for variation_func in name_variations:
            if random.random() < 0.3:  # 30% chance each
                varied_customer['full_name'] = variation_func(
                    varied_customer['full_name'])
                name_parts = varied_customer['full_name'].split()
                if len(name_parts) >= 2:
                    varied_customer['first_name'] = name_parts[0]
                    varied_customer['last_name'] = name_parts[-1]

    # Apply address variations (matches original logic)
    if random.random() < variation_chance:
        for variation_func in address_variations:
            if random.random() < 0.4:  # 40% chance each
                varied_customer['address'] = variation_func(
                    varied_customer['address'])

    # Apply phone variations (matches original logic)
    if random.random() < variation_chance:
        phone_format = random.choice(phone_formats)
        varied_customer['phone'] = phone_format(varied_customer['phone'])

    # Email domain variations (matches original 20% chance)
    if random.random() < 0.2:
        email_local = varied_customer['email'].split('@')[0]
        new_domain = random.choice(
            ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
        varied_customer['email'] = f'{email_local}@{new_domain}'

    # Introduce typos (matches original 10% chance)
    if random.random() < 0.1:
        if random.random() < 0.5:
            # Typo in name
            name = varied_customer['full_name']
            if len(name) > 3:
                pos = random.randint(1, len(name) - 2)
                name_list = list(name)
                name_list[pos] = random.choice('abcdefghijklmnopqrstuvwxyz')
                varied_customer['full_name'] = ''.join(name_list)
        else:
            # Typo in address
            addr = varied_customer['address']
            if len(addr) > 5:
                pos = random.randint(1, len(addr) - 2)
                addr_list = list(addr)
                addr_list[pos] = random.choice('abcdefghijklmnopqrstuvwxyz')
                varied_customer['address'] = ''.join(addr_list)

    # Missing data simulation (matches original 15% chance)
    if random.random() < 0.15:
        fields_to_miss = ['phone', 'company', 'job_title']
        field_to_miss = random.choice(fields_to_miss)
        varied_customer[field_to_miss] = None

    # Add ONLY source-specific fields (like batch versions - NO None padding!)
    if source == 'crm':
        varied_customer.update({
            'lead_source': random.choice(['Website', 'Referral', 'Cold Call', 'Trade Show']),
            'sales_rep': fake.name(),
            'deal_stage': random.choice(['Prospect', 'Qualified', 'Proposal', 'Closed Won', 'Closed Lost']),
        })
    elif source == 'erp':
        varied_customer.update({
            'account_number': f'ACC{random.randint(100000, 999999)}',
            'credit_limit': random.randint(1000, 50000),
            'payment_terms': random.choice(['Net 30', 'Net 60', 'COD', 'Prepaid']),
            'account_status': random.choice(['Active', 'Suspended', 'Closed']),
        })
    elif source == 'ecommerce':
        varied_customer.update({
            'username': fake.user_name(),
            'total_orders': random.randint(1, 50),
            'total_spent': round(random.uniform(50, 5000), 2),
            'preferred_category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports']),
            'marketing_opt_in': random.choice([True, False]),
        })

    return varied_customer


def generate_source_records(partition_iter, source: str, coverage: float, duplication_weights: List[float]):
    """Generate records for a specific source with proper duplication logic."""
    from itertools import chain
    import random

    # Get partition ID for seeding
    partition_id = random.randint(0, 10000)
    random.seed(partition_id)

    all_records = []

    for customer_data in partition_iter:
        # Apply source coverage (matches original logic)
        if random.random() > coverage:
            continue  # Skip this customer for this source

        # Apply duplication logic (matches original exactly)
        if source == 'crm':
            num_records = random.choices([1, 2], weights=[0.85, 0.15])[
                0]  # 15% chance of duplicates
        elif source == 'erp':
            num_records = 1  # Always 1 record
        elif source == 'ecommerce':
            num_records = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[
                0]  # Up to 3 records

        # Generate the specified number of records
        for _ in range(num_records):
            record = apply_data_variations(customer_data, source, partition_id)
            all_records.append(record)

    return all_records


def main():
    """Main function for PySpark MDM data generation."""
    parser = argparse.ArgumentParser(description="PySpark MDM Data Generator")
    parser.add_argument('--project-id', type=str,
                        required=True, help="Google Cloud Project ID")
    parser.add_argument('--dataset-id', type=str,
                        default='mdm_demo', help="BigQuery Dataset ID")
    parser.add_argument('--table-suffix', type=str,
                        default='_scale', help="Table name suffix")
    parser.add_argument('--total-records', type=int,
                        default=100_000_000, help="Total records to generate")
    parser.add_argument('--unique-customers', type=int,
                        required=True, help="Number of unique customers")
    parser.add_argument('--write-mode', type=str, default='overwrite', choices=['overwrite', 'append'],
                        help="Write mode for BigQuery tables")
    parser.add_argument('--partitions', type=int, default=1000,
                        help="Number of Spark partitions")

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce logging

    print(f"🚀 Starting PySpark MDM Data Generator")
    print(f"  Project: {args.project_id}")
    print(f"  Dataset: {args.dataset_id}")
    print(f"  Total Records: ~{args.total_records:,}")
    print(f"  Unique Customers: {args.unique_customers:,}")
    print(f"  Partitions: {args.partitions}")

    # Configure BigQuery settings
    spark.conf.set("temporaryGcsBucket", f"{args.project_id}-dataproc-temp")
    spark.conf.set("parentProject", args.project_id)

    # Generate base customer pool using Spark
    print(
        f"\n🔄 Stage 1: Generating {args.unique_customers:,} unique customers...")

    customers_per_partition = args.unique_customers // args.partitions
    seed_base = 42  # For reproducibility

    # Create RDD of partition parameters
    partition_params = [(i, customers_per_partition, seed_base)
                        for i in range(args.partitions)]
    partition_rdd = spark.sparkContext.parallelize(
        partition_params, args.partitions)

    # Generate base customers in parallel
    customer_rdd = partition_rdd.flatMap(
        lambda params: create_base_customer_data(*params))

    # Convert to DataFrame
    customer_df = spark.createDataFrame(
        customer_rdd, schema=get_customer_schema())
    customer_df.cache()  # Cache for reuse across sources

    customer_count = customer_df.count()
    print(f"✅ Generated {customer_count:,} unique customers")

    # Generate records for each source system
    sources = [
        ('crm', 0.8, [0.85, 0.15]),      # 80% coverage, 15% duplicates
        ('erp', 0.7, [1.0]),             # 70% coverage, no duplicates
        ('ecommerce', 0.6, [0.7, 0.25, 0.05])  # 60% coverage, up to 3 records
    ]

    print(f"\n🔄 Stage 2: Generating source-specific records...")

    for source, coverage, dup_weights in sources:
        print(f"  Generating {source.upper()} data...")

        # Convert DataFrame to RDD for custom processing
        customer_rdd = customer_df.rdd.map(lambda row: row.asDict())

        # Generate source-specific records
        source_rdd = customer_rdd.mapPartitions(
            lambda partition: generate_source_records(
                partition, source, coverage, dup_weights)
        )

        # Select appropriate schema for this source (like batch versions!)
        if source == 'crm':
            schema = get_crm_schema()
        elif source == 'erp':
            schema = get_erp_schema()
        elif source == 'ecommerce':
            schema = get_ecommerce_schema()

        # Convert back to DataFrame with clean source-specific schema
        source_df = spark.createDataFrame(source_rdd, schema=schema)

        # Write to BigQuery
        table_name = f"raw_{source}_customers{args.table_suffix}"

        source_df.write \
            .format("bigquery") \
            .option("table", f"{args.project_id}.{args.dataset_id}.{table_name}") \
            .option("writeMethod", "direct") \
            .mode(args.write_mode) \
            .save()

        record_count = source_df.count()
        print(
            f"    ✅ {source.upper()}: {record_count:,} records written to {table_name}")

    # Cleanup
    customer_df.unpersist()
    spark.stop()

    print(f"\n🎉 PySpark data generation completed successfully!")


if __name__ == "__main__":
    main()
