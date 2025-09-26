"""
Scalable, Fully-Parallel MDM Data Generator

This script generates large volumes of realistic customer data by parallelizing
the entire workflow across multiple CPU cores. It consists of two main
parallel stages:

1.  **Parallel Pool Generation**: The initial creation of the unique base
    customer pool is parallelized to quickly generate the foundational data.
2.  **Parallel Chunk Generation**: The generation of the final records is
    then parallelized, with each worker drawing from the shared customer
    pool.

This approach is highly scalable, memory-efficient, and designed for
maximum performance on multi-core systems.

Key Features:
- Fully-parallel, two-stage architecture to eliminate bottlenecks.
- Creates three separate tables (crm, erp, ecommerce) as expected by the notebook.
- "Generate -> Split -> Save -> Upload -> Cleanup" workflow for each source.
- Adds source-specific fields for a more realistic dataset.
- Fully configurable via command-line arguments.
"""

import argparse
from concurrent.futures import as_completed, ProcessPoolExecutor
import os
import random
from typing import Any, Dict, List
import uuid

import pandas as pd
from faker import Faker
from google.cloud import bigquery
from tqdm import tqdm

# --- Core Data Variation and Generation Logic ---

def create_base_customer_slice(start_id: int, num_customers: int, seed: int) -> List[Dict[str, Any]]:
    """Generates a slice of unique base customers. Designed to be run in parallel."""
    Faker.seed(seed)
    random.seed(seed)
    fake = Faker()
    customers = []
    for i in range(num_customers):
        customer_id = start_id + i
        first_name = fake.first_name()
        last_name = fake.last_name()
        customers.append({
            'customer_id': f'CUST_{customer_id:08d}',
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
            'is_active': random.choice([True, True, True, False]),
        })
    return customers

def create_variations(customer: Dict[str, Any], source: str, fake: Faker) -> Dict[str, Any]:
    """Creates realistic variations and adds source-specific fields."""
    varied_customer = customer.copy()
    varied_customer['source_system'] = source
    varied_customer['record_id'] = str(uuid.uuid4())

    # General variations
    if random.random() < 0.3:
        if varied_customer['full_name']:
            name_variation = random.choice([
                lambda name: name.replace('John', 'Jon'),
                lambda name: name.replace('Michael', 'Mike'),
            ])
            varied_customer['full_name'] = name_variation(varied_customer['full_name'])
    if random.random() < 0.15:
        field_to_miss = random.choice(['phone', 'company', 'job_title'])
        varied_customer[field_to_miss] = None

    # Add source-specific fields
    if source == 'crm':
        varied_customer.update({
            'lead_source': random.choice(['Website', 'Referral', 'Cold Call']),
            'sales_rep': fake.name(),
            'deal_stage': random.choice(['Prospect', 'Qualified', 'Closed Won']),
        })
    elif source == 'erp':
        varied_customer.update({
            'account_number': f'ACC{random.randint(100000, 999999)}',
            'credit_limit': random.randint(1000, 50000),
            'payment_terms': random.choice(['Net 30', 'Net 60', 'COD']),
        })
    elif source == 'ecommerce':
        varied_customer.update({
            'username': fake.user_name(),
            'total_orders': random.randint(1, 50),
            'total_spent': round(random.uniform(50, 5000), 2),
        })
    return varied_customer

# --- Worker and Orchestration Logic ---

def upload_df_to_bq(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str, output_dir: str, chunk_id: int, source: str, write_disposition: str):
    """Saves a DataFrame to a temp CSV and uploads it to BigQuery."""
    if df.empty:
        return

    temp_csv_path = os.path.join(output_dir, f'{source}_chunk_{chunk_id}.csv')
    try:
        df.to_csv(temp_csv_path, index=False)

        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=write_disposition,
        )
        with open(temp_csv_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
    finally:
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)

def generate_and_upload_chunk(
    chunk_id: int,
    chunk_size: int,
    unique_customer_pool: List[Dict[str, Any]],
    project_id: str,
    dataset_id: str,
    table_suffix: str,
    output_dir: str,
    write_disposition: str,
) -> str:
    """Worker function to generate, split, and upload one chunk of data."""
    random.seed(os.getpid() * chunk_id)
    fake = Faker()
    Faker.seed(os.getpid() * chunk_id)

    records = []
    for _ in range(chunk_size):
        base_customer = random.choice(unique_customer_pool)
        source = random.choices(['crm', 'erp', 'ecommerce'], weights=[0.8, 0.7, 0.6], k=1)[0]
        record = create_variations(base_customer, source, fake)
        records.append(record)

    df = pd.DataFrame(records)

    for source in ['crm', 'erp', 'ecommerce']:
        source_df = df[df['source_system'] == source]
        table_id = f"raw_{source}_customers{table_suffix}"
        upload_df_to_bq(source_df, project_id, dataset_id, table_id, output_dir, chunk_id, source, write_disposition)

    return f"Chunk {chunk_id} completed."

def main():
    """Main function to orchestrate parallel data generation."""
    parser = argparse.ArgumentParser(description="Scalable, Fully-Parallel MDM Data Generator")
    parser.add_argument('--total-records', type=int, default=100_000_000, help="Total records to generate.")
    parser.add_argument('--chunk-size', type=int, default=10000, help="Number of records per chunk per worker.")
    parser.add_argument('--num-workers', type=int, default=os.cpu_count(), help="Number of parallel worker processes.")
    parser.add_argument('--unique-customers', type=int, default=25_000_000, help="Number of unique customers in the pool.")
    parser.add_argument('--project-id', type=str, required=True, help="Google Cloud Project ID.")
    parser.add_argument('--dataset-id', type=str, required=True, help="BigQuery Dataset ID.")
    parser.add_argument('--table-suffix', type=str, default="_scale", help="Suffix to append to table names (e.g., '_scale').")
    parser.add_argument('--output-dir', type=str, default='/tmp/mdm_data_chunks', help="Temporary directory for staging CSV files.")
    parser.add_argument('--write-disposition', type=str, default='WRITE_TRUNCATE', help="BigQuery write disposition (WRITE_TRUNCATE or WRITE_APPEND).")

    args = parser.parse_args()

    if args.total_records > 1_000_000_000:
        print("❌ Error: Generating more than 1 billion records is not recommended.")
        return

    print("🚀 Starting Scalable MDM Data Generator...")
    print(f"  Total Records: ~{args.total_records:,}")
    print(f"  Unique Customers: {args.unique_customers:,}")
    print(f"  Workers: {args.num_workers}")
    print(f"  BigQuery Target Suffix: {args.table_suffix}")

    os.makedirs(args.output_dir, exist_ok=True)

    # --- Stage 1: Parallel Generation of the Unique Customer Pool ---
    print(f"\n🔄 Stage 1: Generating {args.unique_customers:,} unique base customers in parallel...")
    unique_customer_pool = []
    pool_chunks = []
    customers_per_worker = (args.unique_customers + args.num_workers - 1) // args.num_workers

    with ProcessPoolExecutor(max_workers=args.num_workers) as executor:
        for i in range(args.num_workers):
            start_id = i * customers_per_worker
            num_customers = min(customers_per_worker, args.unique_customers - start_id)
            if num_customers > 0:
                pool_chunks.append(executor.submit(create_base_customer_slice, start_id, num_customers, i))

        for future in tqdm(as_completed(pool_chunks), total=len(pool_chunks), desc="Creating Customer Pool"):
            unique_customer_pool.extend(future.result())

    print("✅ Customer pool generated.")

    # --- Stage 2: Parallel Generation of Final Data Chunks ---
    num_chunks = (args.total_records + args.chunk_size - 1) // args.chunk_size
    print(f"\n🔄 Stage 2: Preparing to generate {num_chunks:,} final data chunks in parallel...")

    with ProcessPoolExecutor(max_workers=args.num_workers) as executor:
        futures = [
            executor.submit(
                generate_and_upload_chunk,
                i,
                args.chunk_size,
                unique_customer_pool,
                args.project_id,
                args.dataset_id,
                args.table_suffix,
                args.output_dir,
                args.write_disposition
            ) for i in range(num_chunks)
        ]

        for future in tqdm(as_completed(futures), total=num_chunks, desc="Generating Final Chunks"):
            try:
                future.result()
            except Exception as e:
                print(f"\n❌ An error occurred in a worker process: {e}")

    print("\n🎉 Data generation and upload completed successfully!")

if __name__ == "__main__":
    main()
