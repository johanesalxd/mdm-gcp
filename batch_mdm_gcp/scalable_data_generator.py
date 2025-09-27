"""
Scalable, Fully-Parallel MDM Data Generator (IPC Deadlock-Safe)

This script generates large volumes of realistic customer data by parallelizing
the entire workflow across multiple CPU cores. It consists of two main
parallel stages, designed to be safe from inter-process communication (IPC)
deadlocks.

1.  **Parallel Pool Generation (to Disk)**: The initial creation of the unique
    base customer pool is parallelized. Each worker saves its slice of
    customers to a temporary file on disk to avoid passing large data
    objects between processes.
2.  **Parallel Chunk Generation**: The main process consolidates the temporary
    files and then launches a new pool of workers to generate the final
    records, with each worker drawing from the shared customer pool.

This approach is highly scalable, memory-efficient, and robust against
deadlocks.
"""

import argparse
from concurrent.futures import as_completed
from concurrent.futures import ProcessPoolExecutor
import os
import pickle
import random
from typing import Any, Dict, List
import uuid

from faker import Faker
from google.cloud import bigquery
import pandas as pd
from tqdm import tqdm

# --- Core Data Variation and Generation Logic ---


def create_base_customer_slice(start_id: int, num_customers: int, seed: int, output_dir: str) -> str:
    """Generates a slice of unique base customers and saves it to a pickle file."""
    Faker.seed(seed)
    random.seed(seed)
    fake = Faker()
    customers = []
    for i in range(num_customers):
        customer_id = start_id + i
        first_name = fake.first_name()
        last_name = fake.last_name()
        customers.append({
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
            'is_active': random.choice([True, True, True, False]),
        })

    file_path = os.path.join(output_dir, f"pool_slice_{seed}.pkl")
    with open(file_path, 'wb') as f:
        pickle.dump(customers, f)
    return file_path


def create_variations(customer: Dict[str, Any], source: str, fake: Faker) -> Dict[str, Any]:
    """Creates realistic variations and adds source-specific fields."""
    varied_customer = customer.copy()

    # Add source-specific ID
    if source == 'crm':
        varied_customer['source_id'] = f'CRM_{random.randint(10000, 99999)}'
    elif source == 'erp':
        varied_customer['source_id'] = f'ERP_{random.randint(10000, 99999)}'
    elif source == 'ecommerce':
        varied_customer['source_id'] = f'EC_{random.randint(10000, 99999)}'

    varied_customer['source_system'] = source
    varied_customer['record_id'] = str(uuid.uuid4())

    # Apply random variations with realistic probabilities
    variation_chance = 0.3  # 30% chance of variation

    # Complete name variations (from original)
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

    # Complete address variations (from original)
    address_variations = [
        lambda addr: addr.replace('Street', 'St'),
        lambda addr: addr.replace('Avenue', 'Ave'),
        lambda addr: addr.replace('Boulevard', 'Blvd'),
        lambda addr: addr.replace('Road', 'Rd'),
        lambda addr: addr.replace('Drive', 'Dr'),
        lambda addr: addr.replace('Apartment', 'Apt'),
        lambda addr: addr.replace('Suite', 'Ste'),
    ]

    # Complete phone format variations (from original)
    phone_formats = [
        lambda phone: phone,  # Original format
        lambda phone: phone.replace('-', '.'),
        lambda phone: phone.replace('-', ' '),
        lambda phone: phone.replace('-', ''),
        lambda phone: f"({phone[:3]}) {phone[4:7]}-{phone[8:]}",
    ]

    # Name variations
    if random.random() < variation_chance:
        for variation_func in name_variations:
            if random.random() < 0.3:  # 30% chance to apply each variation
                varied_customer['full_name'] = variation_func(
                    varied_customer['full_name'])
                # Update first/last name accordingly
                name_parts = varied_customer['full_name'].split()
                if len(name_parts) >= 2:
                    varied_customer['first_name'] = name_parts[0]
                    varied_customer['last_name'] = name_parts[-1]

    # Address variations
    if random.random() < variation_chance:
        for variation_func in address_variations:
            if random.random() < 0.4:  # 40% chance to apply each variation
                varied_customer['address'] = variation_func(
                    varied_customer['address'])

    # Phone variations
    if random.random() < variation_chance:
        phone_format = random.choice(phone_formats)
        varied_customer['phone'] = phone_format(varied_customer['phone'])

    # Email variations (different domains for same person)
    if random.random() < 0.2:  # 20% chance
        email_local = varied_customer['email'].split('@')[0]
        new_domain = random.choice(
            ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
        varied_customer['email'] = f'{email_local}@{new_domain}'

    # Introduce typos
    if random.random() < 0.1:  # 10% chance of typos
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

    # Missing data simulation
    missing_chance = 0.15  # 15% chance of missing data
    if random.random() < missing_chance:
        fields_to_potentially_miss = ['phone', 'company', 'job_title']
        field_to_miss = random.choice(fields_to_potentially_miss)
        varied_customer[field_to_miss] = None

    # Add complete source-specific fields
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
            job = client.load_table_from_file(
                source_file, table_ref, job_config=job_config)
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

    # Sample customers for this chunk (to ensure we get variety)
    chunk_customers = random.sample(unique_customer_pool, min(
        chunk_size, len(unique_customer_pool)))

    # Generate records using original's customer-centric approach
    all_records = []

    # CRM data generation (80% of customers, 15% chance of duplicates)
    crm_customers = random.sample(
        chunk_customers, int(0.8 * len(chunk_customers)))
    for customer in crm_customers:
        num_records = random.choices([1, 2], weights=[0.85, 0.15])[
            0]  # 15% chance of duplicates
        for _ in range(num_records):
            record = create_variations(customer, 'crm', fake)
            all_records.append(record)

    # ERP data generation (70% of customers, always 1 record)
    erp_customers = random.sample(
        chunk_customers, int(0.7 * len(chunk_customers)))
    for customer in erp_customers:
        record = create_variations(customer, 'erp', fake)
        all_records.append(record)

    # E-commerce data generation (60% of customers, multiple records possible)
    ecom_customers = random.sample(
        chunk_customers, int(0.6 * len(chunk_customers)))
    for customer in ecom_customers:
        num_records = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[
            0]  # Up to 3 records
        for _ in range(num_records):
            record = create_variations(customer, 'ecommerce', fake)
            all_records.append(record)

    # Convert to DataFrame and split by source
    df = pd.DataFrame(all_records)

    for source in ['crm', 'erp', 'ecommerce']:
        source_df = df[df['source_system'] == source]
        table_id = f"raw_{source}_customers{table_suffix}"
        upload_df_to_bq(source_df, project_id, dataset_id, table_id,
                        output_dir, chunk_id, source, write_disposition)

    return f"Chunk {chunk_id} completed with {len(all_records)} total records."


def main():
    """Main function to orchestrate parallel data generation."""
    parser = argparse.ArgumentParser(
        description="Scalable, Fully-Parallel MDM Data Generator")
    parser.add_argument('--total-records', type=int,
                        default=100_000_000, help="Total records to generate.")
    parser.add_argument('--chunk-size', type=int, default=10000,
                        help="Number of records per chunk per worker.")
    parser.add_argument('--num-workers', type=int, default=os.cpu_count(),
                        help="Number of parallel worker processes.")
    parser.add_argument('--unique-customers', type=int, default=25_000_000,
                        help="Number of unique customers in the pool.")
    parser.add_argument('--project-id', type=str,
                        required=True, help="Google Cloud Project ID.")
    parser.add_argument('--dataset-id', type=str,
                        required=True, help="BigQuery Dataset ID.")
    parser.add_argument('--table-suffix', type=str, default="_scale",
                        help="Suffix to append to table names (e.g., '_scale').")
    parser.add_argument('--output-dir', type=str, default='/tmp/mdm_data_chunks',
                        help="Temporary directory for staging files.")
    parser.add_argument('--write-disposition', type=str, default='WRITE_TRUNCATE',
                        help="BigQuery write disposition (WRITE_TRUNCATE or WRITE_APPEND).")

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
    print(
        f"\n🔄 Stage 1: Generating {args.unique_customers:,} unique base customers in parallel...")
    pool_slice_files = []
    customers_per_worker = (args.unique_customers +
                            args.num_workers - 1) // args.num_workers

    with ProcessPoolExecutor(max_workers=args.num_workers) as executor:
        futures = []
        for i in range(args.num_workers):
            start_id = i * customers_per_worker
            num_customers = min(customers_per_worker,
                                args.unique_customers - start_id)
            if num_customers > 0:
                futures.append(executor.submit(
                    create_base_customer_slice, start_id, num_customers, i, args.output_dir))

        for future in tqdm(as_completed(futures), total=len(futures), desc="Creating Pool Slices"):
            pool_slice_files.append(future.result())

    print("✅ Customer pool slices generated. Consolidating...")

    unique_customer_pool = []
    for file_path in tqdm(pool_slice_files, desc="Consolidating Pool"):
        with open(file_path, 'rb') as f:
            unique_customer_pool.extend(pickle.load(f))
        os.remove(file_path)

    print("✅ Customer pool consolidated.")

    # --- Stage 2: Parallel Generation of Final Data Chunks ---
    num_chunks = (args.total_records + args.chunk_size - 1) // args.chunk_size
    print(
        f"\n🔄 Stage 2: Preparing to generate {num_chunks:,} final data chunks in parallel...")

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

        for future in tqdm(as_completed(futures), total=len(futures), desc="Generating Final Chunks"):
            try:
                future.result()
            except Exception as e:
                print(f"\n❌ An error occurred in a worker process: {e}")

    print("\n🎉 Data generation and upload completed successfully!")


if __name__ == "__main__":
    main()
