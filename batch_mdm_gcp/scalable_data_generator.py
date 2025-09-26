"""
Scalable Data Generator for MDM at Scale (100M+ Records)
Creates realistic customer data with cross-chunk duplicates and BigQuery streaming
"""

import argparse
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import hashlib
import json
import os
import pickle
import random
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple
import uuid

from faker import Faker
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import psutil

# Initialize Faker with fixed seed for reproducibility
fake = Faker()
Faker.seed(42)
random.seed(42)


@dataclass
class GenerationConfig:
    """Configuration for scalable data generation"""
    total_records: int = 100_000_000  # 100M records
    chunk_size: int = 1_000_000       # 1M records per chunk
    # 25M unique customers (4x duplication factor)
    unique_customers: int = 25_000_000

    # Cross-chunk duplicate configuration
    # 30% of records are cross-chunk duplicates
    cross_chunk_duplicate_rate: float = 0.3
    # 20% chance of data evolution per chunk
    temporal_evolution_rate: float = 0.2

    # Source system distribution
    crm_coverage: float = 0.8      # 80% of customers in CRM
    erp_coverage: float = 0.7      # 70% of customers in ERP
    ecommerce_coverage: float = 0.6  # 60% of customers in E-commerce

    # Variation rates (from original generator)
    variation_chance: float = 0.3
    typo_chance: float = 0.1
    missing_data_chance: float = 0.15
    email_domain_change_chance: float = 0.2

    # BigQuery configuration
    project_id: str = "your-project-id"
    dataset_id: str = "mdm_demo_scale"
    location: str = "US"

    # Performance settings
    max_workers: int = 4
    batch_insert_size: int = 1000

    # State management
    state_file: str = "generator_state.json"
    customer_pool_file: str = "customer_pool.pkl"

    # Append mode settings
    append_mode: bool = False
    batch_id: str = ""  # Unique identifier for this generation batch


class CustomerPool:
    """Manages the pool of unique customers for cross-chunk relationships"""

    def __init__(self, config: GenerationConfig):
        self.config = config
        self.customers: Dict[str, Dict[str, Any]] = {}
        self.customer_ids: List[str] = []
        # customer_id -> set of chunks
        self.generation_history: Dict[str, Set[int]] = {}

    def create_base_customer(self, customer_id: str) -> Dict[str, Any]:
        """Create a base customer profile"""
        first_name = fake.first_name()
        last_name = fake.last_name()

        customer = {
            'customer_id': customer_id,
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
            'created_chunk': 0,  # Will be updated when first used
            'last_updated_chunk': 0
        }

        return customer

    def get_or_create_customer(self, customer_id: str) -> Dict[str, Any]:
        """Get existing customer or create new one"""
        if customer_id not in self.customers:
            self.customers[customer_id] = self.create_base_customer(
                customer_id)
            self.customer_ids.append(customer_id)
            self.generation_history[customer_id] = set()

        return self.customers[customer_id].copy()

    def mark_customer_used(self, customer_id: str, chunk_id: int):
        """Mark that a customer was used in a specific chunk"""
        if customer_id not in self.generation_history:
            self.generation_history[customer_id] = set()
        self.generation_history[customer_id].add(chunk_id)

        if customer_id in self.customers:
            self.customers[customer_id]['last_updated_chunk'] = chunk_id

    def get_cross_chunk_candidates(self, chunk_id: int, count: int) -> List[str]:
        """Get customer IDs that should appear as cross-chunk duplicates"""
        # Prefer customers from recent chunks for temporal locality
        weighted_customers = []

        for customer_id, chunks in self.generation_history.items():
            if not chunks:  # Skip unused customers
                continue

            # Weight based on recency and frequency
            max_chunk = max(chunks)
            frequency = len(chunks)
            recency_weight = max(0, 10 - (chunk_id - max_chunk))
            frequency_weight = min(frequency, 5)  # Cap at 5

            weight = recency_weight + frequency_weight
            if weight > 0:
                weighted_customers.extend([customer_id] * weight)

        # Sample without replacement
        if len(weighted_customers) < count:
            return weighted_customers

        return random.sample(weighted_customers, count)

    def evolve_customer(self, customer: Dict[str, Any], chunk_id: int) -> Dict[str, Any]:
        """Apply temporal evolution to customer data"""
        if random.random() > self.config.temporal_evolution_rate:
            return customer

        evolved = customer.copy()

        # Update activity dates to be more recent
        evolved['last_activity_date'] = fake.date_between(
            start_date=evolved['last_activity_date'],
            end_date='today'
        )

        # Possible income changes
        if random.random() < 0.1:  # 10% chance
            income_change = random.uniform(0.9, 1.1)  # ±10%
            evolved['annual_income'] = int(
                evolved['annual_income'] * income_change)

        # Possible segment changes
        if random.random() < 0.05:  # 5% chance
            evolved['customer_segment'] = random.choice(
                ['Premium', 'Standard', 'Basic'])

        # Possible address changes
        if random.random() < 0.03:  # 3% chance
            evolved['address'] = fake.street_address()
            evolved['city'] = fake.city()
            evolved['state'] = fake.state_abbr()
            evolved['zip_code'] = fake.zipcode()

        return evolved

    def save_state(self):
        """Save customer pool state to disk"""
        with open(self.config.customer_pool_file, 'wb') as f:
            pickle.dump({
                'customers': self.customers,
                'customer_ids': self.customer_ids,
                'generation_history': {k: list(v) for k, v in self.generation_history.items()}
            }, f)

    def load_state(self):
        """Load customer pool state from disk"""
        if os.path.exists(self.config.customer_pool_file):
            with open(self.config.customer_pool_file, 'rb') as f:
                data = pickle.load(f)
                self.customers = data['customers']
                self.customer_ids = data['customer_ids']
                self.generation_history = {
                    k: set(v) for k, v in data['generation_history'].items()}
            print(f"✅ Loaded {len(self.customers)} customers from state file")


class ScalableMDMGenerator:
    """Main scalable data generator class"""

    def __init__(self, config: GenerationConfig):
        self.config = config
        self.customer_pool = CustomerPool(config)
        self.bq_client = bigquery.Client(project=config.project_id)
        self.generation_state = self.load_generation_state()

        # Variation functions (from original generator)
        self.variations = {
            'name_variations': [
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
            ],
            'address_variations': [
                lambda addr: addr.replace('Street', 'St'),
                lambda addr: addr.replace('Avenue', 'Ave'),
                lambda addr: addr.replace('Boulevard', 'Blvd'),
                lambda addr: addr.replace('Road', 'Rd'),
                lambda addr: addr.replace('Drive', 'Dr'),
                lambda addr: addr.replace('Apartment', 'Apt'),
                lambda addr: addr.replace('Suite', 'Ste'),
            ],
            'phone_formats': [
                lambda phone: phone,
                lambda phone: phone.replace('-', '.'),
                lambda phone: phone.replace('-', ' '),
                lambda phone: phone.replace('-', ''),
                lambda phone: f"({phone[:3]}) {phone[4:7]}-{phone[8:]}",
            ]
        }

    def setup_bigquery(self):
        """Create BigQuery dataset and tables"""
        print("🔄 Setting up BigQuery infrastructure...")

        # Create dataset
        dataset_ref = bigquery.DatasetReference(
            self.config.project_id, self.config.dataset_id)
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.config.location
            dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
            print(f"✅ Dataset {self.config.dataset_id} ready")
        except Exception as e:
            print(f"❌ Error creating dataset: {e}")
            return False

        # Create tables for each source
        sources = ['crm', 'erp', 'ecommerce']
        for source in sources:
            # Add batch_id to table name if in append mode
            table_suffix = f"_scale_{self.config.batch_id}" if self.config.append_mode and self.config.batch_id else "_scale"
            table_id = f"{self.config.project_id}.{self.config.dataset_id}.raw_{source}_customers{table_suffix}"

            # Define schema (based on original generator)
            schema = [
                bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("source_id", "STRING"),
                bigquery.SchemaField("source_system", "STRING"),
                bigquery.SchemaField("first_name", "STRING"),
                bigquery.SchemaField("last_name", "STRING"),
                bigquery.SchemaField("full_name", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("zip_code", "STRING"),
                bigquery.SchemaField("date_of_birth", "DATE"),
                bigquery.SchemaField("company", "STRING"),
                bigquery.SchemaField("job_title", "STRING"),
                bigquery.SchemaField("annual_income", "INTEGER"),
                bigquery.SchemaField("customer_segment", "STRING"),
                bigquery.SchemaField("registration_date", "DATE"),
                bigquery.SchemaField("last_activity_date", "DATE"),
                bigquery.SchemaField("is_active", "BOOLEAN"),
                bigquery.SchemaField("chunk_id", "INTEGER"),
                bigquery.SchemaField("generation_timestamp", "TIMESTAMP"),
            ]

            # Add source-specific fields
            if source == 'crm':
                schema.extend([
                    bigquery.SchemaField("lead_source", "STRING"),
                    bigquery.SchemaField("sales_rep", "STRING"),
                    bigquery.SchemaField("deal_stage", "STRING"),
                ])
            elif source == 'erp':
                schema.extend([
                    bigquery.SchemaField("account_number", "STRING"),
                    bigquery.SchemaField("credit_limit", "INTEGER"),
                    bigquery.SchemaField("payment_terms", "STRING"),
                    bigquery.SchemaField("account_status", "STRING"),
                ])
            elif source == 'ecommerce':
                schema.extend([
                    bigquery.SchemaField("username", "STRING"),
                    bigquery.SchemaField("total_orders", "INTEGER"),
                    bigquery.SchemaField("total_spent", "FLOAT"),
                    bigquery.SchemaField("preferred_category", "STRING"),
                    bigquery.SchemaField("marketing_opt_in", "BOOLEAN"),
                ])

            try:
                table = bigquery.Table(table_id, schema=schema)
                table = self.bq_client.create_table(table, exists_ok=True)
                print(f"✅ Table raw_{source}_customers_scale ready")
            except Exception as e:
                print(f"❌ Error creating table {table_id}: {e}")
                return False

        return True

    def create_variations(self, customer: Dict[str, Any], source: str, chunk_id: int) -> Dict[str, Any]:
        """Create variations of a customer for different sources (adapted from original)"""
        varied_customer = customer.copy()

        # Add source-specific ID and metadata
        if source == 'crm':
            varied_customer['source_id'] = f'CRM_{random.randint(10000, 99999)}'
        elif source == 'erp':
            varied_customer['source_id'] = f'ERP_{random.randint(10000, 99999)}'
        elif source == 'ecommerce':
            varied_customer['source_id'] = f'EC_{random.randint(10000, 99999)}'

        varied_customer['source_system'] = source
        varied_customer['record_id'] = str(uuid.uuid4())
        varied_customer['chunk_id'] = chunk_id
        varied_customer['generation_timestamp'] = datetime.utcnow()

        # Apply variations (same logic as original generator)
        if random.random() < self.config.variation_chance:
            # Name variations
            for variation_func in self.variations['name_variations']:
                if random.random() < 0.3:
                    varied_customer['full_name'] = variation_func(
                        varied_customer['full_name'])
                    name_parts = varied_customer['full_name'].split()
                    if len(name_parts) >= 2:
                        varied_customer['first_name'] = name_parts[0]
                        varied_customer['last_name'] = name_parts[-1]

        if random.random() < self.config.variation_chance:
            # Address variations
            for variation_func in self.variations['address_variations']:
                if random.random() < 0.4:
                    varied_customer['address'] = variation_func(
                        varied_customer['address'])

        if random.random() < self.config.variation_chance:
            # Phone variations
            phone_format = random.choice(self.variations['phone_formats'])
            varied_customer['phone'] = phone_format(varied_customer['phone'])

        # Email variations
        if random.random() < self.config.email_domain_change_chance:
            email_local = varied_customer['email'].split('@')[0]
            new_domain = random.choice(
                ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
            varied_customer['email'] = f'{email_local}@{new_domain}'

        # Introduce typos
        if random.random() < self.config.typo_chance:
            if random.random() < 0.5:
                # Typo in name
                name = varied_customer['full_name']
                if len(name) > 3:
                    pos = random.randint(1, len(name) - 2)
                    name_list = list(name)
                    name_list[pos] = random.choice(
                        'abcdefghijklmnopqrstuvwxyz')
                    varied_customer['full_name'] = ''.join(name_list)
            else:
                # Typo in address
                addr = varied_customer['address']
                if len(addr) > 5:
                    pos = random.randint(1, len(addr) - 2)
                    addr_list = list(addr)
                    addr_list[pos] = random.choice(
                        'abcdefghijklmnopqrstuvwxyz')
                    varied_customer['address'] = ''.join(addr_list)

        # Missing data simulation
        if random.random() < self.config.missing_data_chance:
            fields_to_potentially_miss = ['phone', 'company', 'job_title']
            field_to_miss = random.choice(fields_to_potentially_miss)
            varied_customer[field_to_miss] = None

        # Add source-specific fields
        if source == 'crm':
            varied_customer.update({
                'lead_source': random.choice(['Website', 'Referral', 'Cold Call', 'Trade Show']),
                'sales_rep': fake.name(),
                'deal_stage': random.choice(['Prospect', 'Qualified', 'Proposal', 'Closed Won', 'Closed Lost'])
            })
        elif source == 'erp':
            varied_customer.update({
                'account_number': f'ACC{random.randint(100000, 999999)}',
                'credit_limit': random.randint(1000, 50000),
                'payment_terms': random.choice(['Net 30', 'Net 60', 'COD', 'Prepaid']),
                'account_status': random.choice(['Active', 'Suspended', 'Closed'])
            })
        elif source == 'ecommerce':
            varied_customer.update({
                'username': fake.user_name(),
                'total_orders': random.randint(1, 50),
                'total_spent': round(random.uniform(50, 5000), 2),
                'preferred_category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports']),
                'marketing_opt_in': random.choice([True, False])
            })

        return varied_customer

    def generate_chunk_customer_list(self, chunk_id: int, chunk_size: int) -> List[str]:
        """Generate list of customer IDs for this chunk with cross-chunk duplicates"""
        customer_ids = []

        # Calculate how many should be cross-chunk duplicates
        cross_chunk_count = int(
            chunk_size * self.config.cross_chunk_duplicate_rate)
        new_customer_count = chunk_size - cross_chunk_count

        # Get cross-chunk duplicate candidates
        if chunk_id > 0:  # No cross-chunk duplicates for first chunk
            cross_chunk_candidates = self.customer_pool.get_cross_chunk_candidates(
                chunk_id, cross_chunk_count
            )
            customer_ids.extend(cross_chunk_candidates)

        # Generate new customer IDs
        start_id = chunk_id * self.config.chunk_size
        for i in range(len(customer_ids), chunk_size):
            customer_id = f'CUST_{start_id + i:08d}'
            customer_ids.append(customer_id)

        # Shuffle to distribute cross-chunk duplicates throughout the chunk
        random.shuffle(customer_ids)

        return customer_ids

    def generate_chunk_for_source(self, chunk_id: int, source: str, customer_ids: List[str]) -> List[Dict[str, Any]]:
        """Generate records for a specific source in this chunk"""
        records = []

        # Determine which customers should appear in this source
        if source == 'crm':
            coverage = self.config.crm_coverage
        elif source == 'erp':
            coverage = self.config.erp_coverage
        else:  # ecommerce
            coverage = self.config.ecommerce_coverage

        # Sample customers for this source
        source_customer_count = int(len(customer_ids) * coverage)
        selected_customers = random.sample(customer_ids, source_customer_count)

        for customer_id in selected_customers:
            # Some customers might have multiple records
            if source == 'ecommerce':
                num_records = random.choices(
                    [1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
            elif source == 'crm':
                num_records = random.choices([1, 2], weights=[0.85, 0.15])[0]
            else:  # erp
                num_records = 1

            for _ in range(num_records):
                # Get base customer (with evolution if it's a cross-chunk duplicate)
                base_customer = self.customer_pool.get_or_create_customer(
                    customer_id)

                # Apply temporal evolution for cross-chunk duplicates
                if customer_id in self.customer_pool.generation_history:
                    base_customer = self.customer_pool.evolve_customer(
                        base_customer, chunk_id)

                # Create variations for this source
                record = self.create_variations(
                    base_customer, source, chunk_id)
                records.append(record)

                # Mark customer as used in this chunk
                self.customer_pool.mark_customer_used(customer_id, chunk_id)

        return records

    def stream_records_to_bigquery(self, records: List[Dict[str, Any]], source: str):
        """Stream records to BigQuery in batches"""
        # Use same table naming logic as setup_bigquery
        table_suffix = f"_scale_{self.config.batch_id}" if self.config.append_mode and self.config.batch_id else "_scale"
        table_id = f"{self.config.project_id}.{self.config.dataset_id}.raw_{source}_customers{table_suffix}"

        # Convert records to BigQuery format
        bq_records = []
        for record in records:
            bq_record = {}
            for key, value in record.items():
                if isinstance(value, datetime):
                    bq_record[key] = value.isoformat()
                elif value is None:
                    bq_record[key] = None
                else:
                    bq_record[key] = value
            bq_records.append(bq_record)

        # Insert in batches
        batch_size = self.config.batch_insert_size
        for i in range(0, len(bq_records), batch_size):
            batch = bq_records[i:i + batch_size]

            try:
                errors = self.bq_client.insert_rows_json(table_id, batch)
                if errors:
                    print(f"❌ BigQuery insert errors: {errors}")
                    return False
            except Exception as e:
                print(f"❌ Error inserting batch to BigQuery: {e}")
                return False

        return True

    def generate_chunk(self, chunk_id: int) -> bool:
        """Generate a complete chunk of data for all sources"""
        print(f"\n🔄 Generating chunk {chunk_id + 1}/{self.get_total_chunks()}")
        chunk_start_time = time.time()

        # Generate customer list for this chunk
        customer_ids = self.generate_chunk_customer_list(
            chunk_id, self.config.chunk_size)

        # Generate data for each source
        sources = ['crm', 'erp', 'ecommerce']
        total_records_generated = 0

        for source in sources:
            source_start_time = time.time()

            # Generate records for this source
            records = self.generate_chunk_for_source(
                chunk_id, source, customer_ids)

            if records:
                # Stream to BigQuery
                success = self.stream_records_to_bigquery(records, source)
                if not success:
                    print(f"❌ Failed to stream {source} records to BigQuery")
                    return False

                total_records_generated += len(records)
                source_time = time.time() - source_start_time
                print(
                    f"  ✅ {source.upper()}: {len(records):,} records in {source_time:.1f}s")

        # Update generation state
        chunk_time = time.time() - chunk_start_time
        self.generation_state['completed_chunks'].append({
            'chunk_id': chunk_id,
            'records_generated': total_records_generated,
            'processing_time': chunk_time,
            'timestamp': datetime.utcnow().isoformat()
        })
        self.generation_state['last_completed_chunk'] = chunk_id
        self.save_generation_state()

        # Save customer pool state periodically
        if chunk_id % 10 == 0:  # Every 10 chunks
            self.customer_pool.save_state()

        # Memory and performance reporting
        memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
        print(
            f"  📊 Chunk {chunk_id + 1} completed: {total_records_generated:,} records in {chunk_time:.1f}s ({memory_mb:.0f}MB RAM)")

        return True

    def get_total_chunks(self) -> int:
        """Calculate total number of chunks needed"""
        return (self.config.total_records + self.config.chunk_size - 1) // self.config.chunk_size

    def generate_all_data(self):
        """Generate all chunks of data"""
        print(f"🚀 Starting scalable MDM data generation")
        print(f"📊 Configuration:")
        print(f"  Total records: {self.config.total_records:,}")
        print(f"  Unique customers: {self.config.unique_customers:,}")
        print(f"  Chunk size: {self.config.chunk_size:,}")
        print(f"  Total chunks: {self.get_total_chunks()}")
        print(
            f"  Duplication factor: {self.config.total_records / self.config.unique_customers:.1f}x")

        # Load existing state
        self.customer_pool.load_state()

        # Setup BigQuery
        if not self.setup_bigquery():
            print("❌ Failed to setup BigQuery infrastructure")
            return False

        start_time = time.time()
        total_chunks = self.get_total_chunks()
        start_chunk = self.generation_state.get('last_completed_chunk', -1) + 1

        if start_chunk > 0:
            print(f"🔄 Resuming from chunk {start_chunk + 1}")

        # Generate chunks
        for chunk_id in range(start_chunk, total_chunks):
            success = self.generate_chunk(chunk_id)
            if not success:
                print(f"❌ Failed to generate chunk {chunk_id}")
                return False

            # Progress reporting
            progress = ((chunk_id + 1) / total_chunks) * 100
            elapsed_time = time.time() - start_time
            eta = (elapsed_time / (chunk_id + 1 - start_chunk)) * \
                (total_chunks - chunk_id - 1)

            print(f"  📈 Progress: {progress:.1f}% | ETA: {eta/3600:.1f}h")

        # Final save
        self.customer_pool.save_state()

        # Final statistics
        total_time = time.time() - start_time
        total_records = sum(chunk['records_generated']
                            for chunk in self.generation_state['completed_chunks'])

        print(f"\n🎉 Data generation completed!")
        print(f"📊 Final Statistics:")
        print(f"  Total records generated: {total_records:,}")
        print(f"  Unique customers: {len(self.customer_pool.customers):,}")
        print(f"  Total processing time: {total_time/3600:.1f} hours")
        print(
            f"  Average throughput: {total_records/total_time:.0f} records/second")

        return True

    def load_generation_state(self) -> Dict[str, Any]:
        """Load generation state from disk"""
        if os.path.exists(self.config.state_file):
            with open(self.config.state_file, 'r') as f:
                return json.load(f)
        return {
            'completed_chunks': [],
            'last_completed_chunk': -1,
            'start_time': datetime.utcnow().isoformat()
        }

    def save_generation_state(self):
        """Save generation state to disk"""
        with open(self.config.state_file, 'w') as f:
            json.dump(self.generation_state, f, indent=2)


def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(description='Scalable MDM Data Generator')
    parser.add_argument('--total-records', type=int, default=100_000_000,
                        help='Total number of records to generate (default: 100M)')
    parser.add_argument('--chunk-size', type=int, default=1_000_000,
                        help='Records per chunk (default: 1M)')
    parser.add_argument('--unique-customers', type=int, default=25_000_000,
                        help='Number of unique customers (default: 25M)')
    parser.add_argument('--project-id', type=str, required=True,
                        help='Google Cloud project ID')
    parser.add_argument('--dataset-id', type=str, default='mdm_demo_scale',
                        help='BigQuery dataset ID (default: mdm_demo_scale)')
    parser.add_argument('--cross-chunk-rate', type=float, default=0.3,
                        help='Cross-chunk duplicate rate (default: 0.3)')
    parser.add_argument('--max-workers', type=int, default=4,
                        help='Maximum worker threads (default: 4)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='BigQuery batch insert size (default: 1000)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from last completed chunk')
    parser.add_argument('--append-mode', action='store_true',
                        help='Enable append mode with unique batch ID')
    parser.add_argument('--batch-id', type=str, default='',
                        help='Custom batch ID for append mode (auto-generated if not provided)')

    args = parser.parse_args()

    # Generate batch ID if in append mode
    batch_id = ""
    if args.append_mode:
        if args.batch_id:
            batch_id = args.batch_id
        else:
            # Auto-generate batch ID with timestamp
            from datetime import datetime
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"🔄 Append mode enabled with batch ID: {batch_id}")

    # Create configuration from arguments
    config = GenerationConfig(
        total_records=args.total_records,
        chunk_size=args.chunk_size,
        unique_customers=args.unique_customers,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        cross_chunk_duplicate_rate=args.cross_chunk_rate,
        max_workers=args.max_workers,
        batch_insert_size=args.batch_size,
        append_mode=args.append_mode,
        batch_id=batch_id
    )

    # Create and run generator
    generator = ScalableMDMGenerator(config)

    print("🚀 Scalable MDM Data Generator")
    print("=" * 50)
    print(f"Project: {config.project_id}")
    print(f"Dataset: {config.dataset_id}")
    print(f"Target: {config.total_records:,} records")
    print(f"Unique customers: {config.unique_customers:,}")
    print(
        f"Cross-chunk duplicate rate: {config.cross_chunk_duplicate_rate:.1%}")
    print("=" * 50)

    try:
        success = generator.generate_all_data()
        if success:
            print("\n🎉 Generation completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Generation failed!")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ Generation interrupted by user")
        generator.customer_pool.save_state()
        generator.save_generation_state()
        print("💾 State saved - you can resume with --resume flag")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        generator.customer_pool.save_state()
        generator.save_generation_state()
        sys.exit(1)


if __name__ == "__main__":
    main()
