"""
Data Generator for MDM Sample Data
Creates realistic customer data with intentional duplicates and variations
"""

import random
from typing import Any, Dict, List
import uuid

from faker import Faker
import pandas as pd

fake = Faker()
Faker.seed(42)  # For reproducible results
random.seed(42)


class MDMDataGenerator:
    """Generate sample customer data for MDM testing"""

    def __init__(self, num_unique_customers: int = 120):
        self.num_unique_customers = num_unique_customers
        self.unique_customers = []
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
                lambda phone: phone,  # Original format
                lambda phone: phone.replace('-', '.'),
                lambda phone: phone.replace('-', ' '),
                lambda phone: phone.replace('-', ''),
                lambda phone: f"({phone[:3]}) {phone[4:7]}-{phone[8:]}",
            ]
        }

    def generate_base_customers(self) -> List[Dict[str, Any]]:
        """Generate unique base customers"""
        customers = []

        for i in range(self.num_unique_customers):
            # Generate base customer
            first_name = fake.first_name()
            last_name = fake.last_name()

            customer = {
                'customer_id': f'CUST_{i+1:05d}',
                'first_name': first_name,
                'last_name': last_name,
                'full_name': f'{first_name} {last_name}',
                'email': fake.email(),
                'phone': fake.phone_number()[:12],  # Standardize length
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

        self.unique_customers = customers
        return customers

    def create_variations(self, customer: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Create variations of a customer for different sources"""
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

        # Apply random variations
        variation_chance = 0.3  # 30% chance of variation

        # Name variations
        if random.random() < variation_chance:
            for variation_func in self.variations['name_variations']:
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
            for variation_func in self.variations['address_variations']:
                if random.random() < 0.4:  # 40% chance to apply each variation
                    varied_customer['address'] = variation_func(
                        varied_customer['address'])

        # Phone variations
        if random.random() < variation_chance:
            phone_format = random.choice(self.variations['phone_formats'])
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
        missing_chance = 0.15  # 15% chance of missing data
        if random.random() < missing_chance:
            fields_to_potentially_miss = ['phone', 'company', 'job_title']
            field_to_miss = random.choice(fields_to_potentially_miss)
            varied_customer[field_to_miss] = None

        return varied_customer

    def generate_crm_data(self) -> pd.DataFrame:
        """Generate CRM customer data"""
        if not self.unique_customers:
            self.generate_base_customers()

        crm_customers = []

        # Include 80% of unique customers in CRM
        selected_customers = random.sample(
            self.unique_customers, int(0.8 * len(self.unique_customers)))

        for customer in selected_customers:
            # Some customers might have multiple records in CRM
            num_records = random.choices([1, 2], weights=[0.85, 0.15])[
                0]  # 15% chance of duplicates

            for _ in range(num_records):
                crm_customer = self.create_variations(customer, 'crm')
                # CRM specific fields
                crm_customer['lead_source'] = random.choice(
                    ['Website', 'Referral', 'Cold Call', 'Trade Show'])
                crm_customer['sales_rep'] = fake.name()
                crm_customer['deal_stage'] = random.choice(
                    ['Prospect', 'Qualified', 'Proposal', 'Closed Won', 'Closed Lost'])
                crm_customers.append(crm_customer)

        return pd.DataFrame(crm_customers)

    def generate_erp_data(self) -> pd.DataFrame:
        """Generate ERP customer data"""
        if not self.unique_customers:
            self.generate_base_customers()

        erp_customers = []

        # Include 70% of unique customers in ERP (existing customers)
        selected_customers = random.sample(
            self.unique_customers, int(0.7 * len(self.unique_customers)))

        for customer in selected_customers:
            erp_customer = self.create_variations(customer, 'erp')
            # ERP specific fields
            erp_customer['account_number'] = f'ACC{random.randint(100000, 999999)}'
            erp_customer['credit_limit'] = random.randint(1000, 50000)
            erp_customer['payment_terms'] = random.choice(
                ['Net 30', 'Net 60', 'COD', 'Prepaid'])
            erp_customer['account_status'] = random.choice(
                ['Active', 'Suspended', 'Closed'])
            erp_customers.append(erp_customer)

        return pd.DataFrame(erp_customers)

    def generate_ecommerce_data(self) -> pd.DataFrame:
        """Generate E-commerce customer data"""
        if not self.unique_customers:
            self.generate_base_customers()

        ecommerce_customers = []

        # Include 60% of unique customers in E-commerce
        selected_customers = random.sample(
            self.unique_customers, int(0.6 * len(self.unique_customers)))

        for customer in selected_customers:
            # E-commerce might have more duplicates due to guest checkouts
            num_records = random.choices(
                [1, 2, 3], weights=[0.7, 0.25, 0.05])[0]

            for _ in range(num_records):
                ecom_customer = self.create_variations(customer, 'ecommerce')
                # E-commerce specific fields
                ecom_customer['username'] = fake.user_name()
                ecom_customer['total_orders'] = random.randint(1, 50)
                ecom_customer['total_spent'] = round(
                    random.uniform(50, 5000), 2)
                ecom_customer['preferred_category'] = random.choice(
                    ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'])
                ecom_customer['marketing_opt_in'] = random.choice(
                    [True, False])
                ecommerce_customers.append(ecom_customer)

        return pd.DataFrame(ecommerce_customers)

    def generate_all_datasets(self) -> Dict[str, pd.DataFrame]:
        """Generate all three datasets"""
        self.generate_base_customers()

        datasets = {
            'crm': self.generate_crm_data(),
            'erp': self.generate_erp_data(),
            'ecommerce': self.generate_ecommerce_data()
        }

        return datasets


def main():
    """Generate sample data and save to CSV files"""
    generator = MDMDataGenerator(num_unique_customers=120)
    datasets = generator.generate_all_datasets()

    # Create data directory if it doesn't exist
    import os
    os.makedirs('data', exist_ok=True)

    # Save datasets
    for source, df in datasets.items():
        filename = f'data/{source}_customers.csv'
        df.to_csv(filename, index=False)
        print(f"Generated {len(df)} records for {source} -> {filename}")

    # Print summary statistics
    print("\nDataset Summary:")
    for source, df in datasets.items():
        print(f"{source.upper()}: {len(df)} records")

    total_records = sum(len(df) for df in datasets.values())
    print(f"Total records: {total_records}")
    print(f"Unique customers: {generator.num_unique_customers}")
    print(
        f"Duplication factor: {total_records / generator.num_unique_customers:.2f}x")


if __name__ == "__main__":
    main()
