-- =================================================================================
-- BigQuery Script to Triple the Size of MDM Source Data with Variations
--
-- Description:
--   This script takes the existing `_scale` tables (CRM, ERP, E-commerce) and
--   creates new tables with 3x the data. It does this by inserting the original
--   data and then adding two additional, varied copies of each record.
--
--   This process mimics the variation logic of the Python data generators,
--   providing a fast and cost-effective way to scale up test data without
--   rerunning the Spark job.
--
-- Instructions:
--   1. Set the `project_id` and `dataset_id` variables below.
--   2. Run each `CREATE OR REPLACE TABLE` and `INSERT INTO` statement sequentially.
-- =================================================================================

-- =================================================================================
-- Configuration
-- =================================================================================
DECLARE project_id STRING DEFAULT 'johanesa-playground-326616';
DECLARE dataset_id STRING DEFAULT 'mdm_demo';
DECLARE source_table_suffix STRING DEFAULT '_scale';
DECLARE target_table_suffix STRING DEFAULT '_scale_x3';

-- =================================================================================
-- Helper Functions (Optional but Recommended for Readability)
-- =================================================================================
-- BigQuery does not support persistent UDFs in scripting, so we'll embed the logic directly.
-- The logic below is designed to be as close as possible to the Python generator.

-- =================================================================================
-- 1. Process CRM Data
-- =================================================================================

-- Step 1.1: Create the new CRM table with explicit CRM schema (24 columns matching Spark)
CREATE OR REPLACE TABLE `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale_x3` AS
SELECT
  -- Base fields (21 columns)
  customer_id,
  source_id,
  source_system,
  record_id,
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
  is_active,
  -- CRM-specific fields (3 columns)
  lead_source,
  sales_rep,
  deal_stage
FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale`;

-- Step 1.2: Insert the first set of varied duplicates (Enhanced to match Python logic)
INSERT INTO `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale_x3`
SELECT
  -- Core Identifiers
  customer_id,
  CONCAT('CRM_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
  source_system,
  GENERATE_UUID() AS record_id,

  -- Personal Info with Variations
  first_name,
  last_name,

  -- Name Variations (30% chance, matching Python logic)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'John') > 0 THEN REPLACE(full_name, 'John', 'Jon')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Michael') > 0 THEN REPLACE(full_name, 'Michael', 'Mike')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'William') > 0 THEN REPLACE(full_name, 'William', 'Bill')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Robert') > 0 THEN REPLACE(full_name, 'Robert', 'Bob')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'James') > 0 THEN REPLACE(full_name, 'James', 'Jim')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Christopher') > 0 THEN REPLACE(full_name, 'Christopher', 'Chris')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Matthew') > 0 THEN REPLACE(full_name, 'Matthew', 'Matt')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Anthony') > 0 THEN REPLACE(full_name, 'Anthony', 'Tony')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Elizabeth') > 0 THEN REPLACE(full_name, 'Elizabeth', 'Liz')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Jennifer') > 0 THEN REPLACE(full_name, 'Jennifer', 'Jen')
        -- Typo Introduction (10% chance within name variations)
        WHEN RAND() < 0.1 AND LENGTH(full_name) > 3 THEN
          CONCAT(
            SUBSTR(full_name, 1, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(full_name, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 3 AS INT64))
          )
        ELSE full_name
      END
    ELSE full_name
  END AS full_name,

  -- Email Variation (20% chance of domain change)
  CASE
    WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
      ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
    )
    ELSE email
  END AS email,

  -- Phone Variation (30% chance of format change) - Complete 5 formats
  -- Combined with Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 1 THEN NULL  -- Null phone
    WHEN RAND() < 0.3 THEN
      CASE CAST(FLOOR(RAND() * 5) AS INT64)
        WHEN 0 THEN REPLACE(phone, '-', '.')
        WHEN 1 THEN REPLACE(phone, '-', ' ')
        WHEN 2 THEN REPLACE(phone, '-', '')
        WHEN 3 THEN CONCAT('(', SUBSTR(phone, 1, 3), ') ', SUBSTR(phone, 5, 3), '-', SUBSTR(phone, 9))
        ELSE phone
      END
    ELSE phone
  END AS phone,

  -- Address Variation (30% chance, all 7 variations with nested probability and typos)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        -- Apply individual address variations (40% chance each, matching Python)
        WHEN RAND() < 0.4 THEN
          -- Chain multiple replacements to handle all variations
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            address,
            'Street', 'St'),
            'Avenue', 'Ave'),
            'Boulevard', 'Blvd'),
            'Road', 'Rd'),
            'Drive', 'Dr'),
            'Apartment', 'Apt'),
            'Suite', 'Ste')
        -- Address Typo Introduction (10% chance within address variations)
        WHEN RAND() < 0.1 AND LENGTH(address) > 5 THEN
          CONCAT(
            SUBSTR(address, 1, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(address, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 3 AS INT64))
          )
        ELSE address
      END
    ELSE address
  END AS address,

  city,
  state,
  zip_code,
  date_of_birth,

  -- Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 2 THEN NULL  -- Null company
    ELSE company
  END AS company,

  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 3 THEN NULL  -- Null job_title
    ELSE job_title
  END AS job_title,

  annual_income,
  customer_segment,
  registration_date,
  last_activity_date,
  is_active,

  -- CRM-specific fields (randomized to match Python)
  ['Website', 'Referral', 'Cold Call', 'Trade Show'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))] AS lead_source,

  -- Generate new sales rep name (simplified approach)
  CONCAT(
    ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'Robert', 'Emily', 'William', 'Amanda'][OFFSET(CAST(FLOOR(RAND() * 10) AS INT64))],
    ' ',
    ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'][OFFSET(CAST(FLOOR(RAND() * 10) AS INT64))]
  ) AS sales_rep,

  ['Prospect', 'Qualified', 'Proposal', 'Closed Won', 'Closed Lost'][OFFSET(CAST(FLOOR(RAND() * 5) AS INT64))] AS deal_stage

FROM (
  SELECT *,
    -- Temporary field for address processing
    address as temp_address1,
    -- Missing data simulation variables (single random decision per record)
    RAND() AS missing_data_rand,
    CAST(FLOOR(RAND() * 3) + 1 AS INT64) AS missing_data_choice  -- 1=phone, 2=company, 3=job_title
  FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale`
);


-- Step 1.3: Insert the second set of varied duplicates (Enhanced to match Python logic)
INSERT INTO `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale_x3`
SELECT
  -- Core Identifiers
  customer_id,
  CONCAT('CRM_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
  source_system,
  GENERATE_UUID() AS record_id,

  -- Personal Info with Variations
  first_name,
  last_name,

  -- Name Variations (30% chance, matching Python logic)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'John') > 0 THEN REPLACE(full_name, 'John', 'Jon')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Michael') > 0 THEN REPLACE(full_name, 'Michael', 'Mike')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'William') > 0 THEN REPLACE(full_name, 'William', 'Bill')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Robert') > 0 THEN REPLACE(full_name, 'Robert', 'Bob')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'James') > 0 THEN REPLACE(full_name, 'James', 'Jim')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Christopher') > 0 THEN REPLACE(full_name, 'Christopher', 'Chris')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Matthew') > 0 THEN REPLACE(full_name, 'Matthew', 'Matt')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Anthony') > 0 THEN REPLACE(full_name, 'Anthony', 'Tony')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Elizabeth') > 0 THEN REPLACE(full_name, 'Elizabeth', 'Liz')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Jennifer') > 0 THEN REPLACE(full_name, 'Jennifer', 'Jen')
        -- Typo Introduction (10% chance within name variations)
        WHEN RAND() < 0.1 AND LENGTH(full_name) > 3 THEN
          CONCAT(
            SUBSTR(full_name, 1, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(full_name, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 3 AS INT64))
          )
        ELSE full_name
      END
    ELSE full_name
  END AS full_name,

  -- Email Variation (20% chance of domain change)
  CASE
    WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
      ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
    )
    ELSE email
  END AS email,

  -- Phone Variation (30% chance of format change) - Complete 5 formats
  -- Combined with Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 1 THEN NULL  -- Null phone
    WHEN RAND() < 0.3 THEN
      CASE CAST(FLOOR(RAND() * 5) AS INT64)
        WHEN 0 THEN REPLACE(phone, '-', '.')
        WHEN 1 THEN REPLACE(phone, '-', ' ')
        WHEN 2 THEN REPLACE(phone, '-', '')
        WHEN 3 THEN CONCAT('(', SUBSTR(phone, 1, 3), ') ', SUBSTR(phone, 5, 3), '-', SUBSTR(phone, 9))
        ELSE phone
      END
    ELSE phone
  END AS phone,

  -- Address Variation (30% chance, all 7 variations with nested probability and typos)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        -- Apply individual address variations (40% chance each, matching Python)
        WHEN RAND() < 0.4 THEN
          -- Chain multiple replacements to handle all variations
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            address,
            'Street', 'St'),
            'Avenue', 'Ave'),
            'Boulevard', 'Blvd'),
            'Road', 'Rd'),
            'Drive', 'Dr'),
            'Apartment', 'Apt'),
            'Suite', 'Ste')
        -- Address Typo Introduction (10% chance within address variations)
        WHEN RAND() < 0.1 AND LENGTH(address) > 5 THEN
          CONCAT(
            SUBSTR(address, 1, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(address, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 3 AS INT64))
          )
        ELSE address
      END
    ELSE address
  END AS address,

  city,
  state,
  zip_code,
  date_of_birth,

  -- Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 2 THEN NULL  -- Null company
    ELSE company
  END AS company,

  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 3 THEN NULL  -- Null job_title
    ELSE job_title
  END AS job_title,

  annual_income,
  customer_segment,
  registration_date,
  last_activity_date,
  is_active,

  -- CRM-specific fields (randomized to match Python)
  ['Website', 'Referral', 'Cold Call', 'Trade Show'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))] AS lead_source,

  -- Generate new sales rep name (simplified approach)
  CONCAT(
    ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'Robert', 'Emily', 'William', 'Amanda'][OFFSET(CAST(FLOOR(RAND() * 10) AS INT64))],
    ' ',
    ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'][OFFSET(CAST(FLOOR(RAND() * 10) AS INT64))]
  ) AS sales_rep,

  ['Prospect', 'Qualified', 'Proposal', 'Closed Won', 'Closed Lost'][OFFSET(CAST(FLOOR(RAND() * 5) AS INT64))] AS deal_stage

FROM (
  SELECT *,
    -- Missing data simulation variables (single random decision per record)
    RAND() AS missing_data_rand,
    CAST(FLOOR(RAND() * 3) + 1 AS INT64) AS missing_data_choice  -- 1=phone, 2=company, 3=job_title
  FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale`
);


-- =================================================================================
-- 2. Process ERP Data
-- =================================================================================

-- Step 2.1: Create the new ERP table with explicit ERP schema (25 columns matching Spark)
CREATE OR REPLACE TABLE `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale_x3` AS
SELECT
  -- Base fields (21 columns)
  customer_id,
  source_id,
  source_system,
  record_id,
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
  is_active,
  -- ERP-specific fields (4 columns)
  account_number,
  credit_limit,
  payment_terms,
  account_status
FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale`;

-- Step 2.2: Insert the first set of varied duplicates (Enhanced to match Python logic)
INSERT INTO `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale_x3`
SELECT
  -- Core Identifiers
  customer_id,
  CONCAT('ERP_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
  source_system,
  GENERATE_UUID() AS record_id,

  -- Personal Info with Variations (matching CRM enhancements)
  first_name,
  last_name,

  -- Name Variations (30% chance, matching Python logic)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'John') > 0 THEN REPLACE(full_name, 'John', 'Jon')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Michael') > 0 THEN REPLACE(full_name, 'Michael', 'Mike')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'William') > 0 THEN REPLACE(full_name, 'William', 'Bill')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Robert') > 0 THEN REPLACE(full_name, 'Robert', 'Bob')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'James') > 0 THEN REPLACE(full_name, 'James', 'Jim')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Christopher') > 0 THEN REPLACE(full_name, 'Christopher', 'Chris')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Matthew') > 0 THEN REPLACE(full_name, 'Matthew', 'Matt')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Anthony') > 0 THEN REPLACE(full_name, 'Anthony', 'Tony')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Elizabeth') > 0 THEN REPLACE(full_name, 'Elizabeth', 'Liz')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Jennifer') > 0 THEN REPLACE(full_name, 'Jennifer', 'Jen')
        -- Typo Introduction (10% chance within name variations)
        WHEN RAND() < 0.1 AND LENGTH(full_name) > 3 THEN
          CONCAT(
            SUBSTR(full_name, 1, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(full_name, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 3 AS INT64))
          )
        ELSE full_name
      END
    ELSE full_name
  END AS full_name,

  -- Email Variation (20% chance of domain change)
  CASE
    WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
      ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
    )
    ELSE email
  END AS email,

  -- Phone Variation (30% chance of format change) - Complete 5 formats
  -- Combined with Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 1 THEN NULL  -- Null phone
    WHEN RAND() < 0.3 THEN
      CASE CAST(FLOOR(RAND() * 5) AS INT64)
        WHEN 0 THEN REPLACE(phone, '-', '.')
        WHEN 1 THEN REPLACE(phone, '-', ' ')
        WHEN 2 THEN REPLACE(phone, '-', '')
        WHEN 3 THEN CONCAT('(', SUBSTR(phone, 1, 3), ') ', SUBSTR(phone, 5, 3), '-', SUBSTR(phone, 9))
        ELSE phone
      END
    ELSE phone
  END AS phone,

  -- Address Variation (30% chance, all 7 variations with nested probability and typos)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        -- Apply individual address variations (40% chance each, matching Python)
        WHEN RAND() < 0.4 THEN
          -- Chain multiple replacements to handle all variations
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            address,
            'Street', 'St'),
            'Avenue', 'Ave'),
            'Boulevard', 'Blvd'),
            'Road', 'Rd'),
            'Drive', 'Dr'),
            'Apartment', 'Apt'),
            'Suite', 'Ste')
        -- Address Typo Introduction (10% chance within address variations)
        WHEN RAND() < 0.1 AND LENGTH(address) > 5 THEN
          CONCAT(
            SUBSTR(address, 1, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(address, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 3 AS INT64))
          )
        ELSE address
      END
    ELSE address
  END AS address,

  city,
  state,
  zip_code,
  date_of_birth,

  -- Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 2 THEN NULL  -- Null company
    ELSE company
  END AS company,

  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 3 THEN NULL  -- Null job_title
    ELSE job_title
  END AS job_title,

  annual_income,
  customer_segment,
  registration_date,
  last_activity_date,
  is_active,

  -- ERP-specific fields (randomized to match Python)
  CONCAT('ACC', CAST(FLOOR(RAND() * 900000) + 100000 AS INT64)) AS account_number,
  CAST(FLOOR(RAND() * 49001) + 1000 AS INT64) AS credit_limit,
  ['Net 30', 'Net 60', 'COD', 'Prepaid'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))] AS payment_terms,
  ['Active', 'Suspended', 'Closed'][OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))] AS account_status

FROM (
  SELECT *,
    -- Missing data simulation variables (single random decision per record)
    RAND() AS missing_data_rand,
    CAST(FLOOR(RAND() * 3) + 1 AS INT64) AS missing_data_choice  -- 1=phone, 2=company, 3=job_title
  FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale`
);

-- Step 2.3: Insert the second set of varied duplicates (Enhanced to match Python logic)
INSERT INTO `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale_x3`
SELECT
  -- Core Identifiers
  customer_id,
  CONCAT('ERP_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
  source_system,
  GENERATE_UUID() AS record_id,

  -- Personal Info with Variations (matching CRM enhancements)
  first_name,
  last_name,

  -- Name Variations (30% chance, matching Python logic)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'John') > 0 THEN REPLACE(full_name, 'John', 'Jon')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Michael') > 0 THEN REPLACE(full_name, 'Michael', 'Mike')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'William') > 0 THEN REPLACE(full_name, 'William', 'Bill')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Robert') > 0 THEN REPLACE(full_name, 'Robert', 'Bob')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'James') > 0 THEN REPLACE(full_name, 'James', 'Jim')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Christopher') > 0 THEN REPLACE(full_name, 'Christopher', 'Chris')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Matthew') > 0 THEN REPLACE(full_name, 'Matthew', 'Matt')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Anthony') > 0 THEN REPLACE(full_name, 'Anthony', 'Tony')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Elizabeth') > 0 THEN REPLACE(full_name, 'Elizabeth', 'Liz')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Jennifer') > 0 THEN REPLACE(full_name, 'Jennifer', 'Jen')
        -- Typo Introduction (10% chance within name variations)
        WHEN RAND() < 0.1 AND LENGTH(full_name) > 3 THEN
          CONCAT(
            SUBSTR(full_name, 1, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(full_name, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 3 AS INT64))
          )
        ELSE full_name
      END
    ELSE full_name
  END AS full_name,

  -- Email Variation (20% chance of domain change)
  CASE
    WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
      ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
    )
    ELSE email
  END AS email,

  -- Phone Variation (30% chance of format change) - Complete 5 formats
  -- Combined with Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 1 THEN NULL  -- Null phone
    WHEN RAND() < 0.3 THEN
      CASE CAST(FLOOR(RAND() * 5) AS INT64)
        WHEN 0 THEN REPLACE(phone, '-', '.')
        WHEN 1 THEN REPLACE(phone, '-', ' ')
        WHEN 2 THEN REPLACE(phone, '-', '')
        WHEN 3 THEN CONCAT('(', SUBSTR(phone, 1, 3), ') ', SUBSTR(phone, 5, 3), '-', SUBSTR(phone, 9))
        ELSE phone
      END
    ELSE phone
  END AS phone,

  -- Address Variation (30% chance, all 7 variations with nested probability and typos)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        -- Apply individual address variations (40% chance each, matching Python)
        WHEN RAND() < 0.4 THEN
          -- Chain multiple replacements to handle all variations
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            address,
            'Street', 'St'),
            'Avenue', 'Ave'),
            'Boulevard', 'Blvd'),
            'Road', 'Rd'),
            'Drive', 'Dr'),
            'Apartment', 'Apt'),
            'Suite', 'Ste')
        -- Address Typo Introduction (10% chance within address variations)
        WHEN RAND() < 0.1 AND LENGTH(address) > 5 THEN
          CONCAT(
            SUBSTR(address, 1, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(address, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 3 AS INT64))
          )
        ELSE address
      END
    ELSE address
  END AS address,

  city,
  state,
  zip_code,
  date_of_birth,

  -- Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 2 THEN NULL  -- Null company
    ELSE company
  END AS company,

  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 3 THEN NULL  -- Null job_title
    ELSE job_title
  END AS job_title,

  annual_income,
  customer_segment,
  registration_date,
  last_activity_date,
  is_active,

  -- ERP-specific fields (randomized to match Python)
  CONCAT('ACC', CAST(FLOOR(RAND() * 900000) + 100000 AS INT64)) AS account_number,
  CAST(FLOOR(RAND() * 49001) + 1000 AS INT64) AS credit_limit,
  ['Net 30', 'Net 60', 'COD', 'Prepaid'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))] AS payment_terms,
  ['Active', 'Suspended', 'Closed'][OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))] AS account_status

FROM (
  SELECT *,
    -- Missing data simulation variables (single random decision per record)
    RAND() AS missing_data_rand,
    CAST(FLOOR(RAND() * 3) + 1 AS INT64) AS missing_data_choice  -- 1=phone, 2=company, 3=job_title
  FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale`
);


-- =================================================================================
-- 3. Process E-commerce Data
-- =================================================================================

-- Step 3.1: Create the new E-commerce table with explicit E-commerce schema (26 columns matching Spark)
CREATE OR REPLACE TABLE `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale_x3` AS
SELECT
  -- Base fields (21 columns)
  customer_id,
  source_id,
  source_system,
  record_id,
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
  is_active,
  -- E-commerce-specific fields (5 columns)
  username,
  total_orders,
  total_spent,
  preferred_category,
  marketing_opt_in
FROM `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale`;

-- Step 3.2: Insert the first set of varied duplicates (Enhanced to match Python logic)
INSERT INTO `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale_x3`
SELECT
  -- Core Identifiers
  customer_id,
  CONCAT('EC_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
  source_system,
  GENERATE_UUID() AS record_id,

  -- Personal Info with Variations (matching CRM/ERP enhancements)
  first_name,
  last_name,

  -- Name Variations (30% chance, matching Python logic)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'John') > 0 THEN REPLACE(full_name, 'John', 'Jon')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Michael') > 0 THEN REPLACE(full_name, 'Michael', 'Mike')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'William') > 0 THEN REPLACE(full_name, 'William', 'Bill')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Robert') > 0 THEN REPLACE(full_name, 'Robert', 'Bob')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'James') > 0 THEN REPLACE(full_name, 'James', 'Jim')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Christopher') > 0 THEN REPLACE(full_name, 'Christopher', 'Chris')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Matthew') > 0 THEN REPLACE(full_name, 'Matthew', 'Matt')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Anthony') > 0 THEN REPLACE(full_name, 'Anthony', 'Tony')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Elizabeth') > 0 THEN REPLACE(full_name, 'Elizabeth', 'Liz')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Jennifer') > 0 THEN REPLACE(full_name, 'Jennifer', 'Jen')
        -- Typo Introduction (10% chance within name variations)
        WHEN RAND() < 0.1 AND LENGTH(full_name) > 3 THEN
          CONCAT(
            SUBSTR(full_name, 1, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(full_name, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 3 AS INT64))
          )
        ELSE full_name
      END
    ELSE full_name
  END AS full_name,

  -- Email Variation (20% chance of domain change)
  CASE
    WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
      ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
    )
    ELSE email
  END AS email,

  -- Phone Variation (30% chance of format change) - Complete 5 formats
  -- Combined with Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 1 THEN NULL  -- Null phone
    WHEN RAND() < 0.3 THEN
      CASE CAST(FLOOR(RAND() * 5) AS INT64)
        WHEN 0 THEN REPLACE(phone, '-', '.')
        WHEN 1 THEN REPLACE(phone, '-', ' ')
        WHEN 2 THEN REPLACE(phone, '-', '')
        WHEN 3 THEN CONCAT('(', SUBSTR(phone, 1, 3), ') ', SUBSTR(phone, 5, 3), '-', SUBSTR(phone, 9))
        ELSE phone
      END
    ELSE phone
  END AS phone,

  -- Address Variation (30% chance, all 7 variations with nested probability and typos)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        -- Apply individual address variations (40% chance each, matching Python)
        WHEN RAND() < 0.4 THEN
          -- Chain multiple replacements to handle all variations
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            address,
            'Street', 'St'),
            'Avenue', 'Ave'),
            'Boulevard', 'Blvd'),
            'Road', 'Rd'),
            'Drive', 'Dr'),
            'Apartment', 'Apt'),
            'Suite', 'Ste')
        -- Address Typo Introduction (10% chance within address variations)
        WHEN RAND() < 0.1 AND LENGTH(address) > 5 THEN
          CONCAT(
            SUBSTR(address, 1, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(address, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 3 AS INT64))
          )
        ELSE address
      END
    ELSE address
  END AS address,

  city,
  state,
  zip_code,
  date_of_birth,

  -- Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 2 THEN NULL  -- Null company
    ELSE company
  END AS company,

  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 3 THEN NULL  -- Null job_title
    ELSE job_title
  END AS job_title,

  annual_income,
  customer_segment,
  registration_date,
  last_activity_date,
  is_active,

  -- E-commerce specific fields (randomized to match Python)
  -- Generate new username (simplified approach)
  CONCAT(
    ['user', 'shopper', 'buyer', 'customer', 'member'][OFFSET(CAST(FLOOR(RAND() * 5) AS INT64))],
    CAST(FLOOR(RAND() * 10000) AS STRING)
  ) AS username,

  CAST(FLOOR(RAND() * 50) + 1 AS INT64) AS total_orders,
  ROUND(RAND() * 4950 + 50, 2) AS total_spent,
  ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'][OFFSET(CAST(FLOOR(RAND() * 5) AS INT64))] AS preferred_category,
  RAND() < 0.5 AS marketing_opt_in

FROM (
  SELECT *,
    -- Missing data simulation variables (single random decision per record)
    RAND() AS missing_data_rand,
    CAST(FLOOR(RAND() * 3) + 1 AS INT64) AS missing_data_choice  -- 1=phone, 2=company, 3=job_title
  FROM `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale`
);

-- Step 3.3: Insert the second set of varied duplicates (Enhanced to match Python logic)
INSERT INTO `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale_x3`
SELECT
  -- Core Identifiers
  customer_id,
  CONCAT('EC_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
  source_system,
  GENERATE_UUID() AS record_id,

  -- Personal Info with Variations (matching CRM/ERP enhancements)
  first_name,
  last_name,

  -- Name Variations (30% chance, matching Python logic)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'John') > 0 THEN REPLACE(full_name, 'John', 'Jon')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Michael') > 0 THEN REPLACE(full_name, 'Michael', 'Mike')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'William') > 0 THEN REPLACE(full_name, 'William', 'Bill')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Robert') > 0 THEN REPLACE(full_name, 'Robert', 'Bob')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'James') > 0 THEN REPLACE(full_name, 'James', 'Jim')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Christopher') > 0 THEN REPLACE(full_name, 'Christopher', 'Chris')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Matthew') > 0 THEN REPLACE(full_name, 'Matthew', 'Matt')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Anthony') > 0 THEN REPLACE(full_name, 'Anthony', 'Tony')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Elizabeth') > 0 THEN REPLACE(full_name, 'Elizabeth', 'Liz')
        WHEN RAND() < 0.3 AND STRPOS(full_name, 'Jennifer') > 0 THEN REPLACE(full_name, 'Jennifer', 'Jen')
        -- Typo Introduction (10% chance within name variations)
        WHEN RAND() < 0.1 AND LENGTH(full_name) > 3 THEN
          CONCAT(
            SUBSTR(full_name, 1, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(full_name, CAST(FLOOR(RAND() * (LENGTH(full_name) - 2)) + 3 AS INT64))
          )
        ELSE full_name
      END
    ELSE full_name
  END AS full_name,

  -- Email Variation (20% chance of domain change)
  CASE
    WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
      ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
    )
    ELSE email
  END AS email,

  -- Phone Variation (30% chance of format change) - Complete 5 formats
  -- Combined with Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 1 THEN NULL  -- Null phone
    WHEN RAND() < 0.3 THEN
      CASE CAST(FLOOR(RAND() * 5) AS INT64)
        WHEN 0 THEN REPLACE(phone, '-', '.')
        WHEN 1 THEN REPLACE(phone, '-', ' ')
        WHEN 2 THEN REPLACE(phone, '-', '')
        WHEN 3 THEN CONCAT('(', SUBSTR(phone, 1, 3), ') ', SUBSTR(phone, 5, 3), '-', SUBSTR(phone, 9))
        ELSE phone
      END
    ELSE phone
  END AS phone,

  -- Address Variation (30% chance, all 7 variations with nested probability and typos)
  CASE
    WHEN RAND() < 0.3 THEN
      CASE
        -- Apply individual address variations (40% chance each, matching Python)
        WHEN RAND() < 0.4 THEN
          -- Chain multiple replacements to handle all variations
          REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            address,
            'Street', 'St'),
            'Avenue', 'Ave'),
            'Boulevard', 'Blvd'),
            'Road', 'Rd'),
            'Drive', 'Dr'),
            'Apartment', 'Apt'),
            'Suite', 'Ste')
        -- Address Typo Introduction (10% chance within address variations)
        WHEN RAND() < 0.1 AND LENGTH(address) > 5 THEN
          CONCAT(
            SUBSTR(address, 1, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 1 AS INT64)),
            ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'][OFFSET(CAST(FLOOR(RAND() * 26) AS INT64))],
            SUBSTR(address, CAST(FLOOR(RAND() * (LENGTH(address) - 2)) + 3 AS INT64))
          )
        ELSE address
      END
    ELSE address
  END AS address,

  city,
  state,
  zip_code,
  date_of_birth,

  -- Missing Data Simulation (15% chance, single field selection matching Python logic)
  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 2 THEN NULL  -- Null company
    ELSE company
  END AS company,

  CASE
    WHEN missing_data_rand < 0.15 AND missing_data_choice = 3 THEN NULL  -- Null job_title
    ELSE job_title
  END AS job_title,

  annual_income,
  customer_segment,
  registration_date,
  last_activity_date,
  is_active,

  -- E-commerce specific fields (randomized to match Python)
  -- Generate new username (simplified approach)
  CONCAT(
    ['user', 'shopper', 'buyer', 'customer', 'member'][OFFSET(CAST(FLOOR(RAND() * 5) AS INT64))],
    CAST(FLOOR(RAND() * 10000) AS STRING)
  ) AS username,

  CAST(FLOOR(RAND() * 50) + 1 AS INT64) AS total_orders,
  ROUND(RAND() * 4950 + 50, 2) AS total_spent,
  ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'][OFFSET(CAST(FLOOR(RAND() * 5) AS INT64))] AS preferred_category,
  RAND() < 0.5 AS marketing_opt_in

FROM (
  SELECT *,
    -- Missing data simulation variables (single random decision per record)
    RAND() AS missing_data_rand,
    CAST(FLOOR(RAND() * 3) + 1 AS INT64) AS missing_data_choice  -- 1=phone, 2=company, 3=job_title
  FROM `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale`
);

-- =================================================================================
-- Verification
-- =================================================================================
-- Run these queries to check the new row counts.

-- SELECT
--   'CRM' as source,
--   (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale`) as original_count,
--   (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale_x3`) as new_count
-- UNION ALL
-- SELECT
--   'ERP' as source,
--   (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale`) as original_count,
--   (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale_x3`) as new_count
-- UNION ALL
-- SELECT
--   'E-commerce' as source,
--   (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale`) as original_count,
--   (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale_x3`) as new_count;
