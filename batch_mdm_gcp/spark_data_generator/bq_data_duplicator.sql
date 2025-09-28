-- =================================================================================
-- BigQuery Script for Dynamic MDM Source Data Scaling with Variations
--
-- Description:
--   This script takes the existing `_scale` tables (CRM, ERP, E-commerce) and
--   creates new tables with configurable scaling. It uses FOR loops to create
--   multiple varied copies of each record based on the scale_multiplier setting.
--
--   This process mimics the variation logic of the Python data generators,
--   providing a fast and cost-effective way to scale up test data without
--   rerunning the Spark job.
--
-- Instructions:
--   1. Set the configuration variables below.
--   2. Run the entire script to create _scale_max tables with scaled data.
-- =================================================================================

-- =================================================================================
-- Configuration
-- =================================================================================
DECLARE project_id STRING DEFAULT 'johanesa-playground-326616';
DECLARE dataset_id STRING DEFAULT 'mdm_demo';
DECLARE source_table_suffix STRING DEFAULT '_scale';
DECLARE target_table_suffix STRING DEFAULT '_scale_max';
DECLARE scale_multiplier INT64 DEFAULT 10;  -- Change this to scale data: 3, 5, 10, 20, etc.

-- Loop iteration variables (must be declared at script start)
DECLARE iteration INT64 DEFAULT 2;
DECLARE iteration2 INT64 DEFAULT 2;
DECLARE iteration3 INT64 DEFAULT 2;

-- Validation
IF scale_multiplier < 2 THEN
  RAISE USING MESSAGE = 'Scale multiplier must be ≥ 2 (original + at least 1 copy)';
END IF;

-- =================================================================================
-- 1. Process CRM Data
-- =================================================================================

-- Step 1.1: Create the new CRM table with original data (1x)
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.%s.raw_crm_customers%s` AS
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
FROM `%s.%s.raw_crm_customers%s`
""", project_id, dataset_id, target_table_suffix, project_id, dataset_id, source_table_suffix);

-- Step 1.2-1.N: Add varied copies using WHILE loop
WHILE iteration <= scale_multiplier DO
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.%s.raw_crm_customers%s`
    SELECT
      -- Core Identifiers
      customer_id,
      CONCAT('CRM_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
      source_system,
      GENERATE_UUID() AS record_id,

      -- Personal Info with Variations
      first_name,
      last_name,

      -- Name Variations (30%% chance, matching Python logic)
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
            -- Typo Introduction (10%% chance within name variations)
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

      -- Email Variation (20%% chance of domain change)
      CASE
        WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
          ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
        )
        ELSE email
      END AS email,

      -- Phone Variation with Missing Data Simulation
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

      -- Address Variation (30%% chance, all 7 variations with nested probability and typos)
      CASE
        WHEN RAND() < 0.3 THEN
          CASE
            WHEN RAND() < 0.4 THEN
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                address,
                'Street', 'St'),
                'Avenue', 'Ave'),
                'Boulevard', 'Blvd'),
                'Road', 'Rd'),
                'Drive', 'Dr'),
                'Apartment', 'Apt'),
                'Suite', 'Ste')
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

      -- Missing Data Simulation (15%% chance, single field selection matching Python logic)
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

      -- Generate new sales rep name
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
      FROM `%s.%s.raw_crm_customers%s`
    )
  """, project_id, dataset_id, target_table_suffix, project_id, dataset_id, source_table_suffix);

  SELECT FORMAT('✅ CRM: Completed iteration %d of %d', iteration, scale_multiplier) AS progress;
  SET iteration = iteration + 1;
END WHILE;

-- =================================================================================
-- 2. Process ERP Data
-- =================================================================================

-- Step 2.1: Create the new ERP table with original data (1x)
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.%s.raw_erp_customers%s` AS
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
FROM `%s.%s.raw_erp_customers%s`
""", project_id, dataset_id, target_table_suffix, project_id, dataset_id, source_table_suffix);

-- Step 2.2-2.N: Add varied copies using WHILE loop
WHILE iteration2 <= scale_multiplier DO
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.%s.raw_erp_customers%s`
    SELECT
      -- Core Identifiers
      customer_id,
      CONCAT('ERP_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
      source_system,
      GENERATE_UUID() AS record_id,

      -- Personal Info with Variations (same as CRM)
      first_name,
      last_name,

      -- Name Variations (30%% chance, matching Python logic)
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

      -- Email Variation (20%% chance of domain change)
      CASE
        WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
          ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
        )
        ELSE email
      END AS email,

      -- Phone Variation with Missing Data Simulation
      CASE
        WHEN missing_data_rand < 0.15 AND missing_data_choice = 1 THEN NULL
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

      -- Address Variation (same as CRM)
      CASE
        WHEN RAND() < 0.3 THEN
          CASE
            WHEN RAND() < 0.4 THEN
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                address,
                'Street', 'St'),
                'Avenue', 'Ave'),
                'Boulevard', 'Blvd'),
                'Road', 'Rd'),
                'Drive', 'Dr'),
                'Apartment', 'Apt'),
                'Suite', 'Ste')
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

      -- Missing Data Simulation
      CASE
        WHEN missing_data_rand < 0.15 AND missing_data_choice = 2 THEN NULL
        ELSE company
      END AS company,

      CASE
        WHEN missing_data_rand < 0.15 AND missing_data_choice = 3 THEN NULL
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
        RAND() AS missing_data_rand,
        CAST(FLOOR(RAND() * 3) + 1 AS INT64) AS missing_data_choice
      FROM `%s.%s.raw_erp_customers%s`
    )
  """, project_id, dataset_id, target_table_suffix, project_id, dataset_id, source_table_suffix);

  SELECT FORMAT('✅ ERP: Completed iteration %d of %d', iteration2, scale_multiplier) AS progress;
  SET iteration2 = iteration2 + 1;
END WHILE;

-- =================================================================================
-- 3. Process E-commerce Data
-- =================================================================================

-- Step 3.1: Create the new E-commerce table with original data (1x)
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `%s.%s.raw_ecommerce_customers%s` AS
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
FROM `%s.%s.raw_ecommerce_customers%s`
""", project_id, dataset_id, target_table_suffix, project_id, dataset_id, source_table_suffix);

-- Step 3.2-3.N: Add varied copies using WHILE loop
WHILE iteration3 <= scale_multiplier DO
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.%s.raw_ecommerce_customers%s`
    SELECT
      -- Core Identifiers
      customer_id,
      CONCAT('EC_', CAST(FLOOR(RAND() * 90000) + 10000 AS INT64)) AS source_id,
      source_system,
      GENERATE_UUID() AS record_id,

      -- Personal Info with Variations (same as CRM/ERP)
      first_name,
      last_name,

      -- Name Variations (30%% chance, matching Python logic)
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

      -- Email Variation (20%% chance of domain change)
      CASE
        WHEN RAND() < 0.2 THEN CONCAT(SPLIT(email, '@')[OFFSET(0)], '@',
          ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'][OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))]
        )
        ELSE email
      END AS email,

      -- Phone Variation with Missing Data Simulation
      CASE
        WHEN missing_data_rand < 0.15 AND missing_data_choice = 1 THEN NULL
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

      -- Address Variation (same as CRM/ERP)
      CASE
        WHEN RAND() < 0.3 THEN
          CASE
            WHEN RAND() < 0.4 THEN
              REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                address,
                'Street', 'St'),
                'Avenue', 'Ave'),
                'Boulevard', 'Blvd'),
                'Road', 'Rd'),
                'Drive', 'Dr'),
                'Apartment', 'Apt'),
                'Suite', 'Ste')
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

      -- Missing Data Simulation
      CASE
        WHEN missing_data_rand < 0.15 AND missing_data_choice = 2 THEN NULL
        ELSE company
      END AS company,

      CASE
        WHEN missing_data_rand < 0.15 AND missing_data_choice = 3 THEN NULL
        ELSE job_title
      END AS job_title,

      annual_income,
      customer_segment,
      registration_date,
      last_activity_date,
      is_active,

      -- E-commerce specific fields (randomized to match Python)
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
        RAND() AS missing_data_rand,
        CAST(FLOOR(RAND() * 3) + 1 AS INT64) AS missing_data_choice
      FROM `%s.%s.raw_ecommerce_customers%s`
    )
  """, project_id, dataset_id, target_table_suffix, project_id, dataset_id, source_table_suffix);

  SELECT FORMAT('✅ E-commerce: Completed iteration %d of %d', iteration3, scale_multiplier) AS progress;
  SET iteration3 = iteration3 + 1;
END WHILE;

-- =================================================================================
-- Verification and Summary
-- =================================================================================

-- Final Summary
SELECT
  FORMAT('🎉 Successfully scaled data by %dx multiplier', scale_multiplier) AS completion_message,
  FORMAT('📊 Created tables: raw_*_customers%s', target_table_suffix) AS tables_created;

-- Verification Queries (uncomment to run)
/*
SELECT
  'CRM' as source,
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale`) as original_count,
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale_max`) as new_count,
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale_max`) /
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_crm_customers_scale`) as multiplier_achieved
UNION ALL
SELECT
  'ERP' as source,
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale`) as original_count,
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale_max`) as new_count,
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale_max`) /
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_erp_customers_scale`) as multiplier_achieved
UNION ALL
SELECT
  'E-commerce' as source,
  (SELECT COUNT(*) FROM `johanesa-playground-326616.mdm_demo.raw_ecommerce_customers_scale`) as original_count,
  (SELECT COUNT(*) FROM `johanesa-playgroun
*/
