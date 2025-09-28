# MDM Pipeline: Technical & Business Glossary

This document provides a detailed breakdown of the MDM pipeline, mapping each processing stage to the corresponding function in `batch_mdm_gcp/bigquery_utils.py`. It serves as a glossary for both technical and business users to understand how each logic is implemented in BigQuery.

---

### 1. **Data Standardization**
- **Function:** `generate_standardization_sql` (Line: 113)
- **Description:** This function generates SQL to clean and standardize raw customer data. The logic includes:
    - **Name Normalization:** Converts names to uppercase and removes special characters using `TRIM`, `UPPER`, and `REGEXP_REPLACE`.
    - **Email Cleaning:** Converts emails to lowercase and trims whitespace using `LOWER` and `TRIM`.
    - **Phone Standardization:** Removes all non-numeric characters using `REGEXP_REPLACE`.
    - **Address Normalization:** Standardizes common address abbreviations (e.g., 'STREET' to 'ST') and converts to uppercase using nested `REGEXP_REPLACE` calls.

---

### 2. **AI-Powered Embeddings**
- **Function:** `generate_embedding_sql` (Line: 288)
- **Description:** This function calls the `gemini-embedding-001` model to create vector embeddings.
    - **Content Aggregation:** It first concatenates key fields (`full_name_clean`, `email_clean`, `address_clean`, etc.) into a single `content` string for the model.
    - **`ML.GENERATE_EMBEDDING` Call:** It passes the aggregated content to the model to generate a 768-dimensional vector for each record, representing its semantic meaning.

---

### 3. **Matching Engine**

#### 3.1. **Exact Matching**
- **Function:** `generate_exact_matching_sql` (Line: 311)
- **Description:** Performs high-precision matching on key identifiers.
    - **`CROSS JOIN`:** Creates all possible pairs of records, optimized with `a.record_id < b.record_id` to avoid duplicates and self-matches.
    - **`CASE` Statements:** Assigns a score of `1.0` if there is an exact match on `email_clean`, `phone_clean`, or `customer_id`.
    - **`GREATEST`:** The final `exact_overall_score` is the highest score from the email, phone, and ID checks.

#### 3.2. **Fuzzy Matching**
- **Function:** `generate_fuzzy_matching_sql` (Line: 349)
- **Description:** Handles variations in names and addresses.
    - **`EDIT_DISTANCE` (Levenshtein):** Calculates a normalized similarity score based on the character-level distance between two strings.
    - **`SOUNDEX`:** A phonetic algorithm that assigns a score of `0.8` if the `SOUNDEX` codes of two names match.
    - **Token-Based Matching:** Splits names into words ("tokens") and calculates a similarity score based on the proportion of shared tokens.
    - **Score Combination:** The final `name_fuzzy_score` is the `GREATEST` of the three name-matching techniques. The `fuzzy_overall_score` is the average of the name score and the address score.

#### 3.3. **Vector Matching**
- **Function:** `generate_vector_matching_sql` (Line: 401)
- **Description:** Finds semantically similar records using vector embeddings.
    - **`COSINE_DISTANCE`:** Calculates the distance between the vector embeddings of two records. The query filters for pairs with a distance `< 0.3` to focus on highly similar records.
    - **Similarity Conversion:** The distance is converted to a `vector_similarity_score` using the formula `1 - COSINE_DISTANCE`.

#### 3.4. **Business Rules**
- **Function:** `generate_business_rules_sql` (Line: 421)
- **Description:** Applies domain-specific logic to identify matches.
    - **Rule Scoring:** Assigns partial scores for matches on `company`, `location` (city/state), `age` (within 1 or 5 years), and `income` (within 20% of each other).
    - **Score Aggregation:** Unlike other strategies, these scores are additive, meaning a pair matching on multiple business rules will get a higher combined business score.

#### 3.5. **AI Natural Language Matching**
- **Function:** `generate_ai_natural_language_matching_sql` (Line: 477)
- **Description:** Uses a Large Language Model for human-like comparison.
    - **Step 1: `customer_pairs` CTE:** Creates a small, testable batch of record pairs (e.g., `LIMIT 500`) to send to the AI model.
    - **Step 2: `ai_matches` CTE:** Calls the `gemini-2.5-pro` model using `AI.GENERATE_TABLE`. It constructs a detailed prompt for each pair and specifies the desired `output_schema` (`similarity_score`, `confidence`, `explanation`).
    - **Step 3: Final `SELECT`:** Cleans the AI's output, ensures scores are within the valid 0-1 range, and filters for results where the AI is confident (`confidence > 0.6`).

---

### 4. **Scoring & Decision**
- **Function:** `generate_combined_scoring_sql` (Line: 560)
- **Description:** Creates a final, weighted ensemble score from all five strategies.
    - **Step 1: `all_pairs` CTE:** Gathers a unique list of all record pairs that were identified as a potential match by *any* of the five strategies.
    - **Step 2: `combined_scores` CTE:**
        - Uses `LEFT JOIN` to retrieve the score for each pair from each of the five match tables.
        - Uses `COALESCE(..., 0.0)` to ensure that if a pair wasn't found by a strategy, it gets a score of 0 for that strategy.
        - Calculates the final `combined_score` using a weighted average: **Exact (30%), Fuzzy (25%), Vector (20%), Business (15%), and AI (10%)**.
    - **Step 3: Final `SELECT` & Decision Logic:**
        - Assigns a `match_decision` (`auto_merge`, `human_review`, `no_match`) based on predefined thresholds for the `combined_score`.

---

### 5. **Golden Record Creation**
- **Function:** `generate_golden_record_sql` (Line: 645)
- **Description:** This final, multi-step process clusters matching records and applies survivorship rules to create a single "golden" record for each unique entity.
    - **Step 1: `match_pairs`:** Selects high-confidence pairs (`score >= 0.6`) to be used for clustering.
    - **Step 2: `edges`:** Treats each pair as a connection (or "edge") in a graph, creating bidirectional links for graph traversal.
    - **Step 3-5: Clustering (`initial_clusters`, `propagated_clusters`, `final_clusters`):** This implements a **transitive closure**. It finds all records connected to each other, even indirectly (e.g., if A matches B, and B matches C, then A, B, and C all belong to the same cluster). The smallest `record_id` in the component becomes the `cluster_id`.
    - **Step 6: `golden_records_raw` (Survivorship):** For each cluster, this step selects the "best" attribute values to survive to the golden record. It uses `ARRAY_AGG` with `ORDER BY` and `LIMIT 1` to implement rules like "use the most complete name" or "use the most recent email."
    - **Step 7: `golden_records` (Deterministic ID):** Generates a stable, unique `master_id` for each golden record. It uses a hash of the best available identifier (email, then phone) to ensure the ID is consistent across pipeline runs.
