---
marp: false
theme: gaia
paginate: true
backgroundColor: #fff
backgroundImage: url('https://marp.app/assets/hero-background.svg')
header: 'Modern MDM Architecture on Google Cloud'
footer: 'Google Cloud Platform | Master Data Management'
---

<!-- _class: lead -->
# Modern MDM Architecture on Google Cloud

## Building Enterprise-Grade Master Data Management Solutions

**A comprehensive blueprint for data architects and engineers**

---

## Agenda

- **Why Master Data Management?**
- **Architecture Showdown: Siloed vs. Unified**
- **Deep Dive: The BigQuery 5-Strategy Batch Pipeline**
- **The Real-Time Complement: The Streaming Journey**
- **Bringing It All Together: The Hybrid Lambda Architecture**
- **Summary & Next Steps**

---

<!-- _class: lead -->
# Why Master Data Management?

---

## The Data Fragmentation Challenge

Organizations struggle with:

- **Multiple versions** of the same entity across systems
- **Inconsistent data quality** leading to poor decisions
- **Manual reconciliation** efforts that are time-consuming
- **Compliance risks** from inaccurate data

**Result:** Fragmented view of critical business entities

---

## MDM Value Proposition

MDM creates a **single source of truth** enabling:

- **Improved Decision Making** - Reliable analytics
- **Operational Efficiency** - Reduced errors
- **Enhanced Customer Experience** - 360-degree view
- **Regulatory Compliance** - Accurate reporting
- **Cost Reduction** - Eliminate manual reconciliation

---

<!-- _class: lead -->
# Architecture Showdown: Siloed vs. Unified

---

## Traditional MDM: Complex & Siloed

```mermaid
graph TD
    subgraph Sources["A. Data Sources"]
        S1[CRM]
        S2[ERP]
        S3[E-commerce]
    end

    subgraph ETL["B. ETL/Ingestion Layer"]
        ETL_TOOL["ETL Compute Tool - Informatica PowerCenter, Talend - BigQuery Equivalent: generate_union_sql() + load_dataframe_to_table()"]
    end

    subgraph Storage["C. Storage Layer"]
        DB["MDM Database - Oracle, SQL Server - BigQuery Equivalent: BigQuery Tables (Raw & Golden Records)"]
    end

    subgraph Processing["D. Processing Layer"]
        DQ_TOOL["Data Quality Compute - Standardization & Cleansing - BigQuery Equivalent: generate_standardization_sql()"]
        MATCH_ENGINE["MDM Matching Engine Compute - BigQuery Equivalent: All generate_matching_sql functions"]
    end

    Sources --> ETL_TOOL
    ETL_TOOL --> DB
    DB --> DQ_TOOL
    DQ_TOOL --> DB
    DB --> MATCH_ENGINE
    MATCH_ENGINE --> DB

    classDef silo fill:#ffebee,stroke:#c62828,stroke-width:2px
    class ETL_TOOL,DB,DQ_TOOL,MATCH_ENGINE silo
```

**Problems:** Data movement overhead, multiple licenses, complex infrastructure

**[Detailed Comparison: BigQuery vs. Traditional MDM](./batch_mdm_gcp/MDM_BATCH_COMPARISON.md)**

---

## BigQuery-Native: Unified & Simple

```mermaid
graph TD
    subgraph Sources["A. Data Sources"]
        S1[CRM]
        S2[ERP]
        S3[E-commerce]
    end

    subgraph BigQuery["B. Unified BigQuery Platform"]
        BQ_STORAGE["Storage - Raw & Golden Record Tables - Replaces: MDM Database"]
        BQ_ETL["ETL & Standardization - generate_union_sql() + generate_standardization_sql() - Replaces: ETL Tool + Data Quality Tool"]
        BQ_MATCH["5-Way Matching Engine - All generate_matching_sql functions - Replaces: MDM Matching Engine"]
        BQ_GOLDEN["Golden Record Creation - generate_golden_record_sql() - Replaces: Survivorship Rule Engine"]
    end

    Sources --> BQ_STORAGE
    BQ_STORAGE --> BQ_ETL
    BQ_ETL --> BQ_MATCH
    BQ_MATCH --> BQ_GOLDEN
    BQ_GOLDEN --> BQ_STORAGE

    classDef unified fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    class BQ_STORAGE,BQ_ETL,BQ_MATCH,BQ_GOLDEN unified
```

**Benefits:** Single platform, no data movement, simple Python functions

---

<!-- _class: lead -->
# Deep Dive: The BigQuery 5-Strategy Batch Pipeline

---

## Technical Architecture

```mermaid
graph TB
    subgraph GCP["Google Cloud Platform"]
        subgraph BigQuery["BigQuery"]
            RAW["Raw Tables Per Source"]
            STAGE["Staging Tables Standardized"]
            EMBED_TBL["Embedding Tables With Vectors"]

            subgraph MatchTables["5-Strategy Match Tables"]
                EXACT_TBL["Exact Matches - Email, Phone, ID"]
                FUZZY_TBL["Fuzzy Matches - Name, Address"]
                VECTOR_TBL["Vector Matches - Semantic Similarity"]
                BUSINESS_TBL["Business Matches - Rules & Logic"]
                AI_TBL["AI Natural Language - Direct Comparison"]
            end

            COMBINED["Combined Matches - 5-Strategy Ensemble"]
            GOLDEN["Golden Records - Master Entities"]
        end

        subgraph VertexAI["Vertex AI"]
            GEMINI["Gemini Embedding - gemini-embedding-001 - Vector Generation"]
            GEMINI_PRO["Gemini 2.5 Pro - gemini-2.5-pro - Natural Language"]
        end

        subgraph Processing["Processing"]
            PYTHON["Python Package - batch_mdm_gcp"]
            NOTEBOOK["Jupyter Notebook - Demo Pipeline"]
        end
    end

    subgraph Outputs["Outputs"]
        BI["BI Platforms - Analytics"]
        OPS["Operational Apps - CRM, Marketing"]
        API["Real-time APIs - Customer 360"]
    end

    RAW --> STAGE
    STAGE --> GEMINI
    STAGE --> GEMINI_PRO
    GEMINI --> EMBED_TBL
    STAGE --> EXACT_TBL
    STAGE --> FUZZY_TBL
    EMBED_TBL --> VECTOR_TBL
    STAGE --> BUSINESS_TBL
    GEMINI_PRO --> AI_TBL
    EXACT_TBL --> COMBINED
    FUZZY_TBL --> COMBINED
    VECTOR_TBL --> COMBINED
    BUSINESS_TBL --> COMBINED
    AI_TBL --> COMBINED
    COMBINED --> GOLDEN
    PYTHON --> RAW
    NOTEBOOK --> PYTHON
    GOLDEN --> BI
    GOLDEN --> OPS
    GOLDEN --> API

    classDef bqStyle fill:#4285f4,color:#fff
    classDef aiStyle fill:#34a853,color:#fff
    classDef processStyle fill:#ea4335,color:#fff
    classDef outputStyle fill:#fbbc04,color:#000
    classDef matchStyle fill:#ff9800,color:#fff

    class RAW,STAGE,EMBED_TBL,COMBINED,GOLDEN bqStyle
    class EXACT_TBL,FUZZY_TBL,VECTOR_TBL,BUSINESS_TBL,AI_TBL matchStyle
    class GEMINI,GEMINI_PRO aiStyle
    class PYTHON,NOTEBOOK processStyle
    class BI,OPS,API outputStyle
```

**Technical Components:** 5-strategy matching with dual Gemini AI models

**[Demo Results & Performance Metrics](./batch_mdm_gcp/MDM_BATCH_RESULTS.md)**

---

<!-- _class: lead -->
# The Real-Time Complement: The Streaming Journey

---

## 4-Strategy Real-Time Processing

```mermaid
flowchart TD
    subgraph Input["Input"]
        A[Incoming Record from Kafka/PubSub]
    end

    subgraph Processing["StreamingMDMProcessor"]
        B["Step 1: Standardize - standardize_record()"]
        C["Step 2: 4-Way Matching"]
        C1["find_exact_matches()"]
        C2["find_fuzzy_matches()"]
        C3["find_vector_matches()"]
        C4["apply_business_rules()"]
        D["Step 3: Score & Decide - combine_scores() + make_decision()"]
        E{Decision?}
    end

    subgraph SpannerDB["Spanner Database"]
        subgraph New["New Record Path"]
            F_NEW["Step 4a: Generate ID - generate_deterministic_entity_id()"]
            G_NEW["Step 5a: Create Record - create_new_golden_record()"]
            I_NEW["Step 5b: Stage for Batch - stage_new_entity()"]
        end
        subgraph Merge["Existing Record Path"]
            F_MERGE["Step 4b: Get Existing ID"]
            G_MERGE["Step 5b: Update Record - update_golden_record()"]
        end
        H["Step 6: Log Result - store_match_result()"]
    end

    A --> B
    B --> C
    C --> C1
    C --> C2
    C --> C3
    C --> C4
    C1 --> D
    C2 --> D
    C3 --> D
    C4 --> D
    D --> E
    E -->|Score < 0.8 CREATE_NEW| F_NEW
    E -->|Score >= 0.8 AUTO_MERGE| F_MERGE
    F_NEW --> G_NEW
    F_MERGE --> G_MERGE
    G_NEW --> I_NEW
    I_NEW --> H
    G_MERGE --> H

    classDef newPath fill:#d4edda,stroke:#155724
    classDef mergePath fill:#fff3cd,stroke:#856404
    classDef exactMatch fill:#e3f2fd,stroke:#1565c0
    classDef fuzzyMatch fill:#e8f5e9,stroke:#2e7d32
    classDef vectorMatch fill:#fce4ec,stroke:#c2185b
    classDef businessMatch fill:#fff3e0,stroke:#e65100

    class F_NEW,G_NEW,I_NEW newPath
    class F_MERGE,G_MERGE mergePath
    class C1 exactMatch
    class C2 fuzzyMatch
    class C3 vectorMatch
    class C4 businessMatch
```

**Target:** Sub-400ms processing time for real-time operations

**[Detailed Guide: The Streaming Journey](./streaming_mdm_gcp/MDM_STREAMING_JOURNEY.md)**

---

<!-- _class: lead -->
# Bringing It All Together: The Hybrid Lambda Architecture

---

## Lambda Architecture: Speed + Accuracy

```mermaid
flowchart TD
    subgraph Sources["Data Sources - Lambda Architecture"]
        SRC1["Speed Layer - Streaming (Kafka/PubSub) - Spanner Staging"]
        SRC2["Batch Layer - Daily/Hourly Bulk Loads - CRM, ERP, E-commerce - Direct to BigQuery"]
    end

    subgraph BigQueryBatch["BigQuery Batch Layer - Full 5-Way Processing"]
        B1["Combine: Streaming + Bulk data"]
        B2["Clean and normalize all data"]
        B3["Generate high-quality Gemini embeddings"]
        B4["Run full 5-way matching - Exact, Fuzzy, Vector, Business, AI"]
        B5["Apply advanced survivorship rules - Generate deterministic entity_ids"]
    end

    subgraph WritePhase["Write Phase - Non-destructive"]
        W1["spanner_utils.py - update_golden_records_from_dataframe()"]
        W2["Execute Batch DML UPDATEs - SET embedding, master_address WHERE entity_id"]
        W3["Result: Enhanced golden records - entity_id remains stable"]
    end

    SRC1 --> B1
    SRC2 --> B1
    B1 --> B2
    B2 --> B3
    B3 --> B4
    B4 --> B5
    B5 --> W1
    W1 --> W2
    W2 --> W3

    classDef sourceStyle fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    classDef bqStyle fill:#4285f4,color:#fff,stroke-width:2px
    classDef writeStyle fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px

    class SRC1,SRC2 sourceStyle
    class B1,B2,B3,B4,B5 bqStyle
    class W1,W2,W3 writeStyle
```

**Best of Both Worlds:** Real-time speed + batch accuracy

**[Implementation Guide: Lambda Architecture Pattern B](./streaming_mdm_gcp/MDM_STREAMING_JOURNEY.md#part-4-implementing-the-batch-sync-pattern-b)**

---

## Key Benefits

### **Speed Layer (Spanner)**
- Sub-400ms latency for real-time operations
- Immediate availability for operational systems
- "Good enough" accuracy for live applications

### **Batch Layer (BigQuery)**
- Comprehensive 5-way matching with AI
- High-quality Gemini embeddings
- Advanced survivorship rules
- Processing of both streaming and bulk source data

### **Unified Benefits**
- Deterministic IDs prevent duplicates
- Non-destructive updates maintain stability
- Audit trail via processing_path field

---

<!-- _class: lead -->
# Summary & Next Steps

---

## Key Takeaways

✅ **Traditional MDM** is complex and expensive (multiple systems, data movement)
✅ **BigQuery-native** approach is unified and cost-effective (single platform, Python functions)
✅ **5-strategy matching** enables comprehensive entity resolution with AI
✅ **Streaming complement** provides real-time capabilities with sub-400ms latency
✅ **Lambda architecture** combines speed and accuracy in production

---

## Getting Started

1. **Start with Batch** - Implement BigQuery 5-strategy pipeline first
2. **Add Streaming** - Layer on Spanner for real-time requirements
3. **Implement Sync** - Set up Lambda architecture for best of both worlds
4. **Monitor & Tune** - Use comprehensive metrics for optimization

---

## Resources

- **GitHub Repository:** Complete implementation with notebooks
- **Interactive Demos:** Step-by-step Jupyter notebooks
- **Architecture Guides:** Detailed technical documentation
- **Production Utils:** Ready-to-use Python utilities

---

<!-- _class: lead -->
# Questions & Discussion

**Thank you!**

*Modern MDM Architecture on Google Cloud*
