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

- 🔄 **Multiple versions** of the same entity across systems
- 📉 **Inconsistent data quality** leading to poor decisions
- ⏰ **Manual reconciliation** efforts that are time-consuming
- ⚠️ **Compliance risks** from inaccurate data

**Result:** Fragmented view of critical business entities

---

## MDM Value Proposition

MDM creates a **single source of truth** enabling:

- 📊 **Improved Decision Making** - Reliable analytics
- ⚡ **Operational Efficiency** - Reduced errors
- 🎯 **Enhanced Customer Experience** - 360-degree view
- ✅ **Regulatory Compliance** - Accurate reporting
- 💰 **Cost Reduction** - Eliminate manual reconciliation

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
        ETL_TOOL["🔧 ETL Compute<br/>(e.g., Informatica PowerCenter, Talend)<br/><br/>📝 <b>BigQuery Equivalent:</b><br/>generate_union_sql()<br/>+ load_dataframe_to_table()"]
    end

    subgraph Storage["C. Storage Layer"]
        DB["🗄️ MDM Database<br/>(e.g., Oracle, SQL Server)<br/><br/>📝 <b>BigQuery Equivalent:</b><br/>BigQuery Tables<br/>(Raw & Golden Records)"]
    end

    subgraph Processing["D. Processing Layer"]
        DQ_TOOL["🧹 Data Quality Compute<br/>(Standardization & Cleansing)<br/><br/>📝 <b>BigQuery Equivalent:</b><br/>generate_standardization_sql()"]
        MATCH_ENGINE["🎯 MDM Matching Engine Compute<br/><br/>📝 <b>BigQuery Equivalent:</b><br/>generate_exact_matching_sql()<br/>generate_fuzzy_matching_sql()<br/>generate_vector_matching_sql()<br/>generate_business_rules_sql()<br/>generate_ai_natural_language_matching_sql()"]
    end

    %% Data Flow with Extract/Load labels
    Sources --> ETL_TOOL
    ETL_TOOL -- "Load" --> DB
    DB -- "Extract for Cleansing" --> DQ_TOOL
    DQ_TOOL -- "Load Cleansed Data" --> DB
    DB -- "Extract for Matching" --> MATCH_ENGINE
    MATCH_ENGINE -- "Write Match Results" --> DB

    %% Styling
    classDef silo fill:#ffebee,stroke:#c62828,stroke-width:2px
    class ETL_TOOL,DB,DQ_TOOL,MATCH_ENGINE silo
```

**❌ Problems:** Data movement overhead, multiple licenses, complex infrastructure

**📖 [Detailed Comparison: BigQuery vs. Traditional MDM](./batch_mdm_gcp/MDM_BATCH_COMPARISON.md)**

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
        BQ_STORAGE["<b>Storage</b><br/>Raw & Golden Record Tables<br/><i>Replaces: MDM Database</i>"]

        BQ_ETL["<b>ETL & Standardization</b><br/>generate_union_sql()<br/>generate_standardization_sql()<br/><i>Replaces: ETL Tool + Data Quality Tool</i>"]

        BQ_MATCH["<b>5-Way Matching Engine</b><br/>generate_exact_matching_sql()<br/>generate_fuzzy_matching_sql()<br/>generate_vector_matching_sql()<br/>generate_business_rules_sql()<br/>generate_ai_natural_language_matching_sql()<br/><i>Replaces: MDM Matching Engine</i>"]

        BQ_GOLDEN["<b>Golden Record Creation</b><br/>generate_golden_record_sql()<br/><i>Replaces: Survivorship Rule Engine</i>"]
    end

    %% Data Flow - Sequential Processing
    Sources -- "Load via ELT" --> BQ_STORAGE
    BQ_STORAGE --> BQ_ETL
    BQ_ETL --> BQ_MATCH
    BQ_MATCH --> BQ_GOLDEN
    BQ_GOLDEN --> BQ_STORAGE

    %% Styling
    classDef unified fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    class BQ_STORAGE,BQ_ETL,BQ_MATCH,BQ_GOLDEN unified
```

**✅ Benefits:** Single platform, no data movement, simple Python functions

---

<!-- _class: lead -->
# Deep Dive: The BigQuery 5-Strategy Batch Pipeline

---

## Technical Architecture

```mermaid
graph TB
    subgraph "🌐 Google Cloud Platform"
        subgraph "📊 BigQuery"
            RAW["Raw Tables<br/>Per Source"]
            STAGE["Staging Tables<br/>Standardized"]
            EMBED_TBL["Embedding Tables<br/>With Vectors"]

            subgraph "🎯 5-Strategy Match Tables"
                EXACT_TBL["Exact Matches<br/>Email, Phone, ID"]
                FUZZY_TBL["Fuzzy Matches<br/>Name, Address"]
                VECTOR_TBL["Vector Matches<br/>Semantic Similarity"]
                BUSINESS_TBL["Business Matches<br/>Rules & Logic"]
                AI_TBL["AI Natural Language<br/>Direct Comparison"]
            end

            COMBINED["Combined Matches<br/>5-Strategy Ensemble"]
            GOLDEN["Golden Records<br/>Master Entities"]
        end

        subgraph "🤖 Vertex AI"
            GEMINI["Gemini Embedding<br/>gemini-embedding-001<br/>Vector Generation"]
            GEMINI_PRO["Gemini 2.5 Pro<br/>gemini-2.5-pro<br/>Natural Language"]
        end

        subgraph "🔧 Processing"
            PYTHON["Python Package<br/>batch_mdm_gcp"]
            NOTEBOOK["Jupyter Notebook<br/>Demo Pipeline"]
        end
    end

    subgraph "📈 Outputs"
        BI["BI Platforms<br/>Analytics"]
        OPS["Operational Apps<br/>CRM, Marketing"]
        API["Real-time APIs<br/>Customer 360"]
    end

    %% Data Flow
    RAW --> STAGE

    %% Dual AI Processing
    STAGE --> GEMINI
    STAGE --> GEMINI_PRO
    GEMINI --> EMBED_TBL

    %% 5-Strategy Matching
    STAGE --> EXACT_TBL
    STAGE --> FUZZY_TBL
    EMBED_TBL --> VECTOR_TBL
    STAGE --> BUSINESS_TBL
    GEMINI_PRO --> AI_TBL

    %% Ensemble Combination
    EXACT_TBL --> COMBINED
    FUZZY_TBL --> COMBINED
    VECTOR_TBL --> COMBINED
    BUSINESS_TBL --> COMBINED
    AI_TBL --> COMBINED

    %% Golden Records
    COMBINED --> GOLDEN

    %% Processing Flow
    PYTHON --> RAW
    NOTEBOOK --> PYTHON

    %% Output Distribution
    GOLDEN --> BI
    GOLDEN --> OPS
    GOLDEN --> API

    %% Styling
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

**📖 [Demo Results & Performance Metrics](./batch_mdm_gcp/MDM_BATCH_RESULTS.md)**

---

<!-- _class: lead -->
# The Real-Time Complement: The Streaming Journey

---

## 4-Strategy Real-Time Processing

```mermaid
flowchart TD
    subgraph Input ["📨 Input"]
        A[Incoming Record<br/>e.g., from Kafka/PubSub]
    end

    subgraph Processing ["🔄 StreamingMDMProcessor"]
        B["Step 1: Standardize<br/>standardize_record()"]
        C["Step 2: 4-Way Matching"]
        C1["find_exact_matches()"]
        C2["find_fuzzy_matches()"]
        C3["find_vector_matches()"]
        C4["apply_business_rules()"]
        D["Step 3: Score & Decide<br/>combine_scores()<br/>make_decision()"]
        E{Decision?}
    end

    subgraph SpannerDB ["🗃️ Spanner Database"]
        subgraph New ["🆕 New Record Path"]
            F_NEW["Step 4a: Generate ID<br/>generate_deterministic_entity_id()"]
            G_NEW["Step 5a: Create Record<br/>create_new_golden_record()"]
            I_NEW["Step 5b: Stage for Batch<br/>stage_new_entity()"]
        end
        subgraph Merge ["🔄 Existing Record Path"]
            F_MERGE["Step 4b: Get Existing ID"]
            G_MERGE["Step 5b: Update Record<br/>update_golden_record()"]
        end
        H["Step 6: Log Result<br/>store_match_result()"]
    end

    A --> B
    B --> C
    C --> C1 & C2 & C3 & C4
    C1 & C2 & C3 & C4 --> D
    D --> E
    E -- "Score < 0.8<br/>(CREATE_NEW)" --> F_NEW
    E -- "Score ≥ 0.8<br/>(AUTO_MERGE)" --> F_MERGE
    F_NEW --> G_NEW
    F_MERGE --> G_MERGE
    G_NEW --> I_NEW
    I_NEW --> H
    G_MERGE --> H

    style F_NEW fill:#d4edda,stroke:#155724
    style F_MERGE fill:#fff3cd,stroke:#856404
    style C1 fill:#e3f2fd,stroke:#1565c0
    style C2 fill:#e8f5e9,stroke:#2e7d32
    style C3 fill:#fce4ec,stroke:#c2185b
    style C4 fill:#fff3e0,stroke:#e65100
```

**Target:** Sub-400ms processing time for real-time operations

**📖 [Detailed Guide: The Streaming Journey](./streaming_mdm_gcp/MDM_STREAMING_JOURNEY.md)**

---

<!-- _class: lead -->
# Bringing It All Together: The Hybrid Lambda Architecture

---

## Lambda Architecture: Speed + Accuracy

```mermaid
flowchart TD
    subgraph Sources["📊 Data Sources (Lambda Architecture)"]
        SRC1["Speed Layer<br/>📨 Streaming (Kafka/PubSub)<br/>↓<br/>🗃️ Spanner Staging"]
        SRC2["Batch Layer<br/>📦 Daily/Hourly Bulk Loads<br/>CRM, ERP, E-commerce<br/>↓<br/>💾 Direct to BigQuery"]
    end

    subgraph BigQueryBatch["🧠 BigQuery Batch Layer (Full 5-Way Processing)"]
        B1["Combine: Streaming + Bulk data"]
        B2["Clean and normalize all data"]
        B3["Generate high-quality Gemini embeddings"]
        B4["Run full 5-way matching<br/>(Exact, Fuzzy, Vector, Business, AI)"]
        B5["Apply advanced survivorship rules<br/>Generate deterministic entity_ids"]
    end

    subgraph WritePhase["✍️ Write Phase (Non-destructive)"]
        W1["spanner_utils.py<br/>update_golden_records_from_dataframe()"]
        W2["Execute Batch DML UPDATEs<br/>SET embedding = @new_embedding,<br/>master_address = @corrected_address<br/>WHERE entity_id = @entity_id"]
        W3["🎯 Result: Enhanced golden records<br/>(entity_id remains stable)"]
    end

    %% Flow connections
    SRC1 --> B1
    SRC2 --> B1
    B1 --> B2
    B2 --> B3
    B3 --> B4
    B4 --> B5
    B5 --> W1
    W1 --> W2
    W2 --> W3

    %% Styling
    classDef sourceStyle fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    classDef bqStyle fill:#4285f4,color:#fff,stroke-width:2px
    classDef writeStyle fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px

    class SRC1,SRC2 sourceStyle
    class B1,B2,B3,B4,B5 bqStyle
    class W1,W2,W3 writeStyle
```

**Best of Both Worlds:** Real-time speed + batch accuracy

**📖 [Implementation Guide: Lambda Architecture Pattern B](./streaming_mdm_gcp/MDM_STREAMING_JOURNEY.md#part-4-implementing-the-batch-sync-pattern-b)**

---

## Key Benefits

### **Speed Layer (Spanner)**
- ⚡ Sub-400ms latency for real-time operations
- 🎯 Immediate availability for operational systems
- 📊 "Good enough" accuracy for live applications

### **Batch Layer (BigQuery)**
- 🧠 Comprehensive 5-way matching with AI
- 🎨 High-quality Gemini embeddings
- 📋 Advanced survivorship rules
- 📈 Processing of both streaming and bulk source data

### **Unified Benefits**
- 🔑 Deterministic IDs prevent duplicates
- 🔄 Non-destructive updates maintain stability
- 📊 Audit trail via processing_path field

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

- **📁 GitHub Repository:** Complete implementation with notebooks
- **📊 Interactive Demos:** Step-by-step Jupyter notebooks
- **🏗️ Architecture Guides:** Detailed technical documentation
- **🔧 Production Utils:** Ready-to-use Python utilities

---

<!-- _class: lead -->
# Questions & Discussion

**Thank you!**

*Modern MDM Architecture on Google Cloud*
