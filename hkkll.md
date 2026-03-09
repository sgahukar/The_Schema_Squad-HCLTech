# ABC Insurance - HCLTech Hackathob=n

> **Date:** 17 Feb 2026  
> **Author:** The Schema Squad 

---

# 1. Problem Statement

ABC Insurance requires a **data engineering and analytics platform built entirely on Snowflake**.

The platform must:
- Ingest **policy data (CSV)** and **claims data (NDJSON)**
- Handle **initial loads (500 rows)** and **incremental loads (50 rows)**
- Apply **data quality validations**
- Normalize **denormalized datasets**
- Compute **8 business KPIs**
- Implement **enterprise-grade security**
- Support **automatic incremental processing using CDC**

### Data Validation Checks

The system validates the following fields:

- Email
- Phone number
- ZIP code
- SSN
- Dates
- Premium values

### Technical Constraint

The entire pipeline must use **Snowflake-native features only**:

- No external ETL tools
- No third-party orchestration frameworks
- No external processing engines

---

# 2. Solution Architecture Approach

The system is implemented using a **4-layer architecture inside a single Snowflake database**.

Database: `ABC_INSURANCE_DB`

| Layer | Schema | Purpose |
|------|------|------|
| Landing | RAW | Store source files exactly as received |
| Cleansing | VALIDATED | Data validation and transformation |
| Business | CURATED | Normalized relational data model |
| Analytics | ANALYTICS | KPI reporting views |

Additional governance schemas:

| Schema | Purpose |
|------|------|
| SECURITY | RBAC roles, masking policies, row access policies |
| AUDIT | Operational logging and traceability |

---

# 3. Architecture Overview

```
Source Files (CSV / JSON)
        │
        ▼
RAW Layer
(raw_policies, raw_claims)
        │
        ▼
Streams (CDC)
        │
        ▼
VALIDATED Layer
(data cleaning + validation)
        │
        ▼
CURATED Layer
(customers, addresses, agents, policies, claims)
        │
        ▼
ANALYTICS Layer
(KPI Views REQ1–REQ8)
```

Automation across layers is implemented using:

- **Streams**
- **Tasks**
- **MERGE operations**

---

# 4. Data Sources

| Source File | Format | Records | Description |
|-------------|--------|---------|-------------|
| policies_master_v3.csv | CSV | 500 | Initial policy dataset |
| policies_inc_2026.csv | CSV | 50 | Incremental policy updates |
| claims_master_v3.json | NDJSON | 500 | Initial claims dataset |
| claims_inc_2026.json | NDJSON | 50 | Incremental claims dataset |

### Key Challenges

- Policy dataset is **highly denormalized**
- Claims dataset contains **nested JSON structures**

---

# 5. Layer-by-Layer Architecture

---

# 5.1 RAW Layer — Data Ingestion

**Goal:** Load source files exactly as received with no transformation.

### Components

**Stages**

- `raw_policy_stage`
- `raw_claims_stage`

**File Formats**

- CSV policy format
- JSON NDJSON claims format

### Raw Tables

- `raw_policies`
- `raw_claims`

### Metadata Columns

```
_loaded_at
_source_file
_row_number
```

### Data Loading

```
COPY INTO raw tables
ON_ERROR = 'CONTINUE'
FORCE = FALSE
```

### Streams

Append-only streams detect newly inserted rows for downstream processing.

**Benefit:**  
Raw data remains immutable and can be reprocessed anytime.

---

# 5.2 VALIDATED Layer — Data Cleansing

**Goal:** Standardize and validate data.

### Type Casting

Safe conversions using:

```
TRY_TO_DATE()
TRY_TO_NUMBER()
```

### Standardization

Examples:

```
LOWER(email)
UPPER(state)
INITCAP(name)
TRIM()
```

### JavaScript UDFs

Custom validation functions:

- `udf_validate_email`
- `udf_validate_phone`
- `udf_validate_zip`
- `udf_validate_ssn`
- `udf_mask_ssn`

### Validation Flags

```
is_valid_email
is_valid_phone
is_valid_zip
is_valid_ssn
is_valid_record
```

### Error Tracking

Validation errors stored as:

```
INVALID_EMAIL | INVALID_PHONE | INVALID_DATE
```

### JSON Flattening

Claims JSON is parsed using:

```
raw_data:field_name::datatype
```

### Deduplication

```
QUALIFY ROW_NUMBER()
```

### Data Loading

```
MERGE INTO validated tables
```

Only **records marked as valid** move to the curated layer.

---

# 5.3 CURATED Layer — Business Data Model

**Goal:** Normalize denormalized source data.

### Problem

The source CSV contains **22 columns in one row**, combining:

- customer data
- address
- agent information
- policy details

This leads to **data redundancy**.

### Normalized Tables

| Table | Primary Key | Description |
|------|------|------|
| customers | customer_id | Customer personal information |
| addresses | customer_id | Customer address details |
| agents | agent_id | Insurance agent information |
| policies | policy_number | Policy attributes |
| claims | claim_id | Claim transaction records |

### Derived Fields

Examples:

```
policy_term_days
premium_per_month
is_active
claim_response_time
severity_bucket
paid_to_incurred_ratio
```

### Convenience Views

- `v_policies_full`
- `v_claims_full`

These views combine multiple tables for easier analytics.

---

# 5.4 ANALYTICS Layer — KPI Reporting

Eight business KPIs are implemented as SQL views.

| Requirement | KPI |
|-------------|-----|
| REQ1 | Agent Contact Quality |
| REQ2 | Policy Validation by State |
| REQ3 | Policy-Claim Matching |
| REQ4 | City Premium Benchmark |
| REQ5 | Top Cities Claim Severity |
| REQ6 | Cross-Sell Penetration |
| REQ7 | Onboarding Attachment |
| REQ8 | Policy-Only / Claim-Only |

---

# 6. Automation

Pipeline automation uses **Streams + Tasks**.

### Workflow

```
File Upload
   │
   ▼
COPY INTO RAW
   │
   ▼
Streams detect new data
   │
   ▼
Tasks load VALIDATED layer
   │
   ▼
Tasks populate CURATED layer
   │
   ▼
Analytics views updated
```

### Task Scheduling

```
Runs every 5 minutes
WHEN SYSTEM$STREAM_HAS_DATA
```

### Design Principle

All `MERGE` operations are **idempotent** to prevent duplicate data.

---

# 7. Security Architecture

Enterprise security is implemented using **Snowflake RBAC and data protection features**.

---

# 7.1 Roles

| Role | Access |
|------|------|
| ADMIN_ROLE | Full access |
| ANALYST_ROLE | Masked PII |
| REGION_AGENT_ROLE | Region-specific access |

---

# 7.2 Security Controls

### Dynamic Data Masking

Sensitive fields protected:

```
SSN
Email
Phone
```

Masked output example:

```
XXX-XX-1234
****
```

---

### Row Access Policy

Access controlled by region using:

```
region_role_mapping table
```

Example:

```
REGION_AGENT_EAST → Only East region rows
```

---

### Audit Logging

Audit table:

```
audit_log
```

Tracks:

- operation type
- target table
- user
- role
- timestamp
- affected rows

---

# 8. Snowflake Features Used

| Feature | Purpose |
|--------|--------|
| Internal Stages | File ingestion |
| File Formats | CSV/JSON parsing |
| COPY INTO | Bulk data loading |
| VARIANT | JSON data storage |
| JavaScript UDFs | Data validation |
| Streams | Change data capture |
| Tasks | Pipeline automation |
| MERGE | Incremental upserts |
| QUALIFY | Deduplication |
| Masking Policies | Column-level security |
| Row Access Policies | Row-level security |
| TRY_TO Functions | Safe type conversion |
| Metadata Columns | File tracking |
| Views | Analytics layer |

---

# 9. Data Flow Summary

```
CSV Policy Files
JSON Claim Files
        │
        ▼
RAW Layer
        │
        ▼
VALIDATED Layer
        │
        ▼
CURATED Layer
(customers, agents, addresses, policies, claims)
        │
        ▼
ANALYTICS Layer
REQ1 – REQ8 KPI Views
```

---

# 10. Key Strengths

1. Fully **Snowflake-native architecture**
2. **Idempotent data pipeline**
3. Automatic **incremental data processing**
4. Proper **data normalization**
5. Complete **data lineage tracking**
6. Enterprise-level **security implementation**
7. Advanced **data validation using UDFs**
8. Transparent **error tracking**

---