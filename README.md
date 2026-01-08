<div align="center">
  <img src="https://user-images.githubusercontent.com/25080503/237990810-ab2e14cf-a449-47ac-8c72-6f0857816194.png#gh-light-mode-only" alt="AutomateDV">
  <img src="https://user-images.githubusercontent.com/25080503/237990915-6afbeba8-9e80-44cb-a57b-5b5966ab5c02.png#gh-dark-mode-only" alt="AutomateDV">

  [![Slack](https://img.shields.io/badge/Slack-Join-yellow?style=flat&logo=slack)](https://join.slack.com/t/dbtvault/shared_invite/enQtODY5MTY3OTIyMzg2LWJlZDMyNzM4YzAzYjgzYTY0MTMzNTNjN2EyZDRjOTljYjY0NDYyYzEwMTlhODMzNGY3MmU2ODNhYWUxYmM2NjA)
</div>

### dbt models for AutomateDV Snowflake Demonstration

This is a downloadable example dbt project using [AutomateDV](https://github.com/Datavault-UK/automate-dv) to create a Data Vault 2.0 Data Warehouse
based on the Snowflake TPC-H dataset.

---

## Project Architecture

This project implements a complete Data Vault 2.0 architecture with an Information Mart layer:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            TPCH_SF10 (Source)                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Raw Stage (rdv schema)                              │
│  raw_orders, raw_inventory, raw_transactions                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Staging (rdv schema)                                │
│  v_stg_orders, v_stg_inventory, v_stg_transactions                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Raw Vault (rdv schema)                                │
│  ┌─────────┐  ┌─────────────────────────┐  ┌────────────────────────────┐   │
│  │  Hubs   │  │         Links           │  │        Satellites          │   │
│  │─────────│  │─────────────────────────│  │────────────────────────────│   │
│  │customer │  │ link_customer_order     │  │ sat_order_customer_details │   │
│  │order    │  │ link_order_lineitem     │  │ sat_order_order_details    │   │
│  │lineitem │  │ link_customer_nation    │  │ sat_order_lineitem_details │   │
│  │part     │  │ link_inventory          │  │ sat_inv_*                  │   │
│  │supplier │  │ ...                     │  │ ...                        │   │
│  └─────────┘  └─────────────────────────┘  └────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Business Vault (bdv schema)                            │
│  ┌──────────────┐  ┌──────────────────────────────────────────────────────┐ │
│  │  as_of_date  │  │                   pit_customer                       │ │
│  │  (date spine)│──│  Point-in-Time table for customer satellite lookup  │ │
│  └──────────────┘  └──────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Information Mart (mart schema)                         │
│  ┌─────────────────────────┐  ┌────────────────────────────────────────┐   │
│  │      dim_customer       │  │              fct_orders                │   │
│  │─────────────────────────│  │────────────────────────────────────────│   │
│  │ SCD Type 2 dimension    │  │ Transactional fact table               │   │
│  │ Condensation pattern    │  │ Grain: one row per order               │   │
│  │ from daily PIT snapshots│  │ Links to dim_customer via CUSTOMER_SK  │   │
│  └─────────────────────────┘  └────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Schema Structure

| Schema | Purpose | Models |
|--------|---------|--------|
| `rdv` | Raw Data Vault | Hubs, Links, Satellites, Staging |
| `bdv` | Business Data Vault | PIT tables, Date spines |
| `mart` | Information Marts | Dimensions, Facts |

## Quick Start

### Prerequisites
- Snowflake account with access to `SNOWFLAKE_SAMPLE_DATA.TPCH_SF10`
- Python 3.9+ with dbt-snowflake
- Configure `~/.dbt/profiles.yml` with profile name `dbtvault_snowflake_demo`

### Installation

```bash
# Clone and setup
git clone <repo-url>
cd automate-dv-demo
python -m venv .venv
source .venv/bin/activate
pip install dbt-snowflake

# Install dbt packages
dbt deps
```

### Build Commands

```bash
# Build entire project (recommended order)
dbt run --select raw_stage       # Load raw data
dbt run --select stage           # Stage for vault
dbt run --select raw_vault       # Build vault structures
dbt run --select business_vault  # Build PIT tables
dbt run --select mart            # Build dimensions and facts

# Or build everything at once
dbt build

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Loading Scripts

Helper scripts are provided in `scripts/` to simulate data loads and test incremental processing.

### Reset and Start Fresh

Use `reset_staging.sql` to truncate all staging tables and reload Day 1 data:

```sql
-- In Snowflake worksheet, run:
-- scripts/reset_staging.sql

-- Then rebuild all dbt models:
dbt run --full-refresh && dbt test
```

### Simulate Incremental Loads

Use `simulate_load.sql` to add new data and customer changes:

```sql
-- 1. Edit the configuration variables at the top of the script:
SET load_date = '1992-01-12';           -- New load date
SET num_customers_to_change = 5;         -- Customers to modify
SET num_orders_to_load = 100;            -- New orders to load

-- 2. Run the script in Snowflake

-- 3. Process changes with dbt:
dbt run && dbt test
```

### Script Reference

| Script | Purpose |
|--------|---------|
| `scripts/reset_staging.sql` | Truncate staging, reload Day 1 data, full reset |
| `scripts/simulate_load.sql` | Add incremental load with customer changes |
| `scripts/load_staging_day1.sql` | Original Day 1 initial load |
| `scripts/load_staging_day2.sql` | Day 2 load with customer mutations |
| `scripts/load_staging_day3.sql` | Day 3 load |

### Typical Workflow

```bash
# 1. Reset to clean state
#    Run scripts/reset_staging.sql in Snowflake
dbt run --full-refresh

# 2. Simulate Day 2 load
#    Run scripts/load_staging_day2.sql in Snowflake
dbt run

# 3. Simulate custom load
#    Edit and run scripts/simulate_load.sql in Snowflake
dbt run

# 4. Validate after each load
dbt test
```

## Key Models

### dim_customer (SCD Type 2)
- Implements "condensation" pattern from daily PIT snapshots
- Uses LAG/LEAD window functions for change detection
- Tracks validity periods with VALID_FROM/VALID_TO
- IS_CURRENT flag for current record lookup

### fct_orders (Transactional Fact)
- Grain: one row per order
- Links to dim_customer via CUSTOMER_SK
- Incremental merge strategy
- Preserves ORDER_PK for vault lineage

## Tests

| Test Type | Count | Description |
|-----------|-------|-------------|
| Data Tests (Generic) | 194 | PK uniqueness, not_null, relationships, composite keys |
| Singular Tests | 12 | Cross-layer integration validation |
| Unit Tests | 4 | Mart business logic (SCD2, lookups) |

```bash
# Run all tests
dbt test

# Run by test type
dbt test --select "test_type:singular"   # Integration tests
dbt test --select "test_type:unit"       # Unit tests
dbt test --select "test_type:data"       # Generic data tests
```

### Singular Tests (Integration)
| Test | Validates |
|------|-----------|
| `assert_staging_to_hub_completeness` | No data loss staging → hubs |
| `assert_hub_satellite_completeness` | All hubs have satellite records |
| `assert_pit_covers_all_hubs` | PIT covers all hub customers |
| `assert_dim_covers_pit_customers` | Mart covers all BDV customers |
| `assert_fct_has_customer_reference` | Fact has valid customer refs |
| `assert_order_count_consistency` | Order counts match across layers |
| `assert_link_hub_integrity` | Links have valid hub references |
| `assert_tlink_hub_integrity` | T-links have valid hub references |
| `assert_dim_customer_single_current` | One IS_CURRENT=true per customer |
| `assert_dim_customer_no_validity_gaps` | No gaps in SCD2 validity |

### Unit Tests
| Test | Validates |
|------|-----------|
| `test_dim_customer_scd2_condensation` | SCD2 VALID_FROM/TO boundaries |
| `test_dim_customer_same_hashdiff_creates_single_version` | Hashdiff deduplication |
| `test_fct_orders_customer_lookup` | Customer SK lookup |
| `test_fct_orders_unknown_customer` | Unknown customer handling |

## Documentation

Full model documentation available via:
```bash
dbt docs generate && dbt docs serve
```

---

#### AutomateDV Docs
[![Documentation Status](https://readthedocs.org/projects/dbtvault/badge/?version=latest)](https://automate-dv.readthedocs.io/en/latest/?badge=latest)

Click the button above to read the latest AutomateDV docs.

A step-by-step user guide for using this demo is available [here](https://automate-dv.readthedocs.io/en/latest/worked_example/)

---
[dbt](https://www.getdbt.com/) is a registered trademark of dbt Labs.

Check them out below:

#### dbt Docs
- [What is dbt](https://dbt.readme.io/docs/overview)?
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint)
- [Installation](https://dbt.readme.io/docs/installation)
- Join the [chat](http://ac-slackin.herokuapp.com/) on Slack for live questions and support.
---