# ELT Technology Exploration

A repository for exploring different technology and architecture patterns for loading data into a lakehouse.

## Purpose

This repository serves as a testing ground for evaluating various approaches to:
- Data ingestion and extraction
- Data transformation pipelines
- Loading data into lakehouse architectures
- Metadata-driven and declarative pipeline generation
- Performance benchmarking
- Pattern comparison and best practices

## Starter Dataset: SEC Form N-PORT

We're using the SEC's Form N-PORT dataset as our primary data source for exploration.

**What is N-PORT?**
- Monthly portfolio holdings reports filed by registered investment companies
- Contains detailed information about fund holdings, securities, and risk metrics
- Structured data available in TSV format with comprehensive metadata

**Data Source:** [SEC Form N-PORT Data Sets](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets)

## Repository Structure

```
elt-tech-explore/
├── data/
│   └── nport/              # N-PORT dataset files
│       ├── 2024q4_nport/   # Quarterly data files
│       ├── nport_metadata.json
│       └── nport_readme.htm
├── patterns/
│   ├── python-duckdb/      # DuckDB-based ELT patterns
│   ├── databricks/         # Databricks-specific patterns
│   └── streaming/          # Streaming ingestion patterns
├── benchmarks/             # Performance comparison results
└── docs/                   # Documentation and findings
```

## Technology Patterns to Explore

### Lakehouse Platforms
- **DuckLake**: Open-source storage layer
- **Databricks**: Comparison for out of the box tools
- **Apache Iceberg**: Table format for large analytic datasets

### ETL/ELT Tools 
- **Fivetran** - Managed ELT platform
- **Airbyte** - Open-source data integration
- **dlt** - Python-first data loading tool
- **Meltano** - Singer-based ELT with orchestration
- **Slingdata** - CLI tool for simple file-to-warehouse transfers
- **Estuary Flow** - Real-time data pipelines

### Transformation Tools
- **dbt** - SQL-first transformation and testing
- **SQLMesh** - Modern transformation with virtual environments
- **Databricks** - Unified analytics platform with Delta Lake

### Orchestration Tools
- **Apache Airflow** - Workflow orchestration and scheduling
- **Prefect** - Python-native orchestration
- **Dagster** - Software-defined assets and data orchestration

### Processing Engines
- **Python + Polars** - High-performance DataFrame library
- **Python + DuckDB** - In-process analytical database

### Data Profiling & Quality
- **YData Profiling** - Automated profiling
- **Huey** - Open source profiling tool
- **OpenRefine** - Data cleaning and transformation
- **Apache Griffin** - Big data quality measurement
- **Ataccama One** - Enterprise data profiling
- **Collibra Data Quality** - Governance and quality
- **Informatica Data Explorer** - Metadata-driven profiling
- **Datakit.page** - Online tool

### Metadata & Governance Platforms
- **DataHub** — Open metadata platform with lineage and events
- **OpenMetadata** — Unified catalog, lineage, and pipeline metadata
- **Informatica IDMC**, **Talend** — Classic metadata-driven ETL
- **Dagster Assets** — Metadata-rich pipeline definitions
- **Unity Catalog (Databricks)** — Catalog + metadata for Delta tables

### Streaming Processing
- **Apache Kafka** - Distributed event streaming
- **Azure Event Hubs + Stream Analytics** - Cloud-native streaming



## Getting Started

1. **Data Setup**: The N-PORT data is located in [`data/nport/`](data/nport/)
2. **Choose a Pattern**: Navigate to the [`patterns/`](patterns/) directory
3. **Explore**: Each pattern includes setup instructions and sample code
4. **Benchmark**: Document performance and findings in [`benchmarks/`](benchmarks/)

## Medallion Architecture Approach

This exploration focuses on evaluating tools across a three-tier medallion architecture:

### Bronze Layer (Raw/Staging)
- **Data Staging**: Efficiently land raw data from sources
- **Initial Profiling**: Quick assessment of data quality, patterns, and distributions
- **Schema Discovery**: Automatic inference of data types and structures

### Silver Layer (Cleansed/Conformed)
- **Type Casting**: Convert to strongly-typed schemas
- **Data Cleansing**: Deduplication, normalization, standardization
- **Data Validation**: Apply quality checks to fields and entities
- **Null Handling**: Detect and manage missing values

### Gold Layer (Business/Presentation)
- **Transformation**: Apply business logic, derivations, and calculations
- **Derived Entities**: Create new data assets from source data
- **Data Marts**: Time series storage in Kimball-style dimensional models
- **Aggregations**: Pre-computed metrics and summaries

### Cross-Cutting Concerns
- **Orchestration**: Pipeline execution (user-triggered or event-driven)
- **Performance**: Processing speed and resource utilization
- **Scalability**: Ability to handle growing data volumes
- **Ease of Use**: Developer experience and learning curve
- **Cost**: Infrastructure and operational costs

## Tool Capability Matrix

| Tool | Type | Bronze (Stage) | Silver (Cleanse) | Gold (Transform) | Orchestration | Notes |
|------|------|----------------|------------------|------------------|---------------|-------|
| **Fivetran** | Managed ELT | ✓ | ~ | ✗ | ✓ | Excellent at extraction/loading, limited transformation |
| **Airbyte** | Open ELT | ✓ | ~ | ✗ | ~ | Strong connectors, basic normalization |
| **dlt** | Python ELT | ✓ | ✓ | ~ | ~ | Code-first, flexible schemas, good for custom staging |
| **Meltano** | Open ELT | ✓ | ~ | ~ | ✓ | Singer-based, orchestration focus, on evaluation list |
| **Slingdata** | CLI ELT | ✓ | ~ | ✗ | ~ | Simple file-to-warehouse, on evaluation list |
| **dbt** | Transform | ✗ | ✓ | ✓ | ~ | SQL-first transformation, testing, documentation |
| **SQLMesh** | Transform | ✗ | ✓ | ✓ | ✓ | Virtual data environments, column-level lineage |
| **Airflow** | Orchestration | ~ | ~ | ~ | ✓ | Workflow scheduling, complex DAGs |
| **Prefect** | Orchestration | ~ | ~ | ~ | ✓ | Modern Python-native orchestration |
| **Dagster** | Orchestration | ~ | ✓ | ✓ | ✓ | Software-defined assets, integrated testing |
| **Polars** | Processing | ✓ | ✓ | ✓ | ✗ | High-performance DataFrames, good for all layers |
| **DuckDB** | Processing | ✓ | ✓ | ✓ | ✗ | In-process analytics, excellent SQL performance |
| **OpenMetadata** | Metadata Platform | ~ | ✓ | ✓ | ✅ | Integrates | Unified metadata + lineage |
| **DataHub** | Metadata Platform | ~ | ✓ | ✓ | ✅ | Integrates | Event-driven metadata graph |
| **Informatica / Talend** | ETL Suite | ✓ | ✓ | ✓ | ✅ | ✓ | Classic metadata-driven ETL |

**Legend:**
- ✓ = Primary use case / Strong capability
- ~ = Partial support / Can be used with effort
- ✗ = Not designed for this purpose

**Notes:**
- Most ELT tools focus on Bronze (extraction/loading)
- Transformation tools (dbt, SQLMesh) excel at Silver/Gold layers
- Processing engines (Polars, DuckDB) can handle all layers but need orchestration
- Orchestrators can run pipelines but don't provide transformation logic themselves

## Contributing

This is an exploration repository. Create a section if it doesn't exist already under the patterns folder, and add a README. Document your findings, share insights, and compare approaches.

**License:** MIT - use any of this in your own projects, commercial or otherwise.

**Status:** Active experimentation. Contribute what interests you, no commitment required.
T

## Resources

- [SEC EDGAR N-PORT Information](https://www.sec.gov/divisions/investment/imnport.shtml)
- [N-PORT Technical Specification](data/nport/edgar-form-n-port-xml-tech-spec-113/)
- [N-PORT Metadata](data/nport/nport_metadata.json)
