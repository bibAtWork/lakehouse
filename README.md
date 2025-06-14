# Lakehouse Data Engineering Project

A comprehensive data engineering project demonstrating the integration of:
- **PySpark 4.0** - Distributed data processing
- **dbt** - Data transformation and modeling
- **Delta Lake** - ACID transactions and schema evolution
- **Unity Catalog** - Centralized metadata management (local or cloud)
- **Dagster** - Data orchestration and workflow management


## Prerequisites

- Python 3.9+
- Docker and Docker Compose
- dbt CLI

## Quick Start

1. **Clone and setup the project:**
   ```bash
   git clone <repository-url>
   cd lakehouse
   # Edit .env with your configuration
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start services:**
   ```bash
   docker-compose up -d
   ```

4. **Initialize dbt:**
   ```bash
   cd src/dbt
   dbt deps
   dbt seed
   dbt run
   ```

5. **Start Dagster:**
   ```bash
   dagster dev
   ```

## Features

### Data Ingestion
- PySpark jobs for batch data ingestion
- Delta Lake format for ACID transactions
- Unity Catalog for metadata management (local or cloud)

### Data Transformation
- dbt models for SQL-based transformations
- Incremental processing
- Data quality tests

### Orchestration
- Dagster pipelines for workflow management
- Asset-based data lineage
- Monitoring and alerting

### Data Lake Architecture
- Bronze (raw data)
- Silver (cleaned/validated data)
- Gold (business-ready data)

### Unity Catalog Integration
- **Local Deployment**: Run Unity Catalog locally using Docker containers
- **Cloud Deployment**: Use Databricks Unity Catalog for production
- Centralized metadata management and governance
- Data lineage tracking
- Schema evolution management

## Configuration

### Environment Variables
- `DEPLOYMENT_MODE` - Set to 'local' or 'databricks'
- `SPARK_MASTER_URL` - Spark cluster URL
- `UNITY_CATALOG_HOST` - Unity Catalog service URL (local: http://localhost:8081)
- `UNITY_CATALOG_CATALOG` - Unity Catalog catalog name
- `UNITY_CATALOG_SCHEMA` - Unity Catalog schema name

### Local Unity Catalog Setup
The project includes a local Unity Catalog deployment using Docker:
- Runs on port 8081
- Uses PostgreSQL for metadata storage
- Provides the same functionality as cloud Unity Catalog
- Perfect for development and testing

### Databricks Unity Catalog Setup (Optional)
1. Configure Unity Catalog in your Databricks workspace
2. Create catalog and schema
3. Set up proper permissions
4. Update configuration files

## Development

### Running Tests
```bash
# Run dbt tests
cd src/dbt && dbt test

# Run PySpark tests
python -m pytest tests/

# Run Dagster tests
dagster asset materialize --select "*"
```

### Adding New Data Sources
1. Create ingestion job in `src/spark/jobs/`
2. Add dbt models in `src/dbt/models/`
3. Create Dagster assets in `src/dagster/assets/`
4. Update pipeline in `src/dagster/jobs/`

## Monitoring

- Dagster UI: http://localhost:3000
- Spark UI: http://localhost:8080
- Unity Catalog UI: http://localhost:8081
- MinIO Console: http://localhost:9001
- dbt docs: `dbt docs generate && dbt docs serve`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details 