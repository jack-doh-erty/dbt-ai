# Flight Emissions dbt Project

A dbt project for analyzing flight emissions data from OAG (Official Airline Guide) sample datasets. This project transforms raw flight emissions data into analytical models for insights into airline emissions, route efficiency, and aircraft performance.

## 🚀 Features

- **Staging Models**: Clean and standardize raw flight emissions data
- **Mart Models**: Business-ready analytical models for:
  - Daily emissions summaries
  - Carrier performance analysis
  - Route emissions analysis
  - Aircraft efficiency metrics
  - Flight phase emissions breakdown
- **Data Quality Tests**: Comprehensive testing for data integrity
- **Documentation**: Complete model documentation and descriptions

## 📋 Prerequisites

Before running this project, ensure you have:

1. **Snowflake Account** with access to the OAG flight emissions sample dataset
2. **Python 3.8+** installed
3. **dbt Core** installed
4. **Local dbt profiles configuration**

## 🛠️ Setup

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd dbt-ai
```

### 2. Set Up Python Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install dbt-snowflake
```

### 3. Configure dbt Profile
Create or update your `~/.dbt/profiles.yml` file with your Snowflake credentials:

```yaml
dev:
  target: dev
  outputs:
    dev:
      type: 'snowflake'
      account: 'YOUR_SNOWFLAKE_ACCOUNT'
      user: 'YOUR_USERNAME'
      password: 'YOUR_PASSWORD'
      role: 'YOUR_ROLE'
      database: 'YOUR_DATABASE'
      warehouse: 'YOUR_WAREHOUSE'
      schema: 'YOUR_SCHEMA'
      threads: 4
      client_session_keep_alive: False
```

### 4. Install dbt Dependencies
```bash
dbt deps
```

### 5. Verify Connection
```bash
dbt debug
```

## 📊 Data Sources

This project expects the following Snowflake tables to be available:

- `OAG_FLIGHT_EMISSIONS_DATA_SAMPLE.PUBLIC.ESTIMATED_EMISSIONS_SCHEDULES_SAMPLE`
- `OAG_FLIGHT_EMISSIONS_DATA_SAMPLE.PUBLIC.ESTIMATED_EMISSIONS_STATUS_SAMPLE`

## 🏗️ Project Structure

```
models/
├── raw/
│   └── oag_sources.yml          # Source definitions
├── staging/
│   ├── stg_estimated_emissions_schedules_sample.sql
│   ├── stg_estimated_emissions_schedules_sample.yml
│   ├── stg_estimated_emissions_status_sample.sql
│   └── stg_estimated_emissions_status_sample.yml
└── mart/
    ├── mart_daily_emissions_summary.sql
    ├── mart_carrier_performance.sql
    ├── mart_route_analysis.sql
    ├── mart_aircraft_efficiency.sql
    ├── mart_flight_phase_analysis.sql
    └── mart_daily_emissions_summary.yml
```

## 🚀 Usage

### Run All Models
```bash
dbt build
```

### Run Specific Models
```bash
dbt run --select staging+    # Run staging and downstream models
dbt run --select mart+       # Run mart models only
dbt run --select stg_estimated_emissions_schedules_sample  # Run specific model
```

### Run Tests
```bash
dbt test
```

### Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

### Query Highest CO2 Route
```bash
python utils/query_highest_co2_route.py
```

## 📈 Key Metrics

The project provides insights into:

- **Total CO2 emissions** by carrier, route, and aircraft type
- **Fuel efficiency** comparisons across different dimensions
- **Flight completion rates** and operational performance
- **Emissions by flight phase** (taxi, takeoff, cruise, etc.)
- **Route optimization** opportunities

## 🔧 Customization

### Adding New Models
1. Create SQL files in the appropriate directory (`staging/`, `mart/`, etc.)
2. Add corresponding YAML documentation files
3. Run `dbt run --select <model_name>` to test

### Modifying Tests
Edit the YAML files in the model directories to add or modify data quality tests.

## 🤝 Contributing

2. Create a feature branch
3. Make your changes
4. Add tests and documentation
5. Submit a pull request

## 🆘 Troubleshooting

### Common Issues

**Connection Errors:**
- Verify your Snowflake credentials in `~/.dbt/profiles.yml`
- Ensure your Snowflake account has the required datasets
- Check that your warehouse is running

**Model Errors:**
- Run `dbt debug` to verify your configuration
- Check that source tables exist and are accessible
- Review model dependencies with `dbt ls`

**Test Failures:**
- Some tests may fail due to data quality issues in the sample dataset
- Review test results and adjust expectations as needed

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review dbt documentation at [docs.getdbt.com](https://docs.getdbt.com)
3. Open an issue in this repository 