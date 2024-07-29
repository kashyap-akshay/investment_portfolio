# Investment Portfolio Data ETL Pipeline

This repository contains an ETL pipeline for processing investment portfolio data. The pipeline reads data from CSV files, transforms the data, and loads it into a SQL database. The process is managed using Apache Airflow.

## Requirements

- Python 3.7+
- SQLite
- Apache Airflow
- Pandas
- DBT
- SMTP

## Setup

1. **Clone the repository:**

   ```sh
   git clone https://github.com/kashyap-akshay/investment_portfolio.git
   cd investment_portfolio_etl


# Investment Portfolio Data ETL Pipeline

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run the Jupyter notebook to demonstrate the pipeline:

```bash
jupyter notebook notebooks/data_pipeline_demo.ipynb
```

3. **Set up Airflow:**

   Follow the official [Airflow installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html) to install and set up Airflow.

## Usage

1. **Run the data pipeline script:**

   You can run the `data_pipeline.py` script directly to test the pipeline functionality.

   ```sh
   python data_pipeline.py
   ```

2. **Start Airflow:**

   Start the Airflow web server and scheduler.

   ```sh
   airflow webserver
   airflow scheduler
   ```

3. **Create and activate the Airflow DAG:**

   Ensure the `investment_portfolio_etl.py` file is placed in the `dags` directory of your Airflow home.

   ```sh
   cp investment_portfolio_etl.py $AIRFLOW_HOME/dags/
   ```

4. **Monitor the DAG:**

   Access the Airflow web interface (default: http://localhost:8080) and trigger the `investment_portfolio_etl` DAG. Monitor the tasks and logs for any issues.

## Data Files

Ensure the following data files are placed in the specified directory:

- `holdings_data_1.csv`
- `portfolio_stats_1.csv`
- `trades_data_1.csv`

## Example CSV Files

The CSV files should follow the structure defined in the `data_pipeline.py` script for each data type (`holdings`, `portfolio_stats`, `trades`).

## Contact

For any questions or issues, please contact `akshay.kashyap2002@gmail.com`.

By following these steps, you should have a fully functional ETL pipeline managed by Airflow, including data quality checks and email notifications. Let me know if you need any additional assistance!