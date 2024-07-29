import pandas as pd
import sqlite3
import logging
from datetime import datetime


class DataPipeline:
    def __init__(self, db_connection_string):
        self.db_connection_string = db_connection_string
        self.logger = logging.getLogger('DataPipeline')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def transform_data_in_python(self, df, data_type):
        """
        Transform the data in Python before uploading to SQL.
        """
        self.logger.info(f'Transforming data for {data_type}')
        if data_type == 'holdings':
            df = self.clean_holdings_data(df)
        elif data_type == 'portfolio_stats':
            df = self.clean_portfolio_stats_data(df)
        elif data_type == 'trades':
            df = self.clean_trades_data(df)
        return df

    def clean_holdings_data(self, df):
        df.columns = [col.strip() for col in df.columns]
        df['business_date'] = pd.to_datetime(df['business_date'], errors='coerce')
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['market_value'] = pd.to_numeric(df['market_value'].str.replace(',', ''), errors='coerce')
        df['portfolio_id'] = df['portfolio_id'].str.strip()
        df['security_id'] = df['security_id'].str.strip()
        df['currency'] = df['currency'].str.strip()
        df.dropna(subset=['business_date', 'portfolio_id', 'security_id'], inplace=True)
        return df

    def clean_portfolio_stats_data(self, df):
        df.columns = [col.strip() for col in df.columns]
        df['business_date'] = pd.to_datetime(df['business_date'], errors='coerce')
        df['nav'] = pd.to_numeric(df['nav'].str.replace(',', ''), errors='coerce')
        df['daily_pnl'] = pd.to_numeric(df['daily_pnl'].str.replace(',', ''), errors='coerce')
        df['ytd_return'] = df['ytd_return'].str.replace('%', '').astype(float) / 100
        df.dropna(subset=['business_date', 'portfolio_id'], inplace=True)
        return df

    def clean_trades_data(self, df):
        df.columns = [col.strip() for col in df.columns]
        df['business_date'] = pd.to_datetime(df['business_date'], errors='coerce')
        df['quantity'] = pd.to_numeric(df['quantity'].str.replace('(', '-').str.replace(')', ''), errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['portfolio_id'] = df['portfolio_id'].str.strip()
        df['security_id'] = df['security_id'].str.strip()
        df.dropna(subset=['business_date', 'portfolio_id', 'security_id'], inplace=True)
        return df

    def upload_data_to_sql(self, df, table_name):
        """
        Upload the cleaned data to SQL.
        """
        self.logger.info(f'Uploading data to {table_name}')
        conn = sqlite3.connect(self.db_connection_string)
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()

    def transform_data_in_sql(self):
        """
        Perform SQL transformations.
        """
        self.logger.info('Performing SQL transformations')
        conn = sqlite3.connect(self.db_connection_string)
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS holdings AS
        SELECT business_date, portfolio_id, security_id, exchange, quantity, market_value, currency
        FROM holdings_stage
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_stats AS
        SELECT business_date, portfolio_id, nav, daily_pnl, ytd_return, sharpe_ratio, volatility, var_95
        FROM portfolio_stats_stage
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades AS
        SELECT business_date, portfolio_id, security_id, quantity, price, currency, exchange
        FROM trades_stage
        ''')

        cursor.execute('''
        CREATE VIEW holdings_portfolio_combined AS
        SELECT h.*, ps.nav, ps.daily_pnl, ps.ytd_return
        FROM holdings h
        JOIN portfolio_stats ps ON h.portfolio_id = ps.portfolio_id AND h.business_date = ps.business_date
        ''')

        cursor.execute('''
        CREATE VIEW aggregated_trades AS
        SELECT portfolio_id, COUNT(*) AS total_trades, SUM(quantity) AS total_quantity, SUM(price * quantity) AS total_value
        FROM trades
        GROUP BY portfolio_id
        ''')

        conn.commit()
        conn.close()