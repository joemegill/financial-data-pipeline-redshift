import os
# import boto3
import pandas as pd
# from dotenv import load_dotenv


import os
import numpy as np
import pandas as pd


import yfinance as yf
import time
import datetime as dt

load_dotenv()

s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION'))

BUCKET = os.getenv('S3_BUCKET_NAME')

def download_stock_data(ticker: str, start :str ="2020-10-23", end : str='2025-10-23', interval :str ="1d", retries:int =3, delay:int=5) -> pd.DataFrame:
    """
    Download stock price data for a given ticker, with retry and rate limit handling. 2017-10-23

    Parameters:
    - ticker: str, stock symbol (e.g. 'MSFT')
    - start: star date for downloading
    - end: end date for downlaoing
    - interval: str, data interval (e.g. '1d' for daily)
    - retries: int, number of retries in case of failure
    - delay: int, delay between retries in seconds
    
    Returns:
    - DataFrame containing stock price data.
    """
    for attempt in range(retries):
        try:
            stock_data = yf.download(ticker, start=start, end=end, interval=interval).droplevel(level=1, axis=1)
            return stock_data
        except Exception as e:
            print(f"Error downloading stock data: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                print(f"Failed to download stock data after {retries} attempts.")
                return None


def main(ticker_list: list)-> None:


    for ticker in  ticker_list:
        #formating fix  
        if ticker == "BRKA":
            ticker = "BRK-A"
        
        data = download_stock_data(ticker, start=(dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d') , end=dt.datetime.now().strftime('%Y-%m-%d'))
      
        if (data is not None) and (data.shape[0] > 5):
            os.makedirs('stock_data', exist_ok=True)
            local_path = f"stock_data/{ticker}_stock_data_{dt.datetime.now().strftime('%Y%m%d')}.parquet"
            data.to_parquet(local_path)
            s3_key = f"raw/{ticker}_stock_data_{dt.now().strftime('%Y%m%d')}.parquet"
            s3.upload_file(local_path, BUCKET, s3_key)
        else:
            print(ticker)
        time.sleep(5)






load_dotenv()

s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION'))

BUCKET = os.getenv('S3_BUCKET_NAME')

# def fetch_etrade_positions() -> str:
#     """Fetch portfolio positions and upload CSV to S3. Returns s3_key."""
#     print("Fetching E*TRADE positions (demo data)...")
#     data = [
#         { "symbol": "AAPL", "quantity": 10, "price": 190.25 },
#         { "symbol": "MSFT", "quantity": 5, "price": 420.10 },
#     ]
#     df = pd.DataFrame(data)
#     os.makedirs('data', exist_ok=True)
#     local_path = 'data/etrade_positions.csv'
#     df.to_csv(local_path, index=False)

#     s3_key = 'raw/etrade_positions.csv'
#     print(f"Uploading {local_path} to s3://{BUCKET}/{s3_key}...")
#     s3.upload_file(local_path, BUCKET, s3_key)
#     print("Upload complete.")
#     return s3_key

if __name__ == "__main__":

    main(["AAPL", "MSFT"])