# fyers_instruments.py

import logging

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class FyersInstruments:
    HEADERS = [
        "Fytoken",  # Unique token for each symbol
        "Symbol Details",  # Name of the symbol
        "Exchange Instrument type",  # Exchange instrument type
        "Minimum lot size",  # Minimum qty multiplier
        "Tick size",  # Minimum price multiplier
        "ISIN",  # Unique ISIN provided by exchange for each symbol
        "Trading Session",  # Trading session provided in IST
        "Last update date",  # Date of last update
        "Expiry date",  # Date of expiry for a symbol (for derivative contracts)
        "Symbol ticker",  # Unique string to identify the symbol
        "Exchange",  # Exchange mapping
        "Segment",  # Segment of the symbol
        "Scrip code",  # Token of the Exchange
        "Underlying symbol",  # Name of underlying symbol
        "Underlying scrip code",  # Scrip code of underlying symbol
        "Strike price",  # Strike price
        "Option type",  # CE/PE - For options; XX - For other segments
        "Underlying FyToken",  # Unique token for the underlying symbol
        "Reserved column",  # Reserved for future, kindly ignore
        "Reserved column int",  # Reserved for future, kindly ignore
        "Reserved column"  # Reserved for future, kindly ignore
    ]

    URLS = [
        "https://public.fyers.in/sym_details/NSE_CD.csv",  # NSE – Currency Derivatives
        "https://public.fyers.in/sym_details/NSE_FO.csv",  # NSE – Equity Derivatives
        "https://public.fyers.in/sym_details/NSE_CM.csv",  # NSE – Capital Market
        "https://public.fyers.in/sym_details/BSE_CM.csv",  # BSE – Capital Market
        "https://public.fyers.in/sym_details/BSE_FO.csv",  # BSE - Equity Derivatives
        "https://public.fyers.in/sym_details/MCX_COM.csv"  # MCX - Commodity
    ]

    @classmethod
    def download_instruments(cls):
        try:
            dfs = []
            for url in cls.URLS:
                try:
                    logging.info(f"Downloading from {url}")
                    df = pd.read_csv(url, header=0)
                    df.columns = cls.HEADERS
                    dfs.append(df)
                except Exception as e:
                    logging.error(f"Error downloading from {url}: {e}")

            if not dfs:
                raise Exception("No data downloaded successfully")

            combined_df = pd.concat(dfs, sort=False)
            
            # Save to CSV
            output_file = "fyers_instruments.csv"
            combined_df.to_csv(output_file, index=False)
            logging.info(f"Saved combined data to {output_file}")
            
            return combined_df

        except Exception as e:
            logging.error(f"Error in download_instruments: {e}")
            return None

if __name__ == "__main__":
    print("Starting Fyers instrument download...")
    df = FyersInstruments.download_instruments()
    if df is not None:
        print(f"Successfully downloaded {len(df)} instruments")
        print("\nSample data:")
        print(df.head())
