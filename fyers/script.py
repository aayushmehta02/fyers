import json
import logging
import os
import time
import uuid

import pandas as pd
from download import FyersInstruments
from fyers_api import fyersModel  # type: ignore

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class FyersAPI:
    instruments_df = None
    # Class constants
    EXCHANGE_CODES = {
        'NSE': 10,
        'BSE': 12,
        'NFO': 10,
        'MCX': 11,
        'BFO': 12
    }
    
    SEGMENT_TYPES = {
        "FUTIDX": 11,
        "FUTIVX": 12,
        "FUTSTK": 13,
        "OPTIDX": 14,
        "OPTSTK": 15,
        "FUTCUR": 16,
        "FUTIRT": 17,
        "FUTIRC": 18,
        "OPTCUR": 19,
        "FUTCOM": 30,
        "OPTFUT": 31,
        "OPTCOM": 32
    }

    def __init__(self, session_token, app_id, app_secret):
        """Initialize FyersAPI with credentials and load instrument data."""
        
        self.session_token = session_token
        self.app_id = app_id
        self.app_secret = app_secret
        self.obj = fyersModel.FyersModel(
            token=self.session_token,
            is_async=False,
            client_id=self.app_id,
            log_path=""
        )
        self._load_instruments()

    def _load_instruments(self):
        """Load instruments data using FyersInstruments class."""
        try:
            FyersAPI.instruments_df = FyersInstruments.get_instruments()


            if self.instruments_df is None:
                raise Exception("Failed to load instruments data")
            logging.info("Successfully loaded instruments data")
        except Exception as e:
            logging.error(f"Error loading instruments data: {e}")
            raise Exception("Failed to load instruments data")

    def get_broker_obj(self):
        """Return the Fyers model object."""
        return self.obj

    def get_funds(self):
        """Fetch available funds from the account."""
        try:
            # print(self.obj.funds())
            funds = self.obj.funds().get('fund_limit', [{}])[0]
            # print(funds)
            return funds.get('equityAmount', 0)

            # margins = next((item for item in funds if item.get('id') == 10), None)
            # return margins.get('equityAmount', 0)
        except Exception as e:
            logging.error(f"Error in fetching funds: {e}")
            return 0

    def get_details_from_csv(self, token):
        """Get instrument details from loaded CSV data."""
        token = str(token)
        df = self.instruments_df.copy()
        df['Scrip code'] = df['Scrip code'].astype(str)
        return df[df['Scrip code'] == token]

    def get_ltp(self, exchange_code, symbol_token):
        """Get Last Traded Price for a given token."""
        try:
            row = self.get_details_from_csv(symbol_token)
            if row.empty:
                logging.error(f"No data found for token {symbol_token}")
                return 0

            # Filter by exchange and instrument type
            if exchange_code == "NSE":
                row = row[row['Exchange Instrument type'] == 0]
            elif exchange_code in ["NFO", "BFO"]:
                row = row[row['Exchange Instrument type'] == 14]
                print(row)
            elif exchange_code == "MCX":
                row = row[row['Exchange Instrument type'] == 11]

            if row.empty:
                logging.error(f"No matching instrument found for {exchange_code}:{token}")
                return 0

            symbol = row['Symbol ticker'].values[0]
            data = {"symbols": symbol, "ohlcv_flag": 1}
            
            response = self.obj.quotes(data=data)
            return response.get('d', [{}])[0].get('v', {}).get('lp', 0)
        except Exception as e:
            logging.error(f"Error in fetching LTP: {e}")
            return 0

    def cancel_order_on_broker(self, order_id):
        try:
            # Cancel the order with the provided order_id using Fyers cancel_order() method
            cancel_order_response = self.obj.cancel_order(data={"id":order_id})

            # Print and return the cancel order response
            return cancel_order_response
        except Exception as e:
            print(f"Error in canceling order: {e}")
            return None
    @classmethod
    def filter_by_expiry(cls, df, expiry='W'):
        # Convert Unix timestamp to datetime
        df = df.copy()
        df['Expiry date'] = pd.to_datetime(df['Expiry date'], unit='s')

        # Find the last expiry date in each month
        monthly_expiry_df = df.loc[df.groupby(df['Expiry date'].dt.to_period('M'))['Expiry date'].idxmax()]

        if expiry == 'W':
            # Weekly expiry, return the first record
            return df.iloc[0]
        elif expiry == 'NW':
            # Next weekly expiry, return the second record
            return df.iloc[1]
        elif expiry == 'M':
            # Monthly expiry, return the first record in monthly_expiry_df
            return monthly_expiry_df.iloc[0]
        elif expiry == 'NM':
            # Next monthly expiry, return the second record in monthly_expiry_df
            return monthly_expiry_df.iloc[1]
        elif expiry == 'NNM':
            # Next-next monthly expiry, return the third record in monthly_expiry_df
            return monthly_expiry_df.iloc[2]

    @classmethod
    def filter_fno_instruments(cls, df, exch_seg, symbol, strike_price, ce_pe, instrumenttype):
        exch_seg_code = cls.EXCHANGE_CODES.get(exch_seg, 12)
        segment_type = cls.SEGMENT_TYPES.get(instrumenttype.upper(), None)
        if segment_type is None:
            return pd.DataFrame()
        

        df_filtered = df[(df['Exchange'] == exch_seg_code) &
                         (df['Underlying symbol'] == symbol) &
                         (df['Exchange Instrument type'] == segment_type)]

        if instrumenttype.upper() in ["FUTIDX", "FUTIVX", "FUTSTK", "FUTCUR", "FUTIRT", "FUTIRC", "FUTCOM"]:
            return df_filtered
        return df_filtered[(df_filtered['Strike price'] == strike_price) & (df_filtered['Option type'] == ce_pe)]

    @classmethod
    def get_fyers_token_details(cls, exch_seg, symbol, strike_price=None, is_pe=1, expiry='W', instrumenttype=None):
        ce_pe = "PE" if is_pe == 1 else "CE"
        symbol = symbol.upper()
        
        df = cls.instruments_df.copy()
        try:
            if exch_seg in ['NFO', 'MCX', 'BFO']:
                df_filtered = cls.filter_fno_instruments(
                    df, exch_seg, symbol, strike_price, ce_pe, instrumenttype
                )
                if df_filtered.empty:
                    print(f"No token found for {symbol} {strike_price}{ce_pe} in {exch_seg}")
                    return None, None
                token_info = cls.filter_by_expiry(df_filtered, expiry)
            else:
                exch_seg_code = cls.EXCHANGE_CODES.get(exch_seg, 12)
                df_filtered = df[
                    (df['Exchange'] == exch_seg_code) &
                    (df['Underlying symbol'] == symbol) &
                    (df['Exchange Instrument type'].isin([0, 4, 50]))
                ]
                if df_filtered.empty:
                    print(f"No token found for {symbol} in {exch_seg}")
                    return None, None
                token_info = df_filtered.iloc[0]

            if token_info is not None:
                return token_info['Scrip code'], token_info['Symbol ticker'],  token_info['Minimum lot size']
            return None, None, "No token found"
        except Exception as e:
            print(f'Error caught in getting Fyers token info: {e}')
            return None, None, "Error in getting Fyers token info"

        

    def place_order_on_broker(self, symbol_token, symbol, qty, exchange_code, buy_sell, order_type, price, is_paper=False, is_overnight=False):
        try:
            orderType = 1 if order_type == 'LIMIT' else 2
            product = 'INTRADAY'  # Intraday default
            
            if exchange_code in ['NFO', 'CDS', 'MCX', 'BFO', 'BCD'] and is_overnight:
                product = 'MARGIN'  # Margin for derivatives
            elif is_overnight:
                product = 'CASH'  # Carryforward for cash
            order_params = {
                        'symbol': symbol, 
                        'qty': qty,
                        'type': orderType,
                        'side': 1,
                        'productType': product,
                         'limitPrice': price, 
                        'validity': 'DAY', 
                        'offlineOrder': False,
                        'disclosedQty': 0, 
                        'stopPrice':0,
                          'orderTag': 'tag1'}

            average_price = 0
            order_id = None
            if not is_paper:
                # Place the order using the Fyers API
                response = self.obj.place_order(data=order_params)
                print(response)
                if not response or response.get('Success') == 'None':
                    error = response.get('emsg', 'Order placement failed')
                    print(f"Order placement failed: {error}")
                    return None, None, f"Order placement failed: {response.get('emsg', 'Unknown error')}"
                order_id = response.get('id')
                if not order_id:
                    return None, None, "Order placement failed - No order ID returned."
                else:
                     print(f"Order placed successfully with Order ID: {order_id}")
                
                average_price, status, error_message = self.handle_order_status(order_id)
                if not average_price:
                    return None, None, "Order placement failed - No order ID returned."
                    
             
              
                
                
            else:
                order_id = 'Paper' + str(uuid.uuid4())
                print(f"Paper trade created with ID: {order_id}")

            # Get the last traded price if needed
            if 'average_price' not in locals() or average_price == 0:
                ltp = self.get_ltp(exchange_code, symbol_token)
                average_price = ltp

            order_params['ltp'] = average_price
            order_params['transactiontype'] = buy_sell
            order_params['tradingsymbol'] = symbol
            order_params['quantity'] = qty
            order_params['symboltoken'] = str(symbol_token)
            return order_id, order_params, "Order placed successfully"

        except Exception as e:
            if "insufficient funds" in str(e).lower():
                print("Order placement failed due to insufficient funds.")
                return None, None, "Order placement failed due to insufficient funds."
            print(f"Order placement failed: {e}")
            return None, None, str(e)
    def fetch_order_status(self, order_id, retries=3, delay=0.5):
        try:
            for _ in range(retries):
                time.sleep(delay)
                response = self.obj.orderbook(data={"id": order_id})
                if not response or 'orderBook' not in response:
                    continue
                orders = response.get('orderBook', [])
                for order in orders:
                    if order.get('id') == order_id:
                        return order
            return None
        except Exception as e:
            logging.error(f"Error fetching order status: {e}")
            return None

    def handle_order_status(self, order_id):
        try:
            latest_order = self.fetch_order_status(order_id)
            if not latest_order:
                self.cancel_order_on_broker(order_id)
                return None, None, "Failed to fetch order status during polling"
            status = latest_order.get('status', 0)
            if status == 2:
                return latest_order.get('tradedPrice', 0), 'Completed', None
            if status == 5:
                return self.handle_rejection(latest_order)
            if status in [3, 6]:
                return None, None, f"Order {status}"
            self.cancel_order_on_broker(order_id)
            return None, None, "Order was canceled due to timeout."
        except Exception as e:
            logging.error(f"Error handling order status: {e}")
            return None, None, f"Error handling order status: {str(e)}"

    def handle_rejection(self, order):
        try:
            rejection_reason = order.get('message', 'Unknown reason')
            print(f"Order rejected: {rejection_reason}")
            error_message = (
                "Order placement failed due to insufficient funds."
                if "insufficient" in rejection_reason.lower()
                else f"Order placement failed: {rejection_reason}"
            )
            return None, None, error_message
        except Exception as e:
            logging.error(f"Error handling rejection: {e}")
            return None, None, "Error handling order rejection"
if __name__ == "__main__":
    # Configuration
    creds = {
        "app_id": "8UJ43NICTL-102",
        "app_secret": "17RFXXAD5U",
        "session_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZDoxIiwiZDoyIiwieDowIiwieDoxIiwieDoyIl0sImF0X2hhc2giOiJnQUFBQUFCb0FnZ1VMd2pOUXQ0ekpjNk9Bbkw5X3Y5VVc3R1JiMXJlUEl5QXZMallaY21iaVBsbUVGY290MXQ3QTk1U2hmOHJMLUJKMjVzZnJMT05QdHFpNTU5NjBfOG55VFR5VXNyNDhtY1B5THR3Rnk5ZElrdz0iLCJkaXNwbGF5X25hbWUiOiIiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiI1MDFlNDZmYWUzNmYyNmExNzc4NjU5MDc2NzAxYzMyNGVmZGRhZjM4ZGEwNWMwYzIwMzM1ZTkxZCIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiWUgwMzExMCIsImFwcFR5cGUiOjEwMiwiZXhwIjoxNzQ1MDIyNjAwLCJpYXQiOjE3NDQ5NjM2MDQsImlzcyI6ImFwaS5meWVycy5pbiIsIm5iZiI6MTc0NDk2MzYwNCwic3ViIjoiYWNjZXNzX3Rva2VuIn0.CfBqHlf7JzTyxLHKz0cMcAuhLfa9nivCd9QKajl8QuY"
    }
    
    
   
    if not os.path.exists("fyers_instruments.csv"):
        FyersInstruments.download_instruments()
    
    # Initialize and test API
    fyers_api = FyersAPI(**creds)
    
    print("\nCash Order Test (NSE, TCS):")
    token, symbol, lot_size = fyers_api.get_fyers_token_details('NSE', 'TCS')
    print("Token:", token, "| Symbol:", symbol, "| Lot size:", lot_size)
    print(fyers_api.place_order_on_broker(token, symbol, 1, 'NSE', "BUY", "MARKET", 0, is_paper=False))

    print("\nFNO Order Test (NFO, NIFTY, 23000 PE, Weekly):")
    token, symbol, lot_size = fyers_api.get_fyers_token_details('NFO', 'NIFTY', 23000, 1, 'W', 'OPTIDX')
    print("Token:", token, "| Symbol:", symbol, "| Lot size:", lot_size)
    print(fyers_api.place_order_on_broker(token, symbol, 75, 'NFO', "BUY", "MARKET", 0, is_paper=False))

    print("\nFutures Order Test (NFO, NIFTY, Monthly FUTIDX):")
    token, symbol, lot_size = fyers_api.get_fyers_token_details('NFO', 'NIFTY', expiry='M', instrumenttype='FUTIDX')
    print("Token:", token, "| Symbol:", symbol, "| Lot size:", lot_size)
    print(fyers_api.place_order_on_broker(token, symbol, 75, 'NFO', "BUY", "MARKET", 0, is_paper=False))

    print("\nSensex Option (BFO, SENSEX, 77400 CE, Weekly):")
    token, symbol, lot_size = fyers_api.get_fyers_token_details('BFO', 'SENSEX', 77400, 0, 'W', 'OPTIDX')
    print("Token:", token, "| Symbol:", symbol, "| Lot size:", lot_size)
    print(fyers_api.place_order_on_broker(token, symbol, 20, 'BFO', "BUY", "MARKET", 0, is_paper=False))

    print("\nLTP Examples:")
    print("NSE:", fyers_api.get_ltp('NSE', token))  # Last token from above
    print("NFO:", fyers_api.get_ltp('NFO', token))  # Same
    print(fyers_api.get_funds())
