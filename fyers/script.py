import json
import logging
import os
import time
import uuid

import pandas as pd
from fyers_api import fyersModel

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class FyersAPI:
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
        self.access_token = session_token
        self.app_id = app_id
        self.app_secret = app_secret
        self.obj = fyersModel.FyersModel(
            token=session_token,
            is_async=False,
            client_id=self.app_id,
            log_path=""
        )
        self._load_instruments()

    def _load_instruments(self):
        """Load instruments data from CSV file."""
        try:
            self.instruments_df = pd.read_csv(
                "fyers_instruments.csv",
                dtype={
                    "Fytoken": str,
                    "Exchange Instrument type": int,
                    "Exchange": int,
                    "Strike price": float,
                    "Minimum lot size": int
                },
                low_memory=False
            )
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
            funds = self.obj.funds().get('fund_limit', [])
            margins = next((item for item in funds if item.get('id') == 10), None)
            return margins.get('equityAmount', 0)
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
                    
                
               
                
                # Fetch individual order details after placing the order
                count = 0
                order_details = None
                while count < 3:
                    try:
                        order_details = self.obj.orderbook(data={"id": order_id})
                        break
                    except:
                        time.sleep(0.1)
                        count += 1
                    
                if order_details is None:
                    return None, None, "Order placement failed - Couldn't fetch order details."
                
                order_details = order_details.get('orderBook', [{}])[0]
                status = order_details.get('status')
                filled_quantity = order_details.get('filledQty', 0)

                print(f"Order Status: {status}, Filled Quantity: {filled_quantity}")

                
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
        "app_id": "",
        "app_secret": "",
        "session_token": ""
    }
    
    # Ensure latest instrument data
    from download import FyersInstruments
    if not os.path.exists("fyers_instruments.csv"):
        FyersInstruments.download_instruments()
    
    # Initialize and test API
    fyers_api = FyersAPI(**creds)
    
    # Test functionality
    print("Funds:", fyers_api.get_funds())
    print("LTP:", fyers_api.get_ltp("NSE", "2885"))
    print(fyers_api.place_order_on_broker(
        "874306", "BSE:SENSEX2540870000CE", 100, "BFO", "BUY", "LIMIT", 1000, False, True
    ))