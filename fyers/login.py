import json
import logging
import os
import time

import pandas as pd
from fyers_api import fyersModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class FyersAPI:
    def __init__(self, session_token, app_id, app_secret):
        self.access_token = session_token
        self.app_id = app_id
        self.app_secret = app_secret
        self.obj = fyersModel.FyersModel(token=session_token, is_async=False, client_id=self.app_id, log_path="")
        
        # Read CSV with proper data types
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
        return self.obj

    def get_funds(self):
        try:
            # Fetch margins for the equity segment (other segments are available as well)
            print("Fetching funds")
            funds = self.obj.funds()['fund_limit']
            # print(funds)

            margins = next((item for item in funds if item['id'] == 10), None)
            # print(margins)
            # Get the net available funds from the equity margins
            net_available_funds = margins.get('equityAmount', 0)

            return net_available_funds
        except Exception as e:
            logging.error(f"Error in fetching funds 123: {e}")
            print(f"Error in fetching funds: {e}")
            return 0

    @classmethod
    def filter_by_expiry(cls, df, expiry='W'):
        # Convert Unix timestamp to datetime
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
        instrumenttype = instrumenttype.upper()
        if exch_seg == 'NFO':
            exch_seg = 10
        elif exch_seg == 'MCX':
            exch_seg = 11
        else:
            exch_seg = 12
        segment_dict = {
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
        common_conditions = df[
                             (df['Exchange'] == exch_seg) &
                             (df['Underlying symbol'] == symbol) &
                             (df['Exchange Instrument type'] == segment_dict[instrumenttype])
                             ]
        # Check if the segment and instrument match the simple condition
        if  instrumenttype in ["FUTIDX","FUTIVX","FUTSTK","FUTCUR","FUTIRT","FUTIRC","FUTCOM"]:
            return common_conditions
        else:
            # Conditions for strikePrice and tradingSymbol suffix
            return common_conditions[
                (common_conditions['Strike price'] == strike_price) &
                 (common_conditions['Option type'] == ce_pe)
                ]

    @classmethod
    def get_fyers_token_details(cls, exch_seg, symbol, strike_price=None, is_pe=1, expiry='W', instrumenttype=None):
        ce_pe = "PE" if is_pe == 1 else "CE"
        symbol = symbol.upper()
        try:
            # Handle different segments
            if exch_seg in ['NFO', 'MCX', 'BFO']:
                df_filtered = cls.filter_fno_instruments(cls.instruments_df, exch_seg, symbol, strike_price, ce_pe,
                                                         instrumenttype)

                if df_filtered.empty:
                    print(f"No token found for {symbol} {strike_price}{ce_pe} in {exch_seg}")
                    return None, None

                token_info = cls.filter_by_expiry(df_filtered, expiry)
            else:
                # For regular stocks
                if exch_seg == 'NSE':
                    exch_seg = 10
                else:
                    exch_seg = 12
                df_filtered = cls.instruments_df[
                    (cls.instruments_df['Exchange'] == exch_seg) &
                    (cls.instruments_df['Underlying symbol'] == symbol)&
                    (cls.instruments_df['Exchange Instrument type'] .isin([0, 4, 50]))
                    ]

                if df_filtered.empty:
                    print(f"No token found for {symbol} in {exch_seg}")
                    return None, None
                token_info = df_filtered.iloc[0]

            if token_info is not None:
                option_token = token_info['Fytoken']
                symbol = token_info['Symbol ticker']
                return option_token, symbol
            else:
                return None, None

        except Exception as e:
            print(f'Error caught in getting Fyers token info: {e}')
            return None, None

    def get_details_from_csv(self, symbol):
        return self.instruments_df[self.instruments_df['Symbol ticker'] == symbol]

    def get_ltp(self, exchange, trading_symbol):
        try:
            data = {
            "symbols":"NSE:TCS-EQ",
            
            "ohlcv_flag" : 1
            }

            response = self.obj.quotes(data=data)
            ltp = response['d'][0]['v']['lp']
            return ltp
        except Exception as e:
            logging.error(f"Error in fetching LTP: {e}")
            print(f"Error in fetching LTP: {e}")
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

    def place_order_on_broker(self, symbol_token, symbol, qty, exchange, buy_sell, order_type, price, is_paper=False):
        try:
            if order_type == 'LIMIT':
                orderType = 1
            else:
                orderType = 2
            order_params = {
                "symbol":symbol,
                "qty":qty,
                "type":orderType,
                "side":1 if buy_sell == 'BUY' else -1,
                "productType":"INTRADAY",
                "limitPrice":price if orderType == 1 else 0,
                "stopPrice":0,
                "validity":"DAY",
                "disclosedQty":0,
                "offlineOrder":False,
                "orderTag":"tag1"
            }
            average_price = 0
            order_id = None
            if not is_paper:
                # Place the order using the Fyers API
                response = self.obj.place_order(data=order_params)
                order_id = response['id']
                print(f"Order placed successfully with Order ID: {order_id}")
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
                order_details = order_details['orderBook'][0]
                status = order_details.get('status', None)
                filled_quantity = order_details.get('filledQty', 0)

                print(f"Order Status: {status}, Filled Quantity: {filled_quantity}")

                # Check order completion status
                if status == 2:
                    average_price = order_details.get('tradedPrice', 0)
                elif status == 5 and filled_quantity == 0:
                    rejection_reason = order_details['message']
                    if "insufficient funds" in rejection_reason.lower():
                        print("Order placement failed due to insufficient funds.")
                        return None, None, "Order placement failed due to insufficient funds."
                    else:
                        return None, None, f"Order placement failed: {rejection_reason}"
                else:
                    # Poll for a few seconds if the order is still open
                    for _ in range(9):
                        print("Waiting for order to be filled...")
                        time.sleep(0.3)
                        order_details = self.obj.orderbook(data={"id": order_id})
                        order_details = order_details['orderBook'][0]
                        status = order_details.get('status', None)
                        filled_quantity = order_details.get('filledQty', 0)
                        if status == 2:
                            average_price = order_details.get('tradedPrice', 0)
                            break
                        elif status == 5 and filled_quantity == 0:
                            rejection_reason = order_details['message']
                            if "insufficient funds" in rejection_reason.lower():
                                print("Order placement failed due to insufficient funds.")
                                return None, None, "Order placement failed due to insufficient funds."
                            else:
                                return None, None, f"Order placement failed: {rejection_reason}"
                        else:
                            # Cancel the order if it is still open
                            self.cancel_order_on_broker(order_id)
                            print(f"Order with ID {order_id} was canceled due to timeout.")
                            return None, None, "Order was canceled due to timeout."
            else:
                order_id = 'Paper' + str(int(time.time()))

            # Get the last traded price if needed
            if 'average_price' not in locals() or average_price == 0:
                ltp = self.get_ltp(exchange,symbol)
                average_price = ltp

            order_params['ltp'] = average_price
            # todo: format order_params
            order_params['transactiontype'] = buy_sell
            order_params['tradingsymbol'] = symbol
            order_params['quantity'] = qty
            order_params['symboltoken'] = str(symbol_token)
            # print(order_id, order_params,"ookokokok")
            return order_id, order_params, "Order placed successfully"

        except Exception as e:
            if "insufficient funds" in str(e).lower():
                print("Order placement failed due to insufficient funds.")
                return None, None, "Order placement failed due to insufficient funds."
            print(f"Order placement failed: {e}")
            return None, None, str(e)
        

if __name__ == "__main__":
    creds = {
        "app_id": "8UJ43NICTL-102",
        "app_secret": "17RFXXAD5U",
        "session_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE3NDM1Nzc4MzQsImV4cCI6MTc0MzY0MDIzNCwibmJmIjoxNzQzNTc3ODM0LCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCbjdPTHEzOWYtRllXUlBrQmlwbEFIOG5tcU9rbjFqSUh3X3NZbDhnSUFrR1A3aERad3NlcGVQUUZVaGt3VGNLVWNBM0FZSVllQklDNGp4Ull2NGo5V0tWcUdBTGxZZHQ2d1RnSkdFOWl1bXVjWURQZz0iLCJkaXNwbGF5X25hbWUiOiJIRU1BTkcgTU9OR0EiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiI1MDFlNDZmYWUzNmYyNmExNzc4NjU5MDc2NzAxYzMyNGVmZGRhZjM4ZGEwNWMwYzIwMzM1ZTkxZCIsImZ5X2lkIjoiWUgwMzExMCIsImFwcFR5cGUiOjEwMiwicG9hX2ZsYWciOiJOIn0.Y_Kvcc1rO3Hea_W-C68E9XcIbtmm7pmtMt0D8wPpFxw"
    }
    
    # First ensure we have the latest instrument data
    from download import FyersInstruments
    if not os.path.exists("fyers_instruments.csv"):
        FyersInstruments.download_instruments()
    
    # Initialize API
    fyers_api = FyersAPI(**creds)
    
    # Test functionality
    print("Funds: ",fyers_api.get_funds())
    print("LTP: ",fyers_api.get_ltp("NSE", "INFY"))