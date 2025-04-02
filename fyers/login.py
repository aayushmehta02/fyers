# login.py
import json
import urllib.parse

import pyotp
from breeze_connect import BreezeConnect  # type:ignore
from fyers_api import accessToken, fyersModel
from NorenRestApiPy.NorenApi import NorenApi


class ICICILogin:
    def __init__(self, tusta_user_id, client_id, api_secret):
        self.client_id = client_id
        self.api_secret = api_secret
        
        self.tusta_user_id = tusta_user_id
       
    def icici_handle_login(self):
        try:
            user_broker_details = UserBrokerDetails.getUserBrokerDetailsByUserIdAndBroker(
                self.tusta_user_id , "Fyers" #here user_id is tusta user_id
            )
            api_key = user_broker_details.get('app_key')
            api_secret = user_broker_details.get('api_secret')
            client_id = user_broker_details.get('client_id')
            if not all([
                api_key, #for icici only get client id
                api_secret, #for icici only get api secret
                client_id #for icici only get api key
            ]): 
                raise BrokerError("Missing required Fyers credentials")

            # Initialize ICICI client and login
            session=accessToken.SessionModel(
                client_id=self.client_id,
                secret_key=self.api_secret,  # Make sure your secret key is not exposed publicly
                redirect_uri="https://api.tusta.co/active_users/fyers",
                response_type="code",
                grant_type="authorization_code"
                )
            auth_url = session.generate_authcode()
            print(auth_url)
            session.get_access_token(auth_code="")
            response = session.generate_token()
            access_token = response.get('access_token')



            login_obj = fyersModel.FyersModel(
            token=access_token,
            is_async=False,
            client_id=self.client_id,
            log_path=""
        )
            load = json.loads(login_obj.get_profile())



            UserManager.save_active_user({
            "broker": "Fyers",
            "access_token": access_token,
            "api_key": self.api_key,
            "refresh_token": None,
            "secret_key": self.api_secret,
            "clientCode": client_id,
            "name": user_broker_details['user_name'],
            "uid": self.tusta_user_id,
            "feed_token": None,
            "password": user_broker_details['password'],
            "yob": user_broker_details['yob']
        })

            return redirect(APP_REDIRECT_URL)
        except Exception as e:
            print(f"Error: {e}")
            return False


           
