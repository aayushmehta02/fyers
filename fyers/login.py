# login.py
import json

from flask import redirect, request
from fyers_api import accessToken, fyersModel  # type: ignore


class FyersLogin:
    # def __init__(self, tusta_user_id):
        
        
    #     self.tusta_user_id = tusta_user_id
       
    def fyers_handle_login(self):
        try:
            auth_code = request.args.get('auth_code')
            broker_details = BrokerDetails.getBrokerDetails('Fyers')
            app_id = broker_details['app_id']
            redirect_uri = broker_details['redirect_url']
            app_secret = broker_details['app_secret']
            
            appSession = fyersModel.SessionModel(
                client_id=app_id,
                redirect_uri=redirect_uri,
                response_type="code",
                state="sample",
                secret_key=app_secret,
                grant_type="authorization_code"
            )
            
            appSession.set_token(auth_code)
            response = appSession.generate_token()

            try:
                access_token = response["access_token"]
            except Exception as e:
                print(e, response)
                return {'Message': 'Missing access token'}

            fyers = fyersModel.FyersModel(token=access_token, is_async=False, client_id=app_id, log_path="")
            data = fyers.get_profile()['data']
            client_id = data['fy_id']
            user_broker_details = UserBrokerDetails.getUserBrokerDetailsByClientId(client_id)
            tusta_user_id = user_broker_details['user_id']
            
            

            Users.update(self.tusta_user_id, 'is_new', False)
            UserManager.save_active_user({
                "broker": "Fyers",
                "access_token": access_token,
                "api_key": self.api_key,
                "refresh_token": None,
                "secret_key": self.api_secret,
                "clientCode": client_id,
                "name": user_broker_details['user_name'],
                "uid": tusta_user_id,
                "feed_token": None,
                "password": user_broker_details['password'],
                "yob": user_broker_details['yob']
            })
            # return redirect(APP_REDIRECT_URL)
            
            return redirect('https://tustaco.page.link/HKMg')
        
            
        except Exception as e:
            print("\n**\nerror in add active users:", e)
            return {'Message': 'Failure'}

    
            
        


           
