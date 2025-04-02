@active_users_bp.route('/fyers_redirect', methods=['GET'])
def fyers_redirect():
    print("\n\n", "time:", datetime.now(timezone("Asia/Kolkata")), "api: /active_users/fyers_redirect")
    broker = 'Fyers'
    broker_details = BrokerDetails.getBrokerDetails(broker)
    launch_url = broker_details['launch_url']
    return redirect(launch_url)

@active_users_bp.route('/fyers', methods=['GET'])
def add_fyers_active_user():
    try:
        print("\n\n", "time:", datetime.now(timezone("Asia/Kolkata")), "api: /active_users/fyers")

        auth_code = request.args.get('auth_code')
        broker_details = BrokerDetails.getBrokerDetails('Fyers')
        app_id = broker_details['app_id']
        redirect_uri = broker_details['redirect_url']
        app_secret = broker_details['app_secret']
        appSession = fyersModel.SessionModel(client_id=app_id, redirect_uri=redirect_uri,
                                             response_type="code", state="sample", secret_key=app_secret,
                                             grant_type="authorization_code" )
        appSession.set_token(auth_code)
        response = appSession.generate_token()

        ## There can be two cases over here you can successfully get the acccessToken over the request or you might get some error over here. so to avoid that have this in try except block
        try:
            access_token = response["access_token"]
        except Exception as e:
            print(e, response)
            return {'Message': 'Missing access token'}

        fyers = fyersModel.FyersModel(token=access_token, is_async=False, client_id=app_id, log_path="")
        data = fyers.get_profile()['data']
        client_id = data['fy_id']
        user_broker_details = UserBrokerDetails.getUserBrokerDetailsByClientId(client_id)
        user_id = user_broker_details['user_id']
        ActiveUsers(broker="Fyers", feed_token=None, access_token=access_token,
                    refresh_token=None, clientCode=client_id,
                    name=data['name'], uid=user_id).save()
        Users.update(user_id, 'is_new', False)
        
    except Exception as e:
        print("\n**\nerror in add active users:", e)
        return {'Message': 'Failure'}