import json
import requests
import pyotp
from urllib import parse
import sys
from fyers_apiv3 import fyersModel
import fyers.credentials as cr
import time as tm

ACCESS_TOKEN = None  # Global variable to store access token

APP_ID = cr.APP_ID
APP_TYPE = cr.APP_TYPE
SECRET_KEY = cr.SECRET_KEY
client_id = f'{APP_ID}-{APP_TYPE}'

FY_ID = cr.FY_ID
APP_ID_TYPE = cr.APP_ID_TYPE  # 2denotes web login

#You should take care here that  TOTP secret is generated when you have enabled 2Factor TOTP from Fyers account portal

TOTP_KEY = cr.TOTP_KEY
PIN = cr.PIN # User pin for fyers account
REDIRECT_URI = cr.REDIRECT_URI # Redirect url from the APP in fyers dashboard



# API endpoints
BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api-t1.fyers.in/api/v3"
URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"
URL_TOKEN = BASE_URL_2 + "/token"
URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"

SUCCESS = 1
ERROR = -1




def send_login_otp(fy_id, app_id):
    try:
        result_string = requests.post(url=URL_SEND_LOGIN_OTP, json={
            "fy_id": fy_id, "app_id": app_id})
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]


def verify_totp(request_key, totp):
    # print("6 digits>>>",totp)
    # print("request key>>>",request_key)
    try:
        result_string = requests.post(url=URL_VERIFY_TOTP, json={
            "request_key": request_key, "otp": totp})
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]


def generate_totp(secret):
    try:
        generated_totp = pyotp.TOTP(secret).now()
        return [SUCCESS, generated_totp]

    except Exception as e:
        return [ERROR, e]


def verify_PIN(request_key, pin):
    try:
        payload = {
            "request_key": request_key,
            "identity_type": "pin",
            "identifier": pin
        }

        result_string = requests.post(url=URL_VERIFY_PIN, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        access_token = result["data"]["access_token"]

        return [SUCCESS, access_token]

    except Exception as e:
        return [ERROR, e]


def token(fy_id, app_id, redirect_uri, app_type, access_token):
    try:
        payload = {
            "fyers_id": fy_id,
            "app_id": app_id,
            "redirect_uri": redirect_uri,
            "appType": app_type,
            "code_challenge": "",
            "state": "sample_state",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        headers={'Authorization': f'Bearer {access_token}'}

        result_string = requests.post(
            url=URL_TOKEN, json=payload, headers=headers
        )

        if result_string.status_code != 308:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        url = result["Url"]
        auth_code = parse.parse_qs(parse.urlparse(url).query)['auth_code'][0]

        return [SUCCESS, auth_code]

    except Exception as e:
        return [ERROR, e]

def run():

    # Step 1 - Retrieve request_key from send_login_otp API

    session = fyersModel.SessionModel(client_id=client_id, secret_key=SECRET_KEY, redirect_uri=REDIRECT_URI,response_type='code', grant_type='authorization_code')

    urlToActivate = session.generate_authcode()
    # print(f'URL to activate APP:  {urlToActivate}')

    send_otp_result = send_login_otp(fy_id=FY_ID, app_id=APP_ID_TYPE)

    if send_otp_result[0] != SUCCESS:
        print(f"send_login_otp msg failure - {send_otp_result[1]}")
        status=False
        sys.exit()
    else:
        # print("send_login_otp msg SUCCESS")
        status=False


    # Step 2 - Generate totp
    generate_totp_result = generate_totp(secret=TOTP_KEY)

    if generate_totp_result[0] != SUCCESS:
        print(f"generate_totp msg failure - {generate_totp_result[1]}")
        sys.exit()
    else:
        pass
        # print("generate_totp msg success")


    # Step 3 - Verify totp and get request key from verify_otp API
    # for i in range(1, 3):

    request_key = send_otp_result[1]
    totp = generate_totp_result[1]
    # print("otp>>>",totp)
    verify_totp_result = verify_totp(request_key=request_key, totp=totp)
    # print("r==",verify_totp_result)

    if verify_totp_result[0] != SUCCESS:
        print(f"verify_totp_result msg failure - {verify_totp_result[1]}")
        status=False

        # tm.sleep(1)
    else:
        # print(f"verify_totp_result msg SUCCESS {verify_totp_result}")
        status=False


    if verify_totp_result[0] ==SUCCESS:

        request_key_2 = verify_totp_result[1]

        # Step 4 - Verify pin and send back access token
        ses = requests.Session()
        verify_pin_result = verify_PIN(request_key=request_key_2, pin=PIN)
        if verify_pin_result[0] != SUCCESS:
            print(f"verify_pin_result got failure - {verify_pin_result[1]}")
            sys.exit()
        else:
            pass
            # print("verify_pin_result got success")


        ses.headers.update({
            'authorization': f"Bearer {verify_pin_result[1]}"
        })

         # Step 5 - Get auth code for API V2 App from trade access token
        token_result = token(
            fy_id=FY_ID, app_id=APP_ID, redirect_uri=REDIRECT_URI, app_type=APP_TYPE,
            access_token=verify_pin_result[1]
        )
        if token_result[0] != SUCCESS:
            print(f"token_result msg failure - {token_result[1]}")
            sys.exit()
        else:
            pass
            # print("token_result msg success")

        # Step 6 - Get API V2 access token from validating auth code
        auth_code = token_result[1]
        session.set_token(auth_code)
        response = session.generate_token()
        if response['s'] =='ERROR':
            print("\n Cannot Login. Check your credentials thoroughly!")
            status=False
            tm.sleep(10)
            sys.exit()

        access_token = response["access_token"]
        # print(access_token)

        # Save access_token to 'fyers_data/access_token.json'
        with open('fyers_data/access_token.json', 'w') as f:
            json.dump({"access_token": access_token}, f)

        # Make access_token available for other modules. Can we save it in a way that's easy to import?
        global ACCESS_TOKEN
        ACCESS_TOKEN = access_token



if __name__ == "__main__":
    run()
