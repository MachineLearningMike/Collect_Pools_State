from google.oauth2 import id_token
from google.oauth2 import service_account
import google.auth
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
import time
import requests

# path to your cloud function or any other service
url = 'https://us-central1-nice-proposal-338510.cloudfunctions.net/fetch_pool_events'

# path to you keys file that was downloaded when keys for SA were created
keyFilePath = '../credentials/nice-proposal-338510-de3b4593adae.json'    # json key path
creds = service_account.IDTokenCredentials.from_service_account_file(keyFilePath, target_audience=url)
# auth session
authed_session = AuthorizedSession(creds)

# make authenticated request and print the response, status_code
resp = authed_session.get(url)

# to verify an ID Token
request = google.auth.transport.requests.Request()
token = creds.token

print(token)
# print the generated token
# print(token)
# print(id_token.verify_token(token,request))


# token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6ImJhMDc5YjQyMDI2NDFlNTRhYmNlZDhmYjEzNTRjZTAzOTE5ZmIyOTQiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJodHRwczovL3VzLWNlbnRyYWwxLW5pY2UtcHJvcG9zYWwtMzM4NTEwLmNsb3VkZnVuY3Rpb25zLm5ldC9mZXRjaF9wb29sX2V2ZW50cyIsImF6cCI6ImdjcC1jbG91ZC1mdW5jdGlvbnNAbmljZS1wcm9wb3NhbC0zMzg1MTAuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJlbWFpbCI6ImdjcC1jbG91ZC1mdW5jdGlvbnNAbmljZS1wcm9wb3NhbC0zMzg1MTAuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZXhwIjoxNjYzODM1MTEzLCJpYXQiOjE2NjM4MzE1MTMsImlzcyI6Imh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbSIsInN1YiI6IjExNjAyOTc4Mjc5ODE3OTg0MDk3OCJ9.atnhEnUKUTAZmKf0gq9jLpeZWzgiNM_CWj4UBcMxmV75OKpbiDeyGGlYA3j_CSAu1p-fbHwrLI8vVFXMzz2_vrqM0dQ--BtyZK10Livs8NfUKAmMv7VDHB0P3GHbw_SpXVHqei0ifRyRcX_XbxWIjGNJ7ilvc8kZlwXMg_gDTvTHBXB_Y3fPKq7ikALgpeisCdQsGyFq5_p8iIC4b4ZStnjayG-XVwbUf-j0Fgfdr97EXnvMC9j2JLOVoMF8FB24IYUZ-l_Nc9eBXJMJbzE2MG9GpRWYLfdO7HgdFflCSCQE6CyF2dyt-zC1mEMndY3bMqfEnTDv7Hvu97Ivd27BGg'
# time.sleep(10)



############################
# token = {'aud': 'https://us-central1-nice-proposal-338510.cloudfunctions.net/fetch_pool_events', 'azp': 'bigquery-service@nice-proposal-338510.iam.gserviceaccount.com', 'email': 'bigquery-service@nice-proposal-338510.iam.gserviceaccount.com', 'email_verified': True, 'exp': 1663770703, 'iat': 1663767103, 'iss': 'https://accounts.google.com', 'sub': '117436675962582309304'}