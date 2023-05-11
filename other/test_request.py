import requests
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../credentials/nice-proposal-338510-de3b4593adae.json"

resp = requests.get(url='https://my-http-function-pnuq6bxbza-uc.a.run.app', params={'message': 'first hello'})
print(resp)