import datetime
from datetime import timezone
import time
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas
import pandas_gbq
import requests
import config
chain_info_list = config.chain_info_list
header = { "X-Access-Key": "dXlgP8Vw7dZ5ZGybrvMqal2o24qR5UDh","Content-Type": "application/json",}
txarr = [[],[],[],[],[],[],[]]
stg_per_block_list = []
credentials = service_account.Credentials.from_service_account_info(
        {
          "type": "service_account",
          "project_id": "nice-proposal-338510",
          "private_key_id": "6cc6ac641ca1ce66e1f4e1594db599079a33b633",
          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCmb34zlq7hxxmn\nqYZcbuf0Z4LXKEnKeCc+zUO3DDpaKIcZ03GjTrnEeUzpIhmMPhngN0AxTsgzqA2v\nZqpNoveQT9N7R+5hn8bSAsfbTfqfUNi1Pnn8ksueJ/zyAGLAqFKGZSXgTGK7XRpU\nofdDqWvlBDpfEcYTsackjZzJP3BUV7kqIC3+07ZtfRjL8jZFScwie8foqIcijHlz\nc41reBIRqfOCQQWPzA2CxEiY2l0/Pd1/PERQz1ecd1022ylXBH133oNhvYJCx+zK\ncq/leyJHsIkvdCSfD5gDCcJq3G8tbYcuVqriOmbtB/hRNwHj95JtR7SkJu8gOuBA\nttOUWTaBAgMBAAECggEADH1xIkGhxctxZ0MnaaK1toIuxU2+h+VkxeeVOCnrQR5/\nJlbXVX2QHv30DHTPfQD2Dnz8TMGlmuF23B20vxcb7qZcRQlSCt0jiFlxZ8meJWbl\nnxMICDEjUFmeN7JZYYLgOo9hpxz5tdItVSztx5Pxxwx+WD8XDHL4YPEIUz6hro/i\n1Rr00Hi+1DUZDwLZWtKAQwyLFpWzSdlqm0v5oLiIvfaVLFq8WN9HEFd+xextUaPW\nqWvkBxzVWKS73GsF6fDfyDP4Luj3BYeK756eDOZFXOSCJu6OoDW+1tU97vbmRCCu\ngOgviENvnCovRpBmJOSWU5wTekUYVNlEKe1rUzuTBwKBgQDrC6dkyuj5KuETfOly\nJk98iQKpZN9jok03LtRhCWZDR5Zcp36+XbcWrOpTxgPXrZdP8904FqCfwPyIUYvF\nVj7YxnEBatv8Haqa25xc6aU1cy7gmqaCNopczfv8I0O3h3JjrFMpf8fWaEeBHeXn\ncOesRNb+/JVe2d/+8CbSiDOLOwKBgQC1RftGjmW9W6rnBHu9CHSaP36CEyOV4elv\nrBjDC7untQoQuosizkQOolw8fp/54B6NM4N6YuTyBszZDU8zKa/tE2/mOP8cQ1PL\nT0B+FywVWkle56Z9BWcmwWPl3UxCKnBB4lxRXFMLhUqs3JkQHFKpXBki6S9YUmrT\nIQ+R+CxRcwKBgHRepbbaWzQZzau3WWEBpLL0ppO1dIBAt9gNYGrWm4HN7Jzc9HxN\nq7sXS0DEtdxPfq3AABOn6EiP5LedIAVIqP7saMCZ5mZRTckz15uMthXbfCcJikzH\nsgM2nH59+yXfcnv0sovGTutRX62GXvASTzFUN9mwtkdquWLgBWiQtGxHAoGAaLpY\ntOe6Ac9/Cl79ujWzYBZE2ZODIKnXOuxblfwmW3rtqMKNdftjOG03CsBBXpTMMdnM\nIR4XlXi2SsQ0uKNh/38WTBUr1kFTnQdZhD+Q+XkjIJCHEH3ZnXtnYrsavrNUDcxg\nd6T2WZaVkQ8V40fpvez2nMPJ1aYwD3BvVW7i898CgYBAPKAqWyr5cOyfH5HdndlT\nNPBCOMYsiILWQ3Hc6KeA1s+bGNAk+Pndo6lWyZaslnsrA/3WtFiHARhW2uV5UkIc\nJQneEi5Kp6rZ6MliqEMIoD6//3WHY/up5K1bjTBDYxawHVLoPYe6ZQdRU6x5Uwwk\nRkiAAtPBllcCkeQmgfLYCQ==\n-----END PRIVATE KEY-----\n",
          "client_email": "bigquery-service@nice-proposal-338510.iam.gserviceaccount.com",
          "client_id": "117436675962582309304",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-service%40nice-proposal-338510.iam.gserviceaccount.com"
        }
    )
project_id = 'nice-proposal-338510'
tbl_schema = [
    {'name': 'chain_id', 'type': 'int64'},
    {'name': 'tx', 'type': 'string'},
    {'name': 'block_no', 'type': 'int64'},
    {'name': 'timestamp', 'type': 'int64'},
    {'name': 'value', 'type': 'float64'},
]

def send_api():
    for i, _config in enumerate(chain_info_list):
        if(_config['chain'] != 'arbitrum'):
            for z, _api in enumerate(_config['address']):
                newurl = f"https://api.tenderly.co/api/v1/account/simonvoight/project/project/transactions?page=1&perPage=50000&status[]=success&contractId[]={_api}&functionSelector=0x6a761202"
                get_tx = requests.get(newurl ,headers=header)
                tx_result = get_tx.json()
                for j, _result in enumerate(reversed(tx_result)):            
                        for index, _con_address in enumerate(_config['multisig']):
                            if _result['to'] == _con_address:
                                tx_time = _result['timestamp']
                                formated_date = datetime.datetime.strptime(tx_time,"%Y-%m-%dT%H:%M:%SZ")
                                unix_timestamp = int(formated_date.replace(tzinfo=timezone.utc).timestamp())
                                txarr[i].append(
                                    {'hash':_result['hash'], 
                                    'block':_result['block_number'],
                                    'timestamp':tx_time,
                                    'unixtime':unix_timestamp}
                                    )
        if _config['chain'] == 'arbitrum' :
            newurl = f"https://api.arbiscan.io/api?module=account&action=txlist&address={_config['multisig']}&startblock=1&endblock=99999999&sort=asc&apikey=HVU4VHYBB5J2APNZ3TU5AQ96JZVJTCSPYU"
            get_tx = requests.get(newurl)
            tx_result = get_tx.json()
            for j, _result in enumerate(tx_result['result']):
                if _result['methodId'] == '0x6a761202':
                    txarr[i].append(
                        {'hash':_result['hash'], 
                        'block':_result['blockNumber'],
                        'unixtime':_result['timeStamp']}
                        )
        get_event(i)

def get_event(num):
    decimals = 10 ** 6
    for x, _hash in enumerate(txarr[num]):
        if _hash['hash'] != '0xa72ee7d0d10e339ee171d32366248ab4dd997502d257764cdd338dabac78bd0e' :
            apiurl = f'https://api.tenderly.co/api/v1/public-contract/{chain_info_list[num]["network_id"]}/trace/{_hash["hash"]}'
            get_event = requests.get(apiurl, headers=header)
            _event = get_event.json()
            if _event['state_diff'] :
                for y, _state in enumerate(_event['state_diff']) :
                    if _state['soltype'] and _state['soltype']['name'] == 'stargatePerBlock' and num != 4:
                        stg_per_block_list.append(
                            {'chain_id':int(chain_info_list[num]['chainId']),
                            'tx':str(_hash['hash']),
                            'block_no':int(_hash['block']),
                            # 'timestamp':_hash['timestamp'],
                            'timestamp':int(_hash['unixtime']),
                            'value':float(_state['dirty']) / decimals}
                            )
                    elif _state['soltype'] and _state['soltype']['name'] == 'stargatePerBlock' and num == 4:
                        stg_per_block_list.append(
                            {'chain_id':int(chain_info_list[num]['chainId']),
                            'tx':str(_hash['hash']),
                            'block_no':int(_hash['block']),
                            # 'timestamp':_event['created_at'],
                            'timestamp':int(_hash['unixtime']),
                            'value':float(_state['dirty']) / decimals}
                            )

def main(req1, req2):
    start_time = time.time()
    send_api()
    df = pandas.DataFrame(stg_per_block_list)
    print(df)
    return
    try:
      pandas_gbq.to_gbq(df, f'{project_id}.STG_related.tbl_stg_per_block', project_id=project_id, credentials=credentials, if_exists='replace', table_schema=tbl_schema)
    except:
        print('big query error')
    print("--- %s seconds ---" % (time.time() - start_time))

main(1,2)