import datetime
from datetime import timezone
import time
import csv
import requests
from google.oauth2 import service_account
import math
import config
import pandas
import pandas_gbq

chain_info_list = config.chain_info_list
header = { "X-Access-Key": "dXlgP8Vw7dZ5ZGybrvMqal2o24qR5UDh","Content-Type": "application/json",}
chain_tx_list = [[],[],[],[],[],[],[]]
arr = []
project_id = "nice-proposal-338510"
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
tbl_schema = [
    {'name': 'chain_id', 'type': 'int64'},
    {'name': 'tx', 'type': 'string'},
    {'name': 'block_no', 'type': 'int64'},
    {'name': 'timestamp', 'type': 'int64'},
    {'name': 'pool_id', 'type': 'int64'},
    {'name': 'value', 'type': 'float64'},
]

def send_api():
    for i, _config in enumerate(chain_info_list):
        if(_config['chain'] != 'arbitrum' and _config['chain']!= 'fantom'):
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
                            chain_tx_list[i].append(
                                {'hash':_result['hash'], 
                                'block':_result['block_number'],
                                'timestamp':tx_time,
                                'unixtime':int(unix_timestamp)}
                                )
        if _config['chain'] == 'arbitrum' :
            newurl = f"https://api.arbiscan.io/api?module=account&action=txlist&address={_config['multisig']}&startblock=1&endblock=99999999&sort=asc&apikey=HVU4VHYBB5J2APNZ3TU5AQ96JZVJTCSPYU"
            get_tx = requests.get(newurl)
            tx_result = get_tx.json()
            for j, _result in enumerate(tx_result['result']):
                if _result['methodId'] == '0x6a761202':
                    chain_tx_list[i].append(
                        {'hash':_result['hash'], 
                        'block':_result['blockNumber'],
                        'unixtime':int(_result['timeStamp'])}
                        )
        print(len(chain_tx_list[i]))
        get_event(i)
    for index1, temp1 in enumerate(arr) : # convert block by timestamp
        for index2, temp2 in enumerate(arr) :
            if temp1['chainId'] == temp2['chainId'] and  index2 > index1 and math.floor(int(temp2['unix_time']) / 1000) - math.floor(int(temp1['unix_time']) / 1000) <= 1:
                arr[index1]['tx_hash'] = temp2['tx_hash']
                arr[index1]['block'] = temp2['block']
                arr[index1]['timestamp'] = temp2['timestamp']
                arr[index1]['unix_time'] = temp2['unix_time']
    arr.sort(key=sort_list) #sort array by poolid

    unix_arr = [] #convert sum of allocpoints into 100
    for item in arr:
        unix_arr.append(item['unix_time'])
    print(unix_arr)
    unique_arr = list(set(unix_arr))
    
    print(unique_arr)
    for i in unique_arr:
        sum_allocpoint  = sum([x['allocpoint'] for x in arr if x['unix_time'] == i])
        for item in arr:
            if item['allocpoint'] == 1:
                item['allocpoint'] = 0
            if item['allocpoint'] == 1000:
                item['allocpoint'] =100
            if item['unix_time'] == i and 1000 < sum_allocpoint <= 10000:
                item['allocpoint'] /= 100
            if item['unix_time'] == i and 100 < sum_allocpoint <= 1000:
                item['allocpoint'] /= 10
                
def get_event(num):
    for x, _hash in enumerate(chain_tx_list[num]):
        if _hash['hash'] != '0xa72ee7d0d10e339ee171d32366248ab4dd997502d257764cdd338dabac78bd0e' :
            apiurl = f'https://api.tenderly.co/api/v1/public-contract/{chain_info_list[num]["network_id"]}/trace/{_hash["hash"]}'
            _get_event = requests.get(apiurl, headers=header)
            _event = _get_event.json()
            if num == 0:
                for i, _function in enumerate(_event['call_trace']['calls'][0]['calls']) :
                    if _function['function_name'] == 'execute':
                        for j, _set in enumerate(_function['calls']):
                            if _set['function_name'] =='set':
                                arr.append(
                                    {'chainId':chain_info_list[num]['chainId'],
                                    'tx_hash':_hash['hash'],
                                    'block':_hash['block'],
                                    'timestamp':_hash['timestamp'],
                                    'unix_time':int(_hash['unixtime']),
                                    'pool_id':chain_info_list[num]['poolid'][int(_set['decoded_input'][0]['value'])],
                                    'allocpoint':int(_set['decoded_input'][1]['value'])}
                                    )
                            if _set['function_name'] =='multiSend':
                                for kl, _setset in enumerate(_set['calls']):
                                    if _setset['function_name'] =='set':
                                        arr.append(
                                            {'chainId':chain_info_list[num]['chainId'],
                                            'tx_hash':_hash['hash'],
                                            'block':_hash['block'],
                                            'timestamp':_hash['timestamp'],
                                            'unix_time':int(_hash['unixtime']),
                                            'pool_id':chain_info_list[num]['poolid'][int(_setset['decoded_input'][0]['value'])],
                                            'allocpoint':int(_setset['decoded_input'][1]['value'])}
                                            )
            else :
                for i, _function in enumerate(_event['call_trace']['calls'][0]['calls'][0]['calls']) :
                    if _function['function_name'] == 'execute':
                        for j, _set in enumerate(_function['calls']):
                            if 'function_name' in _set.keys() and _set['function_name'] == 'set':
                                arr.append(
                                    {'chainId':chain_info_list[num]['chainId'],
                                    'tx_hash':_hash['hash'],
                                    'block':int(_hash['block']),
                                    'timestamp':_event['created_at'],
                                    'unix_time':int(_hash['unixtime']),
                                    'pool_id':chain_info_list[num]['poolid'][int(_set['decoded_input'][0]['value'])],
                                    'allocpoint':int(_set['decoded_input'][1]['value'])}
                                    )
                            else :
                                for kl, _setset in enumerate(_set['calls']):
                                    if 'function_name' in _setset.keys() and _setset['function_name'] =='set':
                                        arr.append(
                                            {'chainId':chain_info_list[num]['chainId'],
                                            'tx_hash':_hash['hash'],
                                            'block':int(_hash['block']),
                                            'timestamp':_event['created_at'],
                                            'unix_time':int(_hash['unixtime']),
                                            'pool_id':chain_info_list[num]['poolid'][int(_setset['decoded_input'][0]['value'])],
                                            'allocpoint':int(_setset['decoded_input'][1]['value'])}
                                            )
            print(x, len(arr))
def sort_list(e):
    return e['chainId'],e['block'], e['pool_id']

def main():
    start_time = time.time()                       
    send_api()
    fieldnames = ['chainId', 'tx_hash', 'block', 'timestamp', 'unix_time', 'pool_id', 'allocpoint']
    df = pandas.DataFrame(arr)
    df.drop(columns='timestamp', inplace=True)
    df['chainId'] = df['chainId'].astype('int64')
    df['block'] = df['block'].astype('int64')
    df['unix_time'] = df['unix_time'].astype('int64')
    df['pool_id'] = df['pool_id'].astype('int64')
    df['allocpoint'] = df['allocpoint'].astype('float64')
    df.rename(columns={'chainId': 'chain_id', 'block': 'block_no', 'unix_time': 'timestamp', 'allocpoint': 'value', 'tx_hash': 'tx'}, inplace=True, errors='raise')
    print(df)
    try:
        pandas_gbq.to_gbq(df, f'{project_id}.STG_related.tbl_alloc_point', project_id=project_id, credentials=credentials, if_exists='replace', table_schema=tbl_schema)
    except:
        print('big query error')
    print("--- %s seconds ---" % (time.time() - start_time))

main()