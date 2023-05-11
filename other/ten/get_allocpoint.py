import datetime
from datetime import timezone
import time
import csv
import requests
import math
import configfile

config = configfile.config
header = { "X-Access-Key": "dXlgP8Vw7dZ5ZGybrvMqal2o24qR5UDh","Content-Type": "application/json",}
txarr = [[],[],[],[],[],[],[]]
arr = []

def send_api():
    for i, _config in enumerate(config):
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
                            txarr[i].append(
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
                    txarr[i].append(
                        {'hash':_result['hash'], 
                        'block':_result['blockNumber'],
                        'unixtime':int(_result['timeStamp'])}
                        )
        print(len(txarr[i]))
        get_event(i)
    for index1, temp1 in enumerate(arr) : # convert block by timestamp
        for index2, temp2 in enumerate(arr) :
            if temp1['chainId'] == temp2['chainId'] and  index2 > index1 and math.floor(int(temp2['unix_time']) / 1000) - math.floor(int(temp1['unix_time']) / 1000) <= 1:
                arr[index1]['tx_hash'] = temp2['tx_hash']
                arr[index1]['block'] = temp2['block']
                arr[index1]['timestamp'] = temp2['timestamp']
                arr[index1]['unix_time'] = temp2['unix_time']
    arr.sort(key=sort_array) #sort array by poolid

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
    for x, _hash in enumerate(txarr[num]):
        if _hash['hash'] != '0xa72ee7d0d10e339ee171d32366248ab4dd997502d257764cdd338dabac78bd0e' :
            apiurl = f'https://api.tenderly.co/api/v1/public-contract/{config[num]["network_id"]}/trace/{_hash["hash"]}'
            get_event = requests.get(apiurl, headers=header)
            _event = get_event.json()
            if num == 0:
                for i, _function in enumerate(_event['call_trace']['calls'][0]['calls']) :
                    if _function['function_name'] == 'execute':
                        for j, _set in enumerate(_function['calls']):
                            if _set['function_name'] =='set':
                                arr.append(
                                    {'chainId':config[num]['chainId'],
                                    'tx_hash':_hash['hash'],
                                    'block':_hash['block'],
                                    'timestamp':_hash['timestamp'],
                                    'unix_time':int(_hash['unixtime']),
                                    'pool_id':config[num]['poolid'][int(_set['decoded_input'][0]['value'])],
                                    'allocpoint':int(_set['decoded_input'][1]['value'])}
                                    )
                            if _set['function_name'] =='multiSend':
                                for kl, _setset in enumerate(_set['calls']):
                                    if _setset['function_name'] =='set':
                                        arr.append(
                                            {'chainId':config[num]['chainId'],
                                            'tx_hash':_hash['hash'],
                                            'block':_hash['block'],
                                            'timestamp':_hash['timestamp'],
                                            'unix_time':int(_hash['unixtime']),
                                            'pool_id':config[num]['poolid'][int(_setset['decoded_input'][0]['value'])],
                                            'allocpoint':int(_setset['decoded_input'][1]['value'])}
                                            )
            else :
                for i, _function in enumerate(_event['call_trace']['calls'][0]['calls'][0]['calls']) :
                    if _function['function_name'] == 'execute':
                        for j, _set in enumerate(_function['calls']):
                            if 'function_name' in _set.keys() and _set['function_name'] == 'set':
                                arr.append(
                                    {'chainId':config[num]['chainId'],
                                    'tx_hash':_hash['hash'],
                                    'block':int(_hash['block']),
                                    'timestamp':_event['created_at'],
                                    'unix_time':int(_hash['unixtime']),
                                    'pool_id':config[num]['poolid'][int(_set['decoded_input'][0]['value'])],
                                    'allocpoint':int(_set['decoded_input'][1]['value'])}
                                    )
                            else :
                                for kl, _setset in enumerate(_set['calls']):
                                    if 'function_name' in _setset.keys() and _setset['function_name'] =='set':
                                        arr.append(
                                            {'chainId':config[num]['chainId'],
                                            'tx_hash':_hash['hash'],
                                            'block':int(_hash['block']),
                                            'timestamp':_event['created_at'],
                                            'unix_time':int(_hash['unixtime']),
                                            'pool_id':config[num]['poolid'][int(_setset['decoded_input'][0]['value'])],
                                            'allocpoint':int(_setset['decoded_input'][1]['value'])}
                                            )
            print(x, len(arr))
def sort_array(e):
    return e['chainId'],e['block'], e['pool_id']
start_time = time.time()                       
send_api()
print("--- %s seconds ---" % (time.time() - start_time))
fieldnames = ['chainId', 'tx_hash', 'block', 'timestamp', 'unix_time', 'pool_id', 'allocpoint']
with open('alloc_point.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(arr)
