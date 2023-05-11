import datetime
from datetime import timezone
import time
import csv
import requests
import configfile
config = configfile.config
header = { "X-Access-Key": "dXlgP8Vw7dZ5ZGybrvMqal2o24qR5UDh","Content-Type": "application/json",}
txarr = [[],[],[],[],[],[],[]]
arr = []

def send_api():
    for i, _config in enumerate(config):
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
    for x, _hash in enumerate(txarr[num]):
        if _hash['hash'] != '0xa72ee7d0d10e339ee171d32366248ab4dd997502d257764cdd338dabac78bd0e' :
            apiurl = f'https://api.tenderly.co/api/v1/public-contract/{config[num]["network_id"]}/trace/{_hash["hash"]}'
            get_event = requests.get(apiurl, headers=header)
            _event = get_event.json()
            if _event['state_diff'] :
                for y, _state in enumerate(_event['state_diff']) :
                    if _state['soltype'] and _state['soltype']['name'] == 'stargatePerBlock' and num != 4:
                        arr.append(
                            {'chainId':config[num]['chainId'],
                            'tx_hash':_hash['hash'],
                            'block':_hash['block'],
                            'timestamp':_hash['timestamp'],
                            'unix_time':_hash['unixtime'],
                            'stargateperblock':_state['dirty']}
                            )
                    elif _state['soltype'] and _state['soltype']['name'] == 'stargatePerBlock' and num == 4:
                        arr.append(
                            {'chainId':config[num]['chainId'],
                            'tx_hash':_hash['hash'],
                            'block':_hash['block'],
                            'timestamp':_event['created_at'],
                            'unix_time':_hash['unixtime'],
                            'stargateperblock':_state['dirty']}
                            )
start_time = time.time()
send_api()
print("--- %s seconds ---" % (time.time() - start_time))
fieldnames = ['chainId', 'tx_hash', 'block', 'timestamp', 'unix_time', 'stargateperblock']
with open('StgPerBlock.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(arr)
