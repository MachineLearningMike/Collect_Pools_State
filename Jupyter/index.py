import pandas as pd
import numpy as np
from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
import pandas
import requests
import json
from google.cloud import bigquery
import threading
import time
import textwrap
client = bigquery.Client()


start_block = '0'
end_block = 'latest'
max_num = 1000

pool_id = {
    "USDC": 1,
    "USDT": 2,
    "BUSD": 5,
    "SGETH": 13,
}

chain_id = {
    "ethereum": 1,
    "bsc": 2,
    "avalanche": 6,
    "polygon": 9,
    "arbitrum": 10,
    "optimism": 11,
    "fantom": 12,
}

event_topics = {
	'set': '0x545b620a3000f6303b158b321f06b4e95e28a27d70aecac8c6bdac4f48a9f6b3',
	'add': '0x1c482cb20f653d55406cc8aa89ebf482b8603c0ffebcf7e6182ff8ac1849d12d',
	'mint': '0xb4c03061fb5b7fed76389d5af8f2e0ddb09f8c70d1333abbb62582835e10accb',
	'burn': '0x49995e5dd6158cf69ad3e9777c46755a1a826a446c6416992167462dad033b2a',
	'swapremote': '0xfb2b592367452f1c437675bed47f5e1e6c25188c17d7ba01a12eb030bc41ccef',
	'swap': '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f',
	'deposit': '0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15',
	'withdraw': '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568',
}

chain_info = {
	'ethereum': {
		'address': {
			'USDT': {
				'poolAddress': "0x38EA452219524Bb87e18dE1C24D3bB59510BD783",
				'erc20Address': "0xdAC17F958D2ee523a2206206994597C13D831ec7",
				'deployBlock': 14403402,
			},
			'USDC': {
				'poolAddress': "0xdf0770dF86a8034b3EFEf0A1Bb3c889B8332FF56",
				'erc20Address': "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
				'deployBlock': 14403393,
			},
			'SGETH': {
				'poolAddress': "0x101816545F6bd2b1076434B54383a1E633390A2E",
				'erc20Address': "0x72E2F4830b9E45d52F80aC08CB2bEC0FeF72eD9c",
				'deployBlock': 15035701,
			},
		},
        'LPStaking': '0xB0D502E938ed5f4df2E681fE6E419ff29631d62b',
		'key': '845JF5MISST352XBEQK6XRS6FJE9ZIFWPB',
		'url': 'https://api.etherscan.io/api'
	},
	'bsc': {
		'address': {
			'USDT': {
				'poolAddress': "0x9aA83081AA06AF7208Dcc7A4cB72C94d057D2cda",
				'erc20Address': "0x55d398326f99059fF775485246999027B3197955",
				'deployBlock': 16135132,
			},
			'BUSD': {
				'poolAddress': "0x98a5737749490856b401DB5Dc27F522fC314A4e1",
				'erc20Address': "0xe9e7cea3dedca5984780bafc599bd69add087d56",
				'deployBlock': 16135136
			},
		},
        'LPStaking': '0x3052A0F6ab15b4AE1df39962d5DdEFacA86DaB47',
		'key': 'RRDA5RRPP8US539A3NNGC5IE8DNTW93V4B',
		'url': 'https://api.bscscan.com/api'
	},
	'avalanche': {
		'address': {
			'USDC': {
				'poolAddress': "0x1205f31718499dBf1fCa446663B532Ef87481fe1",
				'deployBlock': 12219159,
				'erc20Address': "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
			},
			'USDT': {
				'poolAddress': "0x29e38769f23701A2e4A8Ef0492e19dA4604Be62c",
				'deployBlock': 12219171,
				'erc20Address': "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
			},
		},
        'LPStaking': '0x8731d54E9D02c286767d56ac03e8037C07e01e98',
		'key': 'DDJSBNQC91M49F2216CCKMWWWX6EJ4U7DE',
		'url': 'https://api.snowtrace.io/api'
	},
	'polygon': {
		'address': {
			'USDC': {
				'poolAddress': "0x1205f31718499dBf1fCa446663B532Ef87481fe1",
				'deployBlock': 26032726,
				'erc20Address': "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
			},
			'USDT': {
				'poolAddress': "0x29e38769f23701A2e4A8Ef0492e19dA4604Be62c",
				'deployBlock': 26032728,
				'erc20Address': "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
			},
		},
        'LPStaking': '0x8731d54E9D02c286767d56ac03e8037C07e01e98',
		'key': 'HBIW4HP9DBQM8VE4W9MRNMYUZ9E3R18WMB',
		'url': 'https://api.polygonscan.com/api'
	},
	'arbitrum': {
		'address': {
			'USDC': {
				'poolAddress': "0x892785f33CdeE22A30AEF750F285E18c18040c3e",
				'deployBlock': 8041115,
				'erc20Address': "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
			},
			'USDT': {
				'poolAddress': "0xB6CfcF89a7B22988bfC96632aC2A9D6daB60d641",
				'deployBlock': 8041122,
				'erc20Address': "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
			},
			'SGETH': {
				'poolAddress': "0x915A55e36A01285A14f05dE6e81ED9cE89772f8e",
				'deployBlock': 16112680, #recently deployed
				'erc20Address': "0x82CbeCF39bEe528B5476FE6d1550af59a9dB6Fc0",
			},
		},
        'LPStaking': '0xeA8DfEE1898a7e0a59f7527F076106d7e44c2176',
		'key': 'QGB3WYFBTR648MMACKWR217U7I1GA9WRXG',
		'url': 'https://api.arbiscan.io/api',
	},
	'optimism': {
		'address': {
			'USDC': {
				'poolAddress': "0xDecC0c09c3B5f6e92EF4184125D5648a66E35298",
				'deployBlock': 4535509, # Mar-17-2022 07:52:39 AM +UTC
				'erc20Address': "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
			},
			'SGETH': {
				'poolAddress': "0xd22363e3762cA7339569F3d33EADe20127D5F98C",
				'deployBlock': 13332734, # Jun-27-2022 06:23:43 PM +UTC
				'erc20Address': "0xb69c8CBCD90A39D8D3d3ccf0a3E968511C3856A0",
			},
		},
        'LPStaking': '0x4DeA9e918c6289a52cd469cAC652727B7b412Cd2',
		'key': '65D69EGDF9SD9PG7SCUGCRDI2F6NKJD4XI',
		'url': 'https://api-optimistic.etherscan.io/api'
	},
	'fantom': {
		'address':{
			'USDC': {
				'poolAddress': "0x12edeA9cd262006cC3C4E77c90d2CD2DD4b1eb97",
				'deployBlock': 33647195 , # Mar-17-2022 07:53:27 AM +UTC
				'erc20Address': "0x04068da6c83afcfa0e13ba15a6696662335d5b75",
			},
		},
        'LPStaking': '0x224D8Fd7aB6AD4c6eb4611Ce56EF35Dec2277F03',
		'key': 'HU8VD7IF1INHGT8BR2XZH5UC1X58AISN1X',
		'url': 'https://api.ftmscan.com/api'
	}
}

def get_credentials():
    global credentials
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

def check_value(project_id, table_id, field, value):
    try:
        ### check exist variable
        query = f"SELECT {field} FROM `{table_id}` WHERE {field} = '{value}'"
        # print(query)
        exist_value = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        if len(exist_value) > 0:
            return True
        else:
            return False
    except:
        return False
def get_rows_with_max(project_id, table_id, field, cond = ''):
    try:
        query = f"SELECT * FROM `{table_id}` WHERE ({field}) IN ( SELECT MAX({field}) FROM `{table_id}` {cond})"
        rows = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        # print(rows)
        return rows
    except:
        return 0
def get_max_id(project_id, table_id):
    query = f"SELECT * FROM `{table_id}` WHERE (id) IN ( SELECT MAX(id) FROM `{table_id}`)"
    try:
        last_rows = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        if len(list(last_rows['id'])) == 0:
            return 0
        else:
            return last_rows['id'][0]
    except:
        return 0
def manipulate_tbl_config(var_name = '', value = '', action = 'update', all_flag = False):
    project_id = "nice-proposal-338510"
    table_id = "nice-proposal-338510.stg_all_info.tbl_config"
    field = "var_name"
    last_id = 0
    # pandas_gbq.read_gbq("UPDATE `nice-proposal-338510.stg_all_info.tbl_config` SET value = 'wer234234234' WHERE var_name = 'CHAIN_INFO'", project_id = "nice-proposal-338510", credentials = credentials)
    ## create new variable
    if action == 'create':
        if check_value(project_id, table_id, field, var_name):
            return ('error: exist variable')
        else:
            ### get last id
            last_id = get_max_id(project_id, table_id)
            ### set variable
            df = pandas.DataFrame(
                    {
                        "id": [last_id + 1],
                        "var_name": [var_name],
                        "value": [value],
                    }
                )
            pandas_gbq.to_gbq(df, table_id, project_id=project_id, credentials = credentials, if_exists = 'append')
            return f"success creating variable"
            
    ## UPDATE dataset.StockDetails SET color = ‘Mystic Green’ WHERE product=’SG Note 20 Ultra’
    elif action == 'update':
        if not check_value(project_id, table_id, field, var_name):
            return "error: no variable or no table"
        else:
            try:
                df = pandas_gbq.read_gbq(f"UPDATE `{table_id}` SET value = '{value}' WHERE var_name = '{var_name}'", project_id, progress_bar_type=None, credentials = credentials)
                return "success changing variable"
            except:
                return "server error"
    elif action == 'delete':
        if all_flag:
            try:
                df = pandas_gbq.read_gbq(f"DELETE FROM `{table_id}` WHERE true", project_id, progress_bar_type=None, credentials = credentials)
                return "success deleting all variables"
            except:
                return "server error"
        else:
            if not check_value(project_id, table_id, field, var_name):
                return "error: no variable or no table"
            else:
                try:
                    df = pandas_gbq.read_gbq(f"DELETE FROM `{table_id}` WHERE var_name = '{var_name}'", project_id, progress_bar_type=None, credentials = credentials)
                    return "success deleting variable"
                except:
                    return "server error"
    elif action == 'select':
        if all_flag:
            try:
                df = pandas_gbq.read_gbq(f"SELECT * FROM `{table_id}`", project_id, progress_bar_type=None, credentials = credentials)
                return df
            except:
                return "server error"
        else:
            if not check_value(project_id, table_id, field, var_name):
                return "error: no variable or no table"
            else:
                try:
                    df = pandas_gbq.read_gbq(f"SELECT * FROM `{table_id}` WHERE var_name = '{var_name}'", project_id, progress_bar_type=None, credentials = credentials)
                    return df
                except:
                    return "server error"
    else:
        return "arg3 error (insert create, select, update, delete)"

def fetch_pool_events(project_id, chain):
    decimals = 10 ** 6
    # print(project_id, table_id, chain)
    events = ['mint', 'burn', 'swap', 'swapremote']
    # table_id_list = []
    from_block = {}
    prev_rows = {}
    for pool in chain_info[chain]['address']:
        for i in range(4):
            # table_id_list.append(f'{project_id}.stg_all_info.tbl_{chain}_{pool}_{events[i]}')
            try:
                prev_rows[pool + '_' + events[i]] = get_rows_with_max(project_id, f'{project_id}.stg_all_info.tbl_{chain}_{pool}', 'block_no', f'WHERE event_type = "{events[i]}"')
                from_block[pool + '_' + events[i]] = prev_rows[pool + '_' + events[i]]['block_no'][0]
            except:
                from_block[pool + '_' + events[i]] = 0
                prev_rows[pool + '_' + events[i]] = {}
                prev_rows[pool + '_' + events[i]]['block_no'] = []
                # pass
    # print(chain, prev_rows)

    resp = {}
    for pool in chain_info[chain]['address']:
        for i in range(4):
            resp[pool + '_' + str(i)] = (requests.get(url=chain_info[chain]['url'], params={
                'module': 'logs',
                'action': 'getLogs',
                'fromBlock': from_block[pool + '_' + events[i]],
                'toBlock': 'latest',
                'topic0': event_topics[events[i]],
                'apikey': chain_info[chain]['key'],
                'address': chain_info[chain]['address'][pool]['poolAddress']
            }).json()['result'])
    
    # print(resp[0][:1])
    ## decode logs

    for pool in chain_info[chain]['address']:
        block_no = []
        time_stamp = []
        tx = []
        event_type = []
        address = []
        amount_lp = []
        amount_sd = []
        mint_fee = []
        dst_fee = []
        eq_reward = []
        eq_fee = []
        protocol_fee = []
        lp_fee = []
        dst_chain = []
        dst_pool = []
        for i in range(4):
            event = events[i]
            for row in resp[f'{pool}_{i}']:
                equal_tx_flag = True
                # print(row ,resp[pool_event])
                ## check events logs with equal tx
                for idx, block in enumerate(prev_rows[pool + '_' + events[i]]['block_no']):
                    if prev_rows[pool + '_' + events[i]]['tx'][idx] == row['transactionHash']:
                        equal_tx_flag = False
                        # print(chain, prev_rows[pool + '_' + events[i]]['tx'][idx], row['transactionHash'])
                if equal_tx_flag:
                    block_no.append(float(eval(row['blockNumber'])))
                    time_stamp.append(float(eval(row['timeStamp'])))
                    tx.append(row['transactionHash'])
                    td_str = ''.join(row['topics']) + row['data']
                    td_str = td_str.replace('0x', '')
                    td_list = textwrap.wrap(td_str, 64)
                    event_type.append(event)
                    if event == 'mint':
                        address.append('0x' + td_list[1][24:])
                        amount_lp.append(float(eval('0x' + td_list[2])) / decimals)
                        amount_sd.append(float(eval('0x' + td_list[3])) / decimals)
                        mint_fee.append(float(eval('0x' + td_list[4])) / decimals)
                        dst_fee.append(0)
                        eq_reward.append(0)
                        eq_fee.append(0)
                        protocol_fee.append(0)
                        lp_fee.append(0)
                        dst_chain.append(0)
                        dst_pool.append(0)
                    if event == 'burn':
                        address.append('0x' + td_list[1][24:])
                        amount_lp.append(float(eval('0x' + td_list[2])) / decimals)
                        amount_sd.append(float(eval('0x' + td_list[3])) / decimals)
                        mint_fee.append(0)
                        dst_fee.append(0)
                        eq_reward.append(0)
                        eq_fee.append(0)
                        protocol_fee.append(0)
                        lp_fee.append(0)
                        dst_chain.append(0)
                        dst_pool.append(0)
                    if event == 'swap':
                        dst_chain.append(int(eval('0x' + td_list[1])))
                        dst_pool.append(int(eval('0x' + td_list[2])))
                        address.append('0x' + td_list[3][24:])
                        amount_sd.append(float(eval('0x' + td_list[4])) / decimals)
                        eq_reward.append(float(eval('0x' + td_list[5])) / decimals)
                        eq_fee.append(float(eval('0x' + td_list[6])) / decimals)
                        protocol_fee.append(float(eval('0x' + td_list[7])) / decimals)
                        lp_fee.append(float(eval('0x' + td_list[8])) / decimals)
                        amount_lp.append(0)
                        mint_fee.append(0)
                        dst_fee.append(0)
                    if event == 'swapremote':
                        address.append('0x' + td_list[1][24:])
                        amount_sd.append(float(eval('0x' + td_list[2])) / decimals)
                        protocol_fee.append(float(eval('0x' + td_list[3])) / decimals)
                        dst_fee.append(float(eval('0x' + td_list[4])) / decimals)
                        amount_lp.append(0)
                        eq_fee.append(0)
                        eq_reward.append(0)
                        dst_chain.append(0)
                        dst_pool.append(0)
                        mint_fee.append(0)
                        lp_fee.append(0)


        df = pandas.DataFrame(
            {
                'block_no': block_no,
                'timestamp': time_stamp,
                'tx': tx,
                'event_type': event_type,
                'address': address,
                'amount_lp': amount_lp,
                'amount_sd': amount_sd,
                'mint_fee': mint_fee,
                'dst_fee': dst_fee,
                'eq_reward': eq_reward,
                'eq_fee': eq_fee,
                'protocol_fee': protocol_fee,
                'lp_fee': lp_fee,
                'dst_chain': dst_chain,
                'dst_pool': dst_pool,
            }
        )
        # df['block_no'] = df['block_no'].astype('string') 
        # df.to_csv(f'{project_id}.stg_all_info.tbl_{chain}_{pool_event}',sep='\t',encoding='utf-8')
        # temp_csv_string = df.to_csv(sep='\t',encoding='utf-8', index=False)
        # temp_csv_string_IO = stringIO(temp_csv_string)
        # new_df = pd.read_csv(temp_csv_string_IO, sep="\t")
        try:
            pandas_gbq.to_gbq(df, f'{project_id}.stg_all_info.tbl_{chain}_{pool}', project_id=project_id, credentials=credentials, if_exists='append', table_schema=[
                {'name': 'block_no', 'type': 'float64'},
                {'name': 'timestamp', 'type': 'float64'},
                {'name': 'tx', 'type': 'string'},
                {'name': 'event_type', 'type': 'string'},
                {'name': 'address', 'type': 'string'},
                {'name': 'amount_lp', 'type': 'float64'},
                {'name': 'amount_sd', 'type': 'float64'},
                {'name': 'mint_fee', 'type': 'float64'},
                {'name': 'dst_fee', 'type': 'float64'},
                {'name': 'eq_reward', 'type': 'float64'},
                {'name': 'eq_fee', 'type': 'float64'},
                {'name': 'protocol_fee', 'type': 'float64'},
                {'name': 'lp_fee', 'type': 'float64'},
                {'name': 'dst_chain', 'type': 'float64'},
                {'name': 'dst_pool', 'type': 'float64'}
                ])
        except:
            print('error to_gbq')
        
def delete_table(project_id, table_id):
    query = f"DROP TABLE `{table_id}`"
    # print(query)
    try:
        pandas_gbq.read_gbq(query, progress_bar_type=None, credentials = credentials)
    except:
        pass

def main():
    ## start time
    start_time = time.time()
    get_credentials()
    ## get last block number
    project_id = "nice-proposal-338510"

    ## fetch pool events logs using multithread
    for chain in chain_id:
        exec(f"{chain}_thread = threading.Thread(target=fetch_pool_events, args = ('{project_id}', '{chain}'))")
    for chain in chain_id:
        exec(f"{chain}_thread.start()")
    for chain in chain_id:
        exec(f"{chain}_thread.join()")
        

    ## fetch pool events logs using only main thread
    # for chain in chain_id:
    #     fetch_pool_events(project_id, chain)


    # DROP TABLE using multi thread
    # for chain in chain_id:
    #     for pool in pool_id:
    #         exec(f"{chain}_{pool}_thread = threading.Thread(target=delete_table, args =('{project_id}', 'nice-proposal-338510.stg_all_info.tbl_{chain}_{pool}'))")
    
    # for chain in chain_id:
    #     for pool in pool_id:
    #         exec(f"{chain}_{pool}_thread.start()")
    
    # for chain in chain_id:
    #     for pool in pool_id:
    #         exec(f"{chain}_{pool}_thread.join()")


    ## end time
    print("--- %s seconds ---" % (time.time() - start_time))
main()