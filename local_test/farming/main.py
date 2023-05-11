import pandas as pd
import numpy as np
from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
import pandas
import requests
import json
import time
import concurrent.futures
import config

client = bigquery.Client()

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

project_id = "nice-proposal-338510"
head_list = config.head_list
pool_id_list = config.pool_id_list
chain_id_list = config.chain_id_list
chain_info_list = config.chain_info_list
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

## related table - tbl_STGpB, tbl_alloc_point, tbl_time_block, tbl_stk, tbl_stablecoin, tbl_stg_price, origin table(tbl_farming)


def closest(lst, K):
     lst = np.asarray(lst)
     idx = (np.abs(lst - K)).argmin()
     return lst[idx]
def get_rows_with_max(table_id, field, cond = ''):
    try:
        query = f"SELECT * FROM `{table_id}` WHERE ({field}) IN ( SELECT MAX({field}) FROM `{table_id}` {cond})"
        rows = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        # print(rows)
        return rows
    except:
        return 0

def get_data(table_id, cond = ''):
    query = f"SELECT * FROM {project_id}.{table_id} {cond}"
    result = pandas_gbq.read_gbq(query, progress_bar_type=None, credentials=credentials)
    return (result)

def process_data():
    tbl_stablecoin_id = "Stargate.tbl_stablecoin"
    df_obj = {}
    tbl_schema = []
    ts_from_list = []
    ts_to_list = []
    predata = {}
    max_time_list = {}
    last_time = 0
    last_time_list = []

    for head in head_list:
        if head == 'TS_FROM' or head == 'TS_TO':
            tbl_schema.append({'name': head, 'type': 'int64'})
        else:
            tbl_schema.append({'name': head, 'type': 'float64'})
            
    # try:
    #     client.get_table(project_id + '.' + tbl_stablecoin_id)  # Make an API request.
    #     print("Table {} already exists.".format(tbl_stablecoin_id))
    # except NotFound:
    #     print("Table {} is not found.".format(tbl_stablecoin_id))
    for head in head_list:
        ## set dataframe
        df_obj[head] = [0]
        if 'LGR' in head:
            df_obj[head] = [1]
        if head == 'TS_FROM':
            df_obj[head] = [1647446400]  ## 2022-03-17 00:00:00
        if head == 'TS_TO':
            df_obj[head] = [1647446400 + 900]

    df = pandas.DataFrame(df_obj)
    pandas_gbq.to_gbq(df, tbl_stablecoin_id, project_id=project_id, credentials=credentials, if_exists='fail', table_schema=tbl_schema)

    
    ## get prev data

    prev_all_fields = get_rows_with_max(project_id, tbl_stablecoin_id, 'TS_FROM')
    prev_TS_TO = prev_all_fields['TS_TO'][0]
    # print(prev_TS_TO)

    ## get data to be processed (data is in after prev_TS_FROM)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for chain in chain_id_list:
            for pool in chain_info_list[chain]['address']:
                exec(f"{chain + '_' + pool} = executor.submit(get_data, 'Pool_event_logs.tbl_{chain}_{pool}', 'WHERE timestamp >= {prev_TS_TO} order by timestamp')")
                for event in event_list:
                    exec(f"{chain + '_' + pool + '_' + event} = executor.submit(get_data, 'Pool_event_logs.tbl_{chain}_{pool}', 'WHERE timestamp >= {prev_TS_TO} and event_type = \"{event}\"')")
                
        for chain in chain_id_list:
            for pool in chain_info_list[chain]['address']:
                exec(f"predata['{chain + '_' + pool}'] = {chain + '_' + pool}.result()")
                for event in event_list:
                    exec(f"max_time_list['{chain + '_' + pool + '_' + event}'] = {chain + '_' + pool + '_' + event}.result()")

    for chain in chain_id_list:
        for pool in chain_info_list[chain]['address']:
            for event in event_list:
                last_time_list.append(max(max_time_list[chain + '_' + pool + '_' + event]['timestamp']))
    
    last_time = min(last_time_list)
    print(prev_TS_TO, last_time)
    while (prev_TS_TO + 900) < last_time:
        ts_from_list.append(prev_TS_TO)
        prev_TS_TO += 900
        ts_to_list.append(prev_TS_TO)
    last_time = ts_to_list[-1]
    all_fields_dataset = {}
    for head in head_list:
        all_fields_dataset[head] = []
        for idx, ts_from in enumerate(ts_from_list):
            if head == 'TS_FROM':
                all_fields_dataset[head].append(ts_from)
            elif head == 'TS_TO':
                all_fields_dataset[head].append(ts_from + 900)
            else:
                if 'LGR' in head:
                    all_fields_dataset[head].append(1)
                else:
                    all_fields_dataset[head].append(0)
    for chain_pool in predata:
        cid = chain_id_list[chain_pool.split('_')[0]]
        pid = pool_id_list[chain_pool.split('_')[1]]
        cp_key =f"{cid}_{pid}"
        print(cp_key)
        prev_ts = prev_all_fields['TS_TO'][0]
        new_flag = True
        new_ts_flag = False
        prev_ts_from = prev_ts
        for idx, timestamp in enumerate(predata[chain_pool]['timestamp']):
            ts_from = closest(ts_from_list, timestamp)
            if ts_from >= last_time and prev_ts_from + 900 < last_time:
                while last_time > prev_ts_from + 900:
                    prev_ts_from += 900
                    all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts_from)] = all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts_from) - 1]
                    all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts_from)] = all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts_from) - 1]
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts_from)] = all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts_from) - 1]
                    all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts_from)] = all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts_from) - 1]
            prev_ts_from = closest(ts_from_list, timestamp)
            if timestamp < last_time:
                if ts_from > timestamp:
                    ts_from -= 900
                if timestamp >= prev_ts:
                    new_ts_flag = True
                event_type = predata[chain_pool]['event_type'][idx]
                if new_flag:
                    while ts_from >= prev_ts:
                        all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'TL_{cp_key}'][0]
                        all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'TLP_{cp_key}'][0]
                        all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'TB_{cp_key}'][0]
                        all_fields_dataset[f'LFC_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'LFC_{cp_key}'][0]
                        all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts)] = prev_all_fields[f'PFC_{cp_key}'][0]
                        prev_ts += 900
                    new_flag = False
                elif new_ts_flag:
                    while ts_from >= prev_ts:
                        all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts)] = all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(prev_ts) - 1]
                        prev_ts += 900
                        new_ts_flag = False
                if event_type == 'mint':
                    all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TLD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_lp'][idx]
                    all_fields_dataset[f'TLPD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_lp'][idx]
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TBD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'LA_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                if event_type == 'burn':
                    all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TLD_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TLP_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_lp'][idx]
                    all_fields_dataset[f'TLPD_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_lp'][idx]
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TBD_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'LW_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                if event_type == 'swapremote':
                    all_fields_dataset[f'TLD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['dst_fee'][idx]
                    newTL = all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] + predata[chain_pool]['dst_fee'][idx]
                    if all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] != 0:
                        all_fields_dataset[f'LGR_{cp_key}'][ts_from_list.index(ts_from)] *= newTL / all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)]
                    all_fields_dataset[f'TL_{cp_key}'][ts_from_list.index(ts_from)] = newTL
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TBD_{cp_key}'][ts_from_list.index(ts_from)] -= predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'LFC_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['lp_fee'][idx]
                    all_fields_dataset[f'LFD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['lp_fee'][idx]
                    all_fields_dataset[f'PFC_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['protocol_fee'][idx]
                    all_fields_dataset[f'PFD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['protocol_fee'][idx]
                    all_fields_dataset[f'TXAs_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                if event_type == 'swap':
                    all_fields_dataset[f'TB_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'TBD_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    all_fields_dataset[f'EQF_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['eq_fee'][idx]
                    all_fields_dataset[f'EQR_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['eq_reward'][idx]
                    all_fields_dataset[f'TXAi_{cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]
                    if len(str(int(predata[chain_pool]['dst_chain'][idx]))) != 3:
                        dst_cp_key = str(int(predata[chain_pool]['dst_chain'][idx])) + '_' + str(int(predata[chain_pool]['dst_pool'][idx]))
                    else:
                        if str(int(predata[chain_pool]['dst_chain'][idx]))[1] == '0':
                            dst_cp_key = str(int(predata[chain_pool]['dst_chain'][idx]))[-1] + '_' + str(int(predata[chain_pool]['dst_pool'][idx]))
                        else:
                            dst_cp_key = str(int(predata[chain_pool]['dst_chain'][idx]))[1:] + '_' + str(int(predata[chain_pool]['dst_pool'][idx]))
                    all_fields_dataset[f'EQF_{cp_key}_{dst_cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['eq_fee'][idx]
                    all_fields_dataset[f'EQR_{cp_key}_{dst_cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['eq_reward'][idx]
                    all_fields_dataset[f'TXAi_{cp_key}_{dst_cp_key}'][ts_from_list.index(ts_from)] += predata[chain_pool]['amount_sd'][idx]

    df = pandas.DataFrame(all_fields_dataset)
    df.to_csv('./stablecoin.csv')
    pandas_gbq.to_gbq(df, tbl_stablecoin_id, project_id=project_id, credentials=credentials, if_exists='append', table_schema=tbl_schema)
def get_pre_data(chain_id, pool_id, start_timestamp):
    result = {}
    result['alloc'] = get_data('STG_related.tbl_allocpoint', f'where timestamp >= {start_timestamp} and chain_id = {chain_id_list[chain_id]} order by timestamp')
    result['stgpb'] = get_data('STG_related.tbl_stg_per_block', f'where timestamp >= {start_timestamp} and chain_id = {chain_id_list[chain_id]} order by timestamp')
    result['stk'] = get_data(f'Staked_lp_token.tbl_{chain_id}_{pool_id}_stk', f'where TS_FROM >={start_timestamp} order by TS_FROM')
    result['tl_tlp'] = pandas_gbq.read_gbq(f'select TS_FROM, TS_TO, TL_{chain_id_list[chain_id]}_{pool_id_list[pool_id]}, TLP_{chain_id_list[chain_id]}_{pool_id_list[pool_id]} from {project_id}.Stargate.tbl_stablecoin order by TS_FROM', progress_bar_type=None, credentials=credentials)
    result['stg_price'] = get_data('STG_related.tbl_stg_price', f'where timestamp >= {start_timestamp} order by timestamp')
    return result
    ## allocpoint

def main(req1, req2):
    ## start time
    tbl_schema = []
    start_time = time.time()
    tbl_id = 'Stargate.tbl_farming'
    for head in head_list:
        if head == 'TS_FROM' or head == 'TS_TO':
            tbl_schema.append({'name': head, 'type': 'int64'})
        else:
            tbl_schema.append({'name': head, 'type': 'float64'})
    df_obj = {}
    # try:
    #     client.get_table(project_id + '.' + tbl_id)  # Make an API request.
    #     print("Table {} already exists.".format(tbl_id))
    # except NotFound:
    #     print("Table {} is not found.".format(tbl_id))
    for head in head_list:
        ## set dataframe
        df_obj[head] = [0]
        if head == 'TS_FROM':
            df_obj[head] = [1647446400]  ## 2022-03-17 00:00:00
        if head == 'TS_TO':
            df_obj[head] = [1647446400 + 900]

    df = pandas.DataFrame(df_obj)
    pandas_gbq.to_gbq(df, tbl_id, project_id=project_id, credentials=credentials, if_exists='replace', table_schema=tbl_schema)

    prev_row = get_rows_with_max(tbl_id, 'TS_FROM')
    prev_ts_to = prev_row['TS_TO'][0]
    pre_data = get_pre_data('ethereum', 'USDC', prev_ts_to)
    print(pre_data)
    ## end time
    print("--- %s seconds ---" % (time.time() - start_time))

main(1,2)