import pandas as pd
import numpy as np
from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
import pandas
import requests
import threading
import time
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

def process_data(chain_id, pool_id):
    tbl_id = f'Staked_lp_token.tbl_{chain_id}_{pool_id}_stk'
    pool_id = pool_id_list[pool_id]
    ts_from_list = []
    ts_to_list = []
    tbl_schema = []
    df_obj = {}
    
    for head in head_list:
        if head == 'TS_FROM' or head == 'TS_TO':
            tbl_schema.append({'name': head, 'type': 'int64'})
        else:
            tbl_schema.append({'name': head, 'type': 'float64'})

    # try:
    #     client.get_table(project_id + '.' + tbl_id)  # Make an API request.
    #     print("Table {} already exists.".format(tbl_id))
    # except NotFound:
    #     print("Table {} is not found.".format(tbl_id))
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
    pandas_gbq.to_gbq(df, tbl_id, project_id=project_id, credentials=credentials, if_exists='replace', table_schema=tbl_schema)


    prev_row = get_rows_with_max(tbl_id, 'TS_FROM')
    prev_TS_TO = prev_row['TS_TO'][0]
    pre_data = get_data(f'Stake_event_logs.tbl_{chain_id}_deposit_withdraw', f"where timestamp >= {prev_TS_TO} and pool_id = {pool_id} order by timestamp")
    last_time = min(max([pre_data['timestamp'][idx] for idx, x in enumerate(pre_data['event_type']) if x == 0]), max([pre_data['timestamp'][idx] for idx, x in enumerate(pre_data['event_type']) if x == 1]))
    while (prev_TS_TO + 900) < last_time:
        ts_from_list.append(prev_TS_TO)
        prev_TS_TO += 900
        ts_to_list.append(prev_TS_TO)
    last_time = ts_to_list[-1]
    prev_TS_TO = prev_row['TS_TO'][0]

    all_fields_dataset = {}
    for head in head_list:
        all_fields_dataset[head] = []
        for ts_from in ts_from_list:
            if head == 'TS_FROM':
                all_fields_dataset[head].append(ts_from)
            elif head == 'TS_TO':
                all_fields_dataset[head].append(ts_from + 900)
            else:
                all_fields_dataset[head].append(0)
    new_flag = True
    new_ts_flag = False
    for idx, timestamp in enumerate(pre_data['timestamp']):
        ts_from = closest(ts_from_list, timestamp)
        if ts_from > timestamp:
            ts_from -= 900
        if timestamp >= prev_TS_TO:
            new_ts_flag = True
        if new_flag:
            while ts_from >= prev_TS_TO:
                all_fields_dataset['amount'][ts_from_list.index(prev_TS_TO)] = prev_row['amount'][0]
                prev_TS_TO += 900
            new_flag = False
        elif new_ts_flag:
            while ts_from >= prev_TS_TO:
                all_fields_dataset['amount'][ts_from_list.index(prev_TS_TO)] = all_fields_dataset['amount'][ts_from_list.index(prev_TS_TO) - 1]
                prev_TS_TO += 900
            new_ts_flag = False
        if ts_from < last_time:
            event_type = pre_data['event_type'][idx]
            if event_type == 0:
                all_fields_dataset['amount'][ts_from_list.index(ts_from)] += pre_data['amount'][idx]
                pass
            if event_type == 1:
                all_fields_dataset['amount'][ts_from_list.index(ts_from)] -= pre_data['amount'][idx]
    df = pandas.DataFrame(all_fields_dataset)
    pandas_gbq.to_gbq(df, tbl_id, project_id=project_id, credentials=credentials, if_exists='replace', table_schema=tbl_schema)

def main(req1, req2):
    ## start time
    start_time = time.time()

    ## fetch pool events logs using multithread
    for chain_id in chain_id_list:
        for pool_id in chain_info_list[chain_id]['address']:
            exec(f"{chain_id}_{pool_id}_thread = threading.Thread(target=process_data, args = (['{chain_id}', '{pool_id}']))")
    for chain_id in chain_id_list:
        for pool_id in chain_info_list[chain_id]['address']:
            exec(f"{chain_id}_{pool_id}_thread.start()")
    for chain_id in chain_id_list:
        for pool_id in chain_info_list[chain_id]['address']:
            exec(f"{chain_id}_{pool_id}_thread.join()")
    
    ## end time
    print("--- %s seconds ---" % (time.time() - start_time))

main(1,2)