from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
import pandas
import requests
import threading
import time
import config
# client = bigquery.Client()
# from google.cloud.exceptions import NotFound

project_id = 'nice-proposal-338510'
chain_id_list = config.chain_id_list
pool_id_list = config.pool_id_list
chain_info_list = config.chain_info_list
event_topics = config.event_topics
decimals = 10 ** 6
tbl_schema = config.tbl_schema
pool_topic_list = config.pool_topic_list
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

def get_rows_with_max(table_id, field, cond = ''):
    try:
        query = f"SELECT * FROM `{table_id}` WHERE ({field}) IN ( SELECT MAX({field}) FROM `{table_id}` {cond})"
        rows = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        # print(rows)
        return rows
    except:
        # print('error')
        return {}
    
def fetch_lpstake_events(chain_id):
    # 0 - deposit,  1 - withdraw in event_topics
    prev_rows = []
    resp_list = []
    for i in range(2):
        prev_rows.append(get_rows_with_max(f'Stake_event_logs.tbl_{chain_id}_deposit_withdraw', 'block_no', f'WHERE event_type = {i}'))

    for i in range(2):
        if len(prev_rows[i]) == 0:
            from_block = 0
        else:
            from_block = prev_rows[i]['block_no'][0]
        resp_list.append(requests.get(url=chain_info_list[chain_id]['url'], params={
            'module': 'logs',
            'action': 'getLogs',
            'fromBlock': from_block,
            'toBlock': 'latest',
            'address': chain_info_list[chain_id]['LPStaking'],
            'topic0': event_topics[i],
            'key': chain_info_list[chain_id]['key']
        }).json())
        time.sleep(5)

    for idx, resp in enumerate(resp_list):
        block_no_list = []
        timestamp_list = []
        event_type_list = []
        amount_list = []
        tx_list = []
        pool_list = []
        for row in resp['result']:
            equal_flag = True
            if len(prev_rows[idx]) != 0:
                for tx in prev_rows[idx]['tx']:
                    if row['transactionHash'] == tx:
                        equal_flag = False
                        print(tx)
                        break
            if equal_flag:
                block_no_list.append(int(eval(row['blockNumber'])))
                timestamp_list.append(int(eval(row['timeStamp'])))
                tx_list.append(row['transactionHash'])
                event_type_list.append(idx)
                amount_list.append((float(eval(row['data']))) / decimals)
                pool_list.append(pool_id_list[pool_topic_list[chain_id][row['topics'][2]]])
        df = pandas.DataFrame({
            'pool_id': pool_list,
            'block_no': block_no_list,
            'timestamp': timestamp_list,
            'tx': tx_list,
            'event_type': event_type_list,
            'amount': amount_list
        })
        try:
            pandas_gbq.to_gbq(df, f'{project_id}.Stake_event_logs.tbl_{chain_id}_deposit_withdraw', project_id=project_id, credentials=credentials, if_exists='append', table_schema=tbl_schema)
        except:
            print('big query error')

def main(req1, req2):
    start_time = time.time()

    ## fetch lpstake events logs using multithread
    for chain_id in chain_id_list:
        exec(f"{chain_id}_thread = threading.Thread(target=fetch_lpstake_events, args = (['{chain_id}']))")
    for chain_id in chain_id_list:
        exec(f"{chain_id}_thread.start()")
    for chain_id in chain_id_list:
        exec(f"{chain_id}_thread.join()")
    print("--- %s seconds ---" % (time.time() - start_time))
