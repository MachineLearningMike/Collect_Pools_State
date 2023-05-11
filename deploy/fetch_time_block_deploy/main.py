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
chain_id_list = config.chain_id_list
chain_info_list = config.chain_info_list
tbl_schema = config.tbl_schema

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

def get_rows_with_max(project_id, table_id, field, cond = ''):
    try:
        query = f"SELECT * FROM `{table_id}` WHERE ({field}) IN ( SELECT MAX({field}) FROM `{table_id}` {cond})"
        rows = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        return rows
    except:
        return {}

def fetch_pool_events(chain):
    ts_from = 0
    last_time = int(time.time()) - 900 * 4 * 24
    prev_rows = {}
    prev_rows = get_rows_with_max(project_id, f'{project_id}.STG_related.tbl_{chain}_time_block', 'timestamp')
    if len(prev_rows) == 0:
        ts_from = 1647446400
    else:
        ts_from = prev_rows['timestamp'][0] + 900

    block_list = []
    timestamp_list = []
    for idx in range(20):
        resp = requests.get(url=chain_info_list[chain]['url'], params={
            'module': 'block',
            'action': 'getblocknobytime',
            'timestamp': ts_from + idx * 900,
            'closest': 'before',
            'apikey': chain_info_list[chain]['key']
        }).json()
        if resp['status'] == '1':
            block_list.append(int(resp['result']))
        else:
            block_list.append(-1)
        timestamp_list.append(ts_from + idx * 900)
    print(block_list)
    # return
    df = pandas.DataFrame(
        {
            'block_no': block_list,
            'timestamp': timestamp_list,
        }
    )
    try:
        pandas_gbq.to_gbq(df, f'{project_id}.STG_related.tbl_{chain}_time_block', project_id=project_id, credentials=credentials, if_exists='append', table_schema=tbl_schema)
    except:
        print('error to_gbq')

def main(req1, req2):
    ## start time
    start_time = time.time()

    # fetch pool events logs using multithread
    for chain_id in chain_id_list:
        exec(f"{chain_id}_thread = threading.Thread(target=fetch_pool_events, args = (['{chain_id}']))")
    for chain_id in chain_id_list:
        exec(f"{chain_id}_thread.start()")
    for chain_id in chain_id_list:
        exec(f"{chain_id}_thread.join()")

    ## end time
    print("--- %s seconds ---" % (time.time() - start_time))

main(1,2)