import ccxt
import pandas as pd
from google.oauth2 import service_account
import datetime
import time
import pandas_gbq

project_id = 'nice-proposal-338510'
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
    {'name': 'timestamp', 'type': 'int64'},
    {'name': 'open', 'type': 'float64'},
    {'name': 'high', 'type': 'float64'},
    {'name': 'low', 'type': 'float64'},
    {'name': 'close', 'type': 'float64'},
    {'name': 'volume', 'type': 'float64'},
]
def get_rows_with_max(project_id, table_id, field, cond = ''):
    try:
        query = f"SELECT * FROM `{table_id}` WHERE ({field}) IN ( SELECT MAX({field}) FROM `{table_id}` {cond})"
        rows = pandas_gbq.read_gbq(query, project_id, progress_bar_type=None, credentials = credentials)
        return rows
    except:
        return {}

def main(req1, req2):
    start_time = time.time()
    prev_rows = {}
    last_timestamp = 0
    prev_rows = get_rows_with_max(project_id, f'{project_id}.STG_related.tbl_stg_price', 'timestamp')
    if len(prev_rows) == 0:
        last_timestamp = 1648216800 + (5000 - 1) * 60   # Fri Mar 25 2022 14:00:00 GMT+0000
    else:
        last_timestamp = prev_rows['timestamp'][0] + 5000 * 60

    ftx = ccxt.ftx({
        'enableRateLimit': True,
        'apiKey': 'aqNgSxz_03v204hNnQRCCMV8uFARz8zNly9tMllr',
        'secret': 'kWY4FawaUqsesnuvfwa_gGHsWdstX798Sr205PKf'
    })
    print(last_timestamp)
    result = ftx.fetch_ohlcv('STG/USD', timeframe='1m', limit=5000,
                                params={'until': last_timestamp * 1000})

    df = pd.DataFrame(result, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = df['timestamp'].div(1000)
    
    try:
        pandas_gbq.to_gbq(df, f'{project_id}.STG_related.tbl_stg_price', project_id=project_id, credentials=credentials, if_exists='append', table_schema=tbl_schema)
    except:
        print('error to_gbq')
    # df.to_csv('out.csv')
    print("--- %s seconds ---" % (time.time() - start_time))