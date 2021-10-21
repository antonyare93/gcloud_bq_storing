import requests
import time
import json
from google.cloud import bigquery

cliente = bigquery.Client(project="api-testing-arias")

url = "https://api.coindesk.com/v1/bpi/currentprice.json"

while True:

    r = requests.get(url)
    resp: str = r.text
    json_resp = r.json()
    updt_time = json_resp['time']
    actualizacion = updt_time['updatedISO']
    prices = json_resp['bpi']
    usd_data = prices['USD']
    eur_data = prices['EUR']
    gbp_data = prices['GBP']
    usd_price = usd_data['rate_float']
    eur_price = eur_data['rate_float']
    gbp_price = gbp_data['rate_float']

    row_to_insert = [{u"EUR_Price": eur_price, u"GBP_Price": gbp_price, u"USD_Price": usd_price, u"Time": actualizacion}]

    query_job = cliente.insert_rows_json("api-testing-arias.btc_coindesk.btc_coindesk", row_to_insert)
    time.sleep(120.00)