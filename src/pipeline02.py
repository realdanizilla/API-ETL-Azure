# este arquivo importa informações da coinbase
import time
import requests
from tinydb import TinyDB
from datetime import datetime

def extract_data_btc():
    url = 'https://api.coinbase.com/v2/prices/spot' # url identifica o endpoint
    response = requests.get(url)
    dados = response.json()
    return dados

def transform_data_btc(dados):
    valor = dados['data']['amount']
    criptomoeda = dados['data']['base']
    moeda = dados['data']['currency']
    timestamp = datetime.now().timestamp()

    dados_transformados = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp
    }
    return dados_transformados

def save_data_btc_tinydb(dados, db_name="bitcoin.json"):
    db = TinyDB(db_name)
    db.insert(dados)
    print("dados salvos com sucesso")


if __name__ == "__main__":
    # Extração dos dados
    while True: # esse é o modo cowboy para orquestrar, mas tem outros
        dados_json = extract_data_btc()
        dados_tratados = transform_data_btc(dados_json)
        save_data_btc_tinydb(dados_tratados)
        print(dados_tratados)
        time.sleep(15)
