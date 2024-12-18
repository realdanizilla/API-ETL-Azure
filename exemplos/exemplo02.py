import requests

url = 'https://jsonplaceholder.typicode.com/comments'
params = {"post Id":1}
response = requests.get(url, params=params)

comentarios = response.json()
print(f"foram encontrados {len(comentarios)} comentários.")
print(f"erro: {response.status_code} - {response.text}")