import requests
import json

url = "https://api.openai.com/v1/chat/completions"

headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer sk-proj-123456789" # token API

}

data = {
    "model": "gpt-3.5-turbo",
    "messages": [{"role": "user", "content": "Qual é a capital da França"}]
}

response = requests.post(url, headers=headers, data=json.dumps(data))

print(response.json()['choices'][0]['messages']['content'])

