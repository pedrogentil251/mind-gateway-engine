import os
import requests

API_KEY = os.getenv("OPENROUTER_API_KEY")
if not API_KEY:
    print("ERRO: A variável OPENROUTER_API_KEY não foi configurada no terminal.")
    exit()

URL = "https://openrouter.ai/api/v1/chat/completions"
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

print("--- Assistente OpenRouter ---")
prompt = input("Digite sua dúvida: ")

payload = {
    "model": "anthropic/claude-sonnet-4-6",
    "messages": [{"role": "user", "content": prompt}]
}

print("\nProcessando... Aguarde.\n")

response = requests.post(URL, headers=headers, json=payload)

if response.status_code == 200:
    print("Resposta da IA:\n")
    print(response.json()["choices"][0]["message"]["content"])
else:
    print(f"Erro {response.status_code}: {response.text}")
