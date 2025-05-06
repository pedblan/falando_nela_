import os

def logar_openai():
    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key is None:
            with open('../api_key.txt', 'r') as f:
                api_key = f.read().strip()
                return api_key
    except:
        raise EnvironmentError("A chave da API OpenAI não foi encontrada nas variáveis de ambiente nem em api_key.txt.")
