import requests
import json
import os

# Configuração das legislaturas
LEGISLATURA_INICIO = input("Digite a legislatura inicial: ")
LEGISLATURA_FIM = input("Digite a legislatura final: ")

def coletar(LEGISLATURA_INICIO, LEGISLATURA_FIM):
    """Coleta dados de senadores em exercício de legislaturas específicas.

    Args:
        LEGISLATURA_INICIO (str): Número da legislatura inicial para a busca.
        LEGISLATURA_FIM (str): Número da legislatura final para a busca.

    Returns:
        dict: Dados obtidos da API do Senado.

    Raises:
        requests.exceptions.RequestException: Caso haja falha na requisição HTTP.
        json.JSONDecodeError: Caso a resposta não possa ser decodificada como JSON.

    Exemplo:
        coletar("53", "55")
    """
    # URL base do serviço
    BASE_URL = "https://legis.senado.leg.br/dadosabertos/senador/lista/legislatura"
    params = {
        "exercicio": "s"  # Filtra apenas senadores que entraram em exercício
    }
    # Cabeçalhos para solicitar resposta em JSON
    headers = {
        "Accept": "application/json"
    }
    # Monta a URL final
    if LEGISLATURA_INICIO == LEGISLATURA_FIM:
        url = f"{BASE_URL}/{LEGISLATURA_INICIO}"
    else:
        url = f"{BASE_URL}/{LEGISLATURA_INICIO}/{LEGISLATURA_FIM}"
    try:
        # Faz a requisição
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Levanta exceção para erros HTTP

        # Processa a resposta em JSON
        data = response.json()


    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar o serviço: {e}")
    except json.JSONDecodeError:
        print("Erro ao processar a resposta como JSON.")

    return data


def salvar(dados):
    """Salva dados coletados dos senadores em um arquivo JSON.

        Args:
            dados (dict): Dados coletados da API.

        Exemplo:
            salvar(dados)
        """
    # Limpa os dados
    dados_limpos = dados['ListaParlamentarLegislatura']['Parlamentares']['Parlamentar']
    # Diretório para salvar o arquivo
    output_dir = "../../data/senadores"
    os.makedirs(output_dir, exist_ok=True)
    if LEGISLATURA_INICIO == LEGISLATURA_FIM:
        output_file = f"{output_dir}/senadores_legislatura_{LEGISLATURA_INICIO}.json"
    else:
        output_file = f"{output_dir}/senadores_legislaturas_{LEGISLATURA_INICIO}_a_{LEGISLATURA_FIM}.json"

    # Salva os dados em um arquivo JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados_limpos, f, ensure_ascii=False, indent=4)

    print(f"Dados salvos com sucesso no arquivo {output_file}")

def main():
    dados = coletar(LEGISLATURA_INICIO, LEGISLATURA_FIM)
    salvar(dados)

if __name__ == "__main__":
    main()