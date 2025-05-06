import json
import requests
import os

# Lista de senadores já coletada (limpa)
DIRETORIO_SENADORES = "../../data/senadores"
os.makedirs(DIRETORIO_SENADORES, exist_ok=True)
ARQUIVO_JSON = input("Digite o nome do arquivo JSON com os metadados dos senadores (exemplo: senadores_legislatura_57.json): ")
SENADORES_FILE = os.path.join(DIRETORIO_SENADORES, ARQUIVO_JSON)
BASE_URL = "https://legis.senado.leg.br/dadosabertos"


def confere_lista(senadores_file):
    """Confere e retorna uma lista de códigos parlamentares a partir de um arquivo JSON.

    Args:
        senadores_file (str): Caminho para o arquivo JSON com a lista de senadores.

    Returns:
        list: Lista de códigos parlamentares.
    """
    with open(senadores_file, "r", encoding="utf-8") as f:
        senadores = json.load(f)
    lista_codigos_parlamentares = [senador['IdentificacaoParlamentar']['CodigoParlamentar'] for senador in senadores]
    return lista_codigos_parlamentares


def fetch_data(endpoint, params):
    """Realiza requisição GET para um endpoint específico da API do Senado.

    Args:
        endpoint (str): Endpoint específico da API.
        params (dict): Parâmetros da requisição.

    Returns:
        str: Dados obtidos da API em formato XML.

    Raises:
        requests.exceptions.RequestException: Erro ao realizar a requisição.
    """
    url = f"{BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar o serviço: {e}")
        return None


def collect_and_save_data(lista_codigos_parlamentares, endpoint_template, filename):
    """Coleta e salva dados específicos dos senadores em um único arquivo XML.

    Args:
        lista_codigos_parlamentares (list): Lista dos códigos parlamentares.
        endpoint_template (str): Template do endpoint da API.
        filename (str): Nome do arquivo XML para salvar os dados.
    """
    dados = []

    for codigo in lista_codigos_parlamentares:
        print(f"Processando {filename} para o senador com código: {codigo}")
        data = fetch_data(endpoint_template.format(codigo=codigo), {})
        if data:
            dados.append({"codigo": codigo, "data": data})
        print(f"Finalizado para o senador {codigo}.\n")

    output_file = f"{DIRETORIO_SENADORES}/{filename}.xml"
    with open(output_file, "w", encoding="utf-8") as f:
        for item in dados:
            f.write(f"<!-- Senador: {item['codigo']} -->\n")
            f.write(item['data'])
            f.write("\n")

    print(f"Todos os dados foram salvos em {output_file}")


def main():
    """Função principal que executa todas as coletas e salvamentos dos dados dos senadores."""
    lista_codigos_parlamentares = confere_lista(SENADORES_FILE)
    endpoints = {
        "autorias": "senador/{codigo}/autorias",
        "cargos": "senador/{codigo}/cargos",
        "comissoes": "senador/{codigo}/comissoes",
        "filiacoes": "senador/{codigo}/filiacoes",
        "historico_academico": "senador/{codigo}/historicoAcademico",
        "licencas": "senador/{codigo}/licencas",
        "liderancas": "senador/{codigo}/liderancas",
        "mandatos": "senador/{codigo}/mandatos",
        "profissoes": "senador/{codigo}/profissao",
        "votacoes": "senador/{codigo}/votacoes"
    }

    for filename, endpoint in endpoints.items():
        collect_and_save_data(lista_codigos_parlamentares, endpoint, filename)


if __name__ == "__main__":
    main()
