import requests
import json
import os
import time
import random
import calendar
from datetime import datetime
import xml.etree.ElementTree as ET

# Configuração
BASE_URL = "http://legis.senado.leg.br/dadosabertos"
OUTPUT_DIR = "../../data/discursos/requisicoes"
LOG_FILE = "../../data/discursos/logs/erros_requisicao_discursos_log.txt"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Lista de senadores já coletada

SENADORES_FILE = "../../data/senadores/senadores_legislatura_57.json"
with open(SENADORES_FILE, "r", encoding="utf-8") as f:
    senadores = json.load(f)

# Função para determinar datas completas de mandato e salvar em JSON
def salvar_mandatos_em_json(senadores):
    mandatos = {}
    for senador in senadores:
        codigo = senador['IdentificacaoParlamentar']['CodigoParlamentar']
        datas_ativos = []
        for mandato in senador.get('Mandatos', {}).get('Mandato', []):
            if 'PrimeiraLegislaturaDoMandato' in mandato:
                data_inicio = mandato['PrimeiraLegislaturaDoMandato']['DataInicio']
                data_fim = mandato['PrimeiraLegislaturaDoMandato']['DataFim']
                datas_ativos.append({"DataInicio": data_inicio, "DataFim": data_fim})
            if 'SegundaLegislaturaDoMandato' in mandato:
                data_inicio = mandato['SegundaLegislaturaDoMandato']['DataInicio']
                data_fim = mandato['SegundaLegislaturaDoMandato']['DataFim']
                datas_ativos.append({"DataInicio": data_inicio, "DataFim": data_fim})
        mandatos[codigo] = datas_ativos

    # Salvar no arquivo JSON
    with open("../../data/senadores/mandatos_temp.json", "w", encoding="utf-8") as f:
        json.dump(mandatos, f, ensure_ascii=False, indent=4)

    print("Mandatos salvos em mandatos_temp.json")

# Função para registrar erros em um log
def registrar_erro(mensagem):
    with open(LOG_FILE, "a", encoding="utf-8") as log:
        log.write(mensagem + "\n")

# Função para verificar discursos por ano, trimestre e mês
def verificar_discursos_detalhado():
    # Carregar mandatos
    with open("../../data/senadores/mandatos_temp.json", "r", encoding="utf-8") as f:
        mandatos = json.load(f)

    for codigo, periodos in mandatos.items():
        output_file = os.path.join(OUTPUT_DIR, f"discursos_{codigo}.json")
        if os.path.exists(output_file):
            print(f"Resultados já existem para senador {codigo}. Pulando...")
            continue

        print(f"Processando senador {codigo}...")
        resultados = {}
        for periodo in periodos:
            data_inicio = datetime.strptime(periodo["DataInicio"], "%Y-%m-%d")
            data_fim = datetime.strptime(periodo["DataFim"], "%Y-%m-%d")

            for ano in range(data_inicio.year, min(data_fim.year, 2025) + 1):
                print(f"  Verificando discursos no ano {ano}...")
                data_inicio_ano = f"{ano}0101"
                data_fim_ano = f"{ano}1231"

                url = f"{BASE_URL}/senador/{codigo}/discursos?dataInicio={data_inicio_ano}&dataFim={data_fim_ano}"
                time.sleep(random.uniform(0.5, 1.5))

                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    root = ET.fromstring(response.text)
                    if root.find(".//Pronunciamento") is not None:
                        resultados[ano] = {}
                        for trimestre in range(1, 5):
                            mes_inicio = (trimestre - 1) * 3 + 1
                            mes_fim = trimestre * 3

                            data_inicio_trim = f"{ano}{mes_inicio:02}01"
                            ultimo_dia_trim = calendar.monthrange(ano, mes_fim)[1]
                            data_fim_trim = f"{ano}{mes_fim:02}{ultimo_dia_trim}"

                            print(f"    Verificando trimestre {trimestre}/{ano}...")
                            url_trim = f"{BASE_URL}/senador/{codigo}/discursos?dataInicio={data_inicio_trim}&dataFim={data_fim_trim}"
                            time.sleep(random.uniform(0.5, 1.5))

                            try:
                                response_trim = requests.get(url_trim)
                                response_trim.raise_for_status()
                                root_trim = ET.fromstring(response_trim.text)
                                if root_trim.find(".//Pronunciamento") is not None:
                                    resultados[ano][trimestre] = {}
                                    for mes in range(mes_inicio, mes_fim + 1):
                                        data_inicio_mes = f"{ano}{mes:02}01"
                                        ultimo_dia_mes = calendar.monthrange(ano, mes)[1]
                                        data_fim_mes = f"{ano}{mes:02}{ultimo_dia_mes}"

                                        print(f"      Verificando mês {mes:02}/{ano}...")
                                        url_mes = f"{BASE_URL}/senador/{codigo}/discursos?dataInicio={data_inicio_mes}&dataFim={data_fim_mes}"
                                        time.sleep(random.uniform(0.5, 1.5))

                                        try:
                                            response_mes = requests.get(url_mes)
                                            response_mes.raise_for_status()
                                            root_mes = ET.fromstring(response_mes.text)
                                            pronunciamentos = []

                                            for pron in root_mes.findall(".//Pronunciamento"):
                                                pronunciamento = {
                                                    "CodigoPronunciamento": pron.findtext("CodigoPronunciamento"),
                                                    "DataPronunciamento": pron.findtext("DataPronunciamento"),
                                                    "TextoResumo": pron.findtext("TextoResumo"),
                                                    "TipoUsoPalavra": pron.find("TipoUsoPalavra/Descricao").text if pron.find("TipoUsoPalavra/Descricao") else None,
                                                    "UrlTexto": pron.findtext("UrlTexto"),
                                                    "Indexacao": pron.findtext("Indexacao"),
                                                    "SiglaPartidoParlamentarNaData": pron.findtext("SiglaPartidoParlamentarNaData"),
                                                    "UfParlamentarNaData": pron.findtext("UfParlamentarNaData"),
                                                    "SiglaCasaPronunciamento": pron.findtext("SiglaCasaPronunciamento"),
                                                    "SessaoPlenaria": {
                                                        "CodigoSessao": pron.findtext("SessaoPlenaria/CodigoSessao"),
                                                        "NomeCasaSessao": pron.findtext("SessaoPlenaria/NomeCasaSessao"),
                                                        "DataSessao": pron.findtext("SessaoPlenaria/DataSessao"),
                                                        "HoraInicioSessao": pron.findtext("SessaoPlenaria/HoraInicioSessao"),
                                                    },
                                                    "Aparteantes": [
                                                        {
                                                            "CodigoParlamentar": aparte.findtext("CodigoParlamentar"),
                                                            "NomeAparteante": aparte.findtext("NomeAparteante")
                                                        }
                                                        for aparte in pron.findall("Aparteantes/Aparteante")
                                                    ],
                                                    "Publicacoes": [
                                                        {
                                                            "DescricaoVeiculoPublicacao": pub.findtext("DescricaoVeiculoPublicacao"),
                                                            "DataPublicacao": pub.findtext("DataPublicacao"),
                                                            "UrlDiario": pub.findtext("UrlDiario")
                                                        }
                                                        for pub in pron.findall("Publicacoes/Publicacao")
                                                    ]
                                                }
                                                pronunciamentos.append(pronunciamento)

                                            if pronunciamentos:
                                                resultados[ano][trimestre][mes] = pronunciamentos
                                        except requests.exceptions.RequestException as e:
                                            mensagem = f"        Erro ao verificar discursos para {codigo} em {mes:02}/{ano}: {e}"
                                            print(mensagem)
                                            registrar_erro(mensagem)
                                            resultados[ano][trimestre][mes] = []
                                else:
                                    resultados[ano][trimestre] = {}
                            except requests.exceptions.RequestException as e:
                                mensagem = f"      Erro ao verificar discursos para {codigo} no trimestre {trimestre}/{ano}: {e}"
                                print(mensagem)
                                registrar_erro(mensagem)
                                resultados[ano][trimestre] = {}
                    else:
                        resultados[ano] = {}
                except requests.exceptions.RequestException as e:
                    mensagem = f"    Erro ao verificar discursos para {codigo} em {ano}: {e}"
                    print(mensagem)
                    registrar_erro(mensagem)
                    resultados[ano] = {}

        # Salvar resultados individuais por senador
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=4)

        print(f"Resultados salvos para senador {codigo} em {output_file}")

def main():
    salvar_mandatos_em_json(senadores)
    verificar_discursos_detalhado()

if __name__ == "__main__":
    main()