import json
import os
import time
import random
import requests
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from openai import OpenAI
import stat

caminho_geckodriver = "./geckodriver"
# Adiciona permissão de execução ao proprietário, grupo e outros
os.chmod(caminho_geckodriver, os.stat(caminho_geckodriver).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


# Configuração do log
log_file_path = os.path.join(os.getcwd(), '../../data/discursos/logs/download_textos_log.log')
if not os.path.exists(log_file_path):
    open(log_file_path, 'w').close()
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Baixar:
    def __init__(self, geckodriver_path='./geckodriver'):
        firefox_options = Options()
        firefox_options.add_argument('--headless')
        firefox_options.add_argument('--disable-gpu')
        firefox_options.add_argument('--no-sandbox')
        if not os.path.isfile(geckodriver_path):
            raise FileNotFoundError(f"O GeckoDriver não foi encontrado no caminho especificado: {geckodriver_path}")
        service = Service(geckodriver_path)
        self.driver = webdriver.Firefox(service=service, options=firefox_options)
        self.openai_client = self.get_openai_client()

    @staticmethod
    def get_openai_client() -> OpenAI:
        """
        Obtém o cliente da API OpenAI usando a chave da variável de ambiente.
        """
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("A chave da API OpenAI não foi encontrada nas variáveis de ambiente.")
        return OpenAI(api_key=api_key)

    def transcrever_com_api(self, arquivo_de_audio: str, idioma: str = "pt") -> str:
        """
        Transcreve um arquivo de áudio usando a API da OpenAI.
        """
        try:
            with open(arquivo_de_audio, "rb") as audio_file:
                response = self.openai_client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file,
                    language=idioma
                )
                return response.text
        except Exception as e:
            return f"Erro ao transcrever o áudio: {str(e)}"

    @staticmethod
    def baixar_video_direto(url, output_folder):
        """
        Baixa um vídeo diretamente pelo URL.
        """
        try:
            time.sleep(random.random())
            filename = os.path.join(output_folder, url.split("/")[-1])
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            logging.info(f"Vídeo baixado com sucesso: {url}")
            print(f"Vídeo baixado com sucesso: {filename}")
            return filename
        except requests.RequestException as e:
            logging.error(f"Erro ao baixar vídeo: {url} - {e}")
            print(f"Erro ao baixar vídeo: {e}")
            return None

    @staticmethod
    def salvar_json_atualizado(json_path, discursos):
        """
        Salva os resultados atualizados em um arquivo JSON.
        """
        if not discursos:
            print("Nenhum discurso para salvar no JSON.")
            return

        try:
            with open(json_path, mode='w', encoding='utf-8') as file:
                json.dump(discursos, file, indent=4, ensure_ascii=False)
            print(f"Dados atualizados e salvos no JSON: {json_path}")
        except IOError as e:
            print(f"Erro ao salvar discursos em JSON: {e}")

    def processar_discursos(self, discursos):
        """
        Processa discursos de diferentes estruturas JSON, lidando com variações na hierarquia e campos opcionais.
        """
        processed = []

        def extrair_discursos(conteudo):
            """Extrai discursos de diferentes níveis da hierarquia."""
            if isinstance(conteudo, list):
                processed.extend(conteudo)
            elif isinstance(conteudo, dict):
                for subconteudo in conteudo.values():
                    extrair_discursos(subconteudo)

        extrair_discursos(discursos)
        return processed

    def dividir_arquivos(self, total_partes, parte_atual):
        """
        Divide os arquivos JSON em partes para processamento paralelo ou sequencial.
        """
        requisicoes_folder = '../../data/discursos/requisicoes'
        arquivos = [f for f in os.listdir(requisicoes_folder) if f.startswith("discursos_") and f.endswith(".json")]
        arquivos.sort()  # Ordenar para garantir consistência

        total_arquivos = len(arquivos)
        tamanho_parte = (total_arquivos + total_partes - 1) // total_partes  # Tamanho aproximado da parte
        inicio = (parte_atual - 1) * tamanho_parte
        fim = inicio + tamanho_parte

        return arquivos[inicio:fim]

    def baixar_textos(self, total_partes=1, parte_atual=1):
        try:
            # Obter a divisão de arquivos
            arquivos_para_processar = self.dividir_arquivos(total_partes, parte_atual)
            base_folder = '../../data/discursos/txt'
            video_folder = '../../data/discursos/videos'
            os.makedirs(base_folder, exist_ok=True)
            os.makedirs(video_folder, exist_ok=True)

            for json_file in arquivos_para_processar:
                json_path = os.path.join('../../data/discursos/requisicoes', json_file)

                try:
                    with open(json_path, "r", encoding="utf-8") as file:
                        discursos = json.load(file)
                except json.JSONDecodeError as e:
                    logging.error(
                        f"Erro ao carregar o arquivo JSON '{json_path}': Linha {e.lineno}, Coluna {e.colno} - {e.msg}")
                    print(
                        f"Erro ao carregar o arquivo JSON '{json_path}': Verifique o arquivo para consertar o problema.")
                    continue
                except Exception as e:
                    logging.error(f"Erro inesperado ao abrir o arquivo JSON '{json_path}': {e}")
                    print(f"Erro inesperado ao abrir o arquivo JSON '{json_path}'.")
                    continue

                if isinstance(discursos, dict):
                    discursos = self.processar_discursos(discursos)

                for index, item in enumerate(discursos, start=1):
                    print(f"Processando discurso {index}/{len(discursos)} no arquivo {json_file}")
                    codigo, url, senador = item.get("CodigoPronunciamento"), item.get("UrlTexto"), item.get("senador")

                    # Verificar se o arquivo do discurso já foi baixado
                    texto_filename = os.path.join(base_folder, f"{codigo}.txt")
                    transcricao_filename = os.path.join(base_folder, f"{codigo}_transcricao.txt")
                    if os.path.exists(texto_filename) or os.path.exists(transcricao_filename):
                        print(f"Discurso {codigo} já processado. Pulando.")
                        item["Forma"] = "texto" if os.path.exists(texto_filename) else "vídeo"
                        item["CaminhoArquivo"] = texto_filename if os.path.exists(texto_filename) else transcricao_filename
                        item["DownloadConcluido"] = 1
                        self.salvar_json_atualizado(json_path, discursos)  # Salvar após cada iteração
                        continue

                    try:
                        logging.info(f"Acessando URL: {url}")
                        time.sleep(random.random())
                        self.driver.get(url)

                        # Tentativas para localizar o texto
                        texto = None
                        try:
                            time.sleep(random.random())
                            texto_element = self.driver.find_element(By.CLASS_NAME, 'texto-integral')
                            texto = texto_element.text
                            logging.info(f"URL corresponde a texto: {url}")
                        except:
                            try:
                                texto_integral_element = self.driver.find_element(By.ID, 'accordion-texto')
                                texto_element = texto_integral_element.find_element(By.CLASS_NAME, 'texto-integral')
                                texto = texto_element.text
                                logging.info(f"URL corresponde a texto (accordion): {url}")
                            except:
                                try:
                                    indisponivel_element = self.driver.find_element(By.CLASS_NAME, 'label-warning')
                                    if indisponivel_element.text == "Não disponível":
                                        logging.warning(f"Texto não disponível para discurso {codigo}. URL: {url}")
                                        print(f"Texto não disponível para discurso {codigo}.")
                                except:
                                    logging.warning(f"Não foi possível localizar o texto para discurso {codigo}. URL: {url}")
                                    print(f"Não foi possível localizar o texto para discurso {codigo}.")

                        if texto:
                            with open(texto_filename, 'w', encoding='utf-8') as file:
                                file.write(texto)
                            logging.info(f"Discurso {codigo} salvo com sucesso como texto em {texto_filename}")
                            print(f"Discurso {codigo} salvo como texto em {texto_filename}")
                            item["Forma"] = "texto"
                            item["CaminhoArquivo"] = texto_filename
                            item["DownloadConcluido"] = 1
                            self.salvar_json_atualizado(json_path, discursos)  # Salvar após cada iteração

                        else:
                            # Captura do vídeo
                            try:
                                time.sleep(random.random())
                                video_element = self.driver.find_element(By.CSS_SELECTOR, '#player_pronunciamento_1 > source')
                                video_url = video_element.get_attribute('src')
                                logging.info(f"URL corresponde a vídeo: {url}")
                                video_path = self.baixar_video_direto(video_url, video_folder)
                                if video_path:
                                    transcricao = self.transcrever_com_api(video_path)
                                    with open(transcricao_filename, 'w', encoding='utf-8') as file:
                                        file.write(transcricao)
                                    logging.info(f"Transcrição do vídeo {codigo} salva com sucesso em {transcricao_filename}")
                                    print(f"Transcrição do vídeo {codigo} salva em {transcricao_filename}")
                                    item["Forma"] = "vídeo"
                                    item["CaminhoArquivo"] = transcricao_filename
                                    item["DownloadConcluido"] = 1
                                    self.salvar_json_atualizado(json_path, discursos)  # Salvar após cada iteração
                            except Exception as e:
                                logging.error(f"Erro ao processar vídeo do discurso {codigo}: {e}")
                                print(f"Erro ao processar vídeo do discurso {codigo}: {e}")

                    except Exception as e:
                        logging.error(f"Erro ao carregar {url}: {e}")
                        print(f"Erro ao carregar {url}: {e}")
        finally:
            self.driver.quit()

def main():
    baixar = Baixar()
    baixar.baixar_textos(total_partes=5, parte_atual=1)
    time.sleep(300 * random.random())
    baixar.baixar_textos(total_partes=5, parte_atual=2)
    time.sleep(300 * random.random())
    baixar.baixar_textos(total_partes=5, parte_atual=3)
    time.sleep(300 * random.random())
    baixar.baixar_textos(total_partes=5, parte_atual=4)
    time.sleep(300 * random.random())
    baixar.baixar_textos(total_partes=5, parte_atual=5)
    time.sleep(300 * random.random())

if __name__ == "__main__":
    main()

