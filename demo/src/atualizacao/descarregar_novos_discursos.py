import sqlite3
import os
import time
import random
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
import requests
import stat

# Caminhos
caminho_geckodriver = "../coleta/geckodriver"
banco_dados = '../../data/DiscursosSenadores.sqlite'

# Permissão de execução ao GeckoDriver
os.chmod(caminho_geckodriver, os.stat(caminho_geckodriver).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

# Configuração do log
log_file_path = os.path.join(os.getcwd(), '../../data/discursos/logs/download_textos_log.log')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Baixar:
    def __init__(self, geckodriver_path=caminho_geckodriver):
        firefox_options = Options()
        firefox_options.add_argument('--headless')
        service = Service(geckodriver_path)
        self.driver = webdriver.Firefox(service=service, options=firefox_options)

    def baixar_texto_discurso(self, codigo, url, texto_filename):
        try:
            self.driver.get(url)
            time.sleep(random.uniform(1, 3))

            texto_element = self.driver.find_element(By.CLASS_NAME, 'texto-integral')
            texto = texto_element.text

            with open(texto_filename, 'w', encoding='utf-8') as file:
                file.write(texto)

            logging.info(f"Discurso {codigo} salvo como texto.")
            print(f"Discurso {codigo} baixado com sucesso.")
            return True

        except Exception as e:
            logging.error(f"Erro ao baixar texto do discurso {codigo}: {e}")
            print(f"Erro ao baixar discurso {codigo}: {e}")
            return False

    def processar_discursos_banco(self):
        conn = sqlite3.connect(banco_dados)
        cursor = conn.cursor()

        cursor.execute("SELECT CodigoPronunciamento, UrlTexto FROM Discursos WHERE DownloadConcluido = 0")
        discursos = cursor.fetchall()

        base_folder = '../../data/discursos/txt'
        os.makedirs(base_folder, exist_ok=True)

        for codigo, url in discursos:
            print(f"Processando discurso {codigo}")
            texto_filename = os.path.join(base_folder, f"{codigo}.txt")

            sucesso = self.baixar_texto_discurso(codigo, url, texto_filename)

            if sucesso:
                cursor.execute("UPDATE Discursos SET DownloadConcluido = 1, CaminhoArquivo = ?, Forma = 'texto' WHERE CodigoPronunciamento = ?",
                               (texto_filename, codigo))
                conn.commit()

        conn.close()
        self.driver.quit()


def main():
    baixar = Baixar()
    baixar.processar_discursos_banco()

if __name__ == "__main__":
    main()
