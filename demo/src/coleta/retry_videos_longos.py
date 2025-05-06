import os
import sqlite3
import ffmpeg
import requests
import time
import random
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from openai import OpenAI

def consultar_banco_de_dados(db_path):
    """Consulta o banco de dados para obter discursos com erro de transcrição."""
    conexao = sqlite3.connect(db_path)
    cursor = conexao.cursor()
    cursor.execute("SELECT CodigoPronunciamento, UrlTexto FROM discursos WHERE TextoIntegral LIKE '%Erro ao transcrever%'")
    discursos = cursor.fetchall()
    conexao.close()
    print(f"{len(discursos)} discursos encontrados para processamento.")
    return discursos

def baixar_videos(driver, discursos, output_folder):
    """Baixa os vídeos e os salva com o nome correspondente ao CodigoPronunciamento."""
    os.makedirs(output_folder, exist_ok=True)
    for codigo, url in discursos:
        video_path = os.path.join(output_folder, f"{codigo}.mp4")
        if os.path.exists(video_path):
            print(f"Vídeo já existente para Código: {codigo}. Pulando download.")
            continue
        try:
            print(f"Iniciando download do vídeo para Código: {codigo}")
            driver.get(url)
            time.sleep(random.uniform(1, 2))

            video_element = driver.find_element(By.CSS_SELECTOR, '#player_pronunciamento_1 > source')
            video_url = video_element.get_attribute('src')

            if video_url:
                response = requests.get(video_url, stream=True)
                response.raise_for_status()
                with open(video_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                print(f"Vídeo baixado com sucesso: {video_path}")
            else:
                print(f"URL de vídeo não encontrada para Código: {codigo}")
        except Exception as e:
            print(f"Erro ao baixar vídeo para Código {codigo}: {e}")

def converter_para_mp3(input_folder, output_folder):
    """Converte vídeos MP4 para arquivos MP3."""
    os.makedirs(output_folder, exist_ok=True)
    for video_file in os.listdir(input_folder):
        if video_file.endswith('.mp4'):
            codigo = os.path.splitext(video_file)[0]
            video_path = os.path.join(input_folder, video_file)
            audio_path = os.path.join(output_folder, f"{codigo}.mp3")
            if os.path.exists(audio_path):
                print(f"Áudio já existente para Código: {codigo}. Pulando conversão.")
                continue
            try:
                print(f"Convertendo vídeo para áudio: {video_file}")
                ffmpeg.input(video_path).output(audio_path, format='mp3', acodec='libmp3lame').run(overwrite_output=True)
                print(f"Áudio extraído com sucesso: {audio_path}")
            except ffmpeg.Error as e:
                print(f"Erro ao extrair áudio de {video_path}: {e}")

def dividir_audio(caminho_audio, output_folder, chunk_size_mb=8):
    """Divide um arquivo de áudio em partes menores."""
    os.makedirs(output_folder, exist_ok=True)
    try:
        if not os.path.exists(caminho_audio):
            raise FileNotFoundError(f"Arquivo de áudio não encontrado: {caminho_audio}")

        titulo = os.path.splitext(os.path.basename(caminho_audio))[0]
        chunk_size_bytes = chunk_size_mb * 1024 * 1024

        probe = ffmpeg.probe(caminho_audio)
        duration = float(probe['format']['duration'])
        file_size = int(probe['format']['size'])

        num_chunks = max(1, int(file_size / chunk_size_bytes))
        chunk_duration = duration / num_chunks

        chunk_paths = []
        for i in range(num_chunks):
            chunk_file = os.path.join(output_folder, f"{titulo}_chunk_{i}.mp3")
            ffmpeg.input(caminho_audio, ss=i * chunk_duration, t=chunk_duration).output(chunk_file).run(overwrite_output=True)
            chunk_paths.append(chunk_file)

        print(f"Áudio dividido em {len(chunk_paths)} partes.")
        return chunk_paths
    except ffmpeg.Error as e:
        print(f"Erro ao dividir o áudio: {e}")
        return []

def transcrever_audio_com_openai(arquivo_audio, client):
    """Transcreve um arquivo de áudio usando a API da OpenAI."""
    try:
        with open(arquivo_audio, "rb") as audio_file:
            response = client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file
            )
            return response.text
    except Exception as e:
        print(f"Erro ao transcrever {arquivo_audio}: {e}")
        return ""

def transcrever_e_salvar_temp(input_folder, output_folder, temp_json_path, openai_client):
    """Transcreve os áudios e salva os resultados em arquivos JSON temporários."""
    os.makedirs(output_folder, exist_ok=True)
    temp_results = {}

    for audio_file in os.listdir(input_folder):
        if audio_file.endswith('.mp3'):
            codigo = os.path.splitext(audio_file)[0]
            audio_path = os.path.join(input_folder, audio_file)
            chunk_folder = os.path.join(input_folder, f"chunks_{codigo}")
            chunk_paths = dividir_audio(audio_path, chunk_folder)

            texto_completo = ""

            try:
                for chunk in chunk_paths:
                    print(f"Transcrevendo parte: {chunk}")
                    texto = transcrever_audio_com_openai(chunk, openai_client)
                    texto_completo += texto + "\n"

                output_path = os.path.join(output_folder, f"{codigo}_transcricao.txt")
                with open(output_path, "w", encoding="utf-8") as file:
                    file.write(texto_completo)

                temp_results[codigo] = {
                    "TextoIntegral": texto_completo,
                    "CaminhoArquivo": output_path
                }

                print(f"Transcrição salva em: {output_path}")
            except Exception as e:
                print(f"Erro ao transcrever ou salvar para Código {codigo}: {e}")

    with open(temp_json_path, "w", encoding="utf-8") as json_file:
        json.dump(temp_results, json_file, ensure_ascii=False, indent=4)
    print(f"Resultados temporários salvos em {temp_json_path}")

if __name__ == "__main__":
    DB_PATH = "../../data/DiscursosSenadores.sqlite"
    VIDEO_FOLDER = "../../temp/videos"
    AUDIO_FOLDER = "../../temp/audios"
    TRANSCRICAO_FOLDER = "../../data/discursos/txt"
    TEMP_JSON_PATH = "../../temp/transcricoes_temp.json"

    driver = webdriver.Firefox(service=Service('./geckodriver'), options=Options().add_argument('--headless'))
    openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    try:
        discursos = consultar_banco_de_dados(DB_PATH)
        baixar_videos(driver, discursos, VIDEO_FOLDER)
        converter_para_mp3(VIDEO_FOLDER, AUDIO_FOLDER)
        transcrever_e_salvar_temp(AUDIO_FOLDER, TRANSCRICAO_FOLDER, TEMP_JSON_PATH, openai_client)
    finally:
        driver.quit()

    print("Processamento concluído.")
