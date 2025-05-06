import sqlite3
import pandas as pd
import faiss
import json
import numpy as np
from bertopic import BERTopic


# 📌 Função para carregar dados do banco SQLite
def carregar_dados(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT da.CodigoPronunciamento, 
           da.SumarioConstituicao, 
           d.DataPronunciamento
    FROM DiscursosAnalises AS da
    JOIN Discursos AS d 
    ON da.CodigoPronunciamento = d.CodigoPronunciamento;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    return df


# 📌 Função para carregar embeddings FAISS e metadados
def carregar_embeddings(caminho_faiss, caminho_metadata):
    # Carregar o índice FAISS
    index = faiss.read_index(caminho_faiss)

    # Carregar metadados
    with open(caminho_metadata, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    # Se metadata for uma lista, converter para dicionário
    if isinstance(metadata, list):
        if index.ntotal == len(metadata):
            metadata = {str(i): metadata[i] for i in range(len(metadata))}
        else:
            raise ValueError("Número de embeddings não corresponde ao número de entradas no FAISS!")

    return index, metadata



# 📌 Função para modelar tópicos dinamicamente
def modelar_topicos(df, embeddings):
    modelo = BERTopic(language="portuguese", calculate_probabilities=True)

    # Converter DataPronunciamento para datetime e criar a coluna Ano
    df["DataPronunciamento"] = pd.to_datetime(df["DataPronunciamento"], errors="coerce")
    df["Ano"] = df["DataPronunciamento"].dt.year

    # Garantir que SumarioConstituicao seja string e remover valores vazios
    df["SumarioConstituicao"] = df["SumarioConstituicao"].fillna("").astype(str)
    df = df[df["SumarioConstituicao"].apply(lambda x: isinstance(x, str) and len(x.strip()) > 0)]

    # Aplicar BERTopic usando os embeddings carregados
    topicos, probs = modelo.fit_transform(df["SumarioConstituicao"].tolist(), embeddings=embeddings)

    # Adicionar os tópicos ao DataFrame
    df["Topico"] = topicos

    return modelo, df
