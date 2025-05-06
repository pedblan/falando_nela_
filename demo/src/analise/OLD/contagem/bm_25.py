import sqlite3
import pandas as pd
import numpy as np
import re
from rank_bm25 import BM25Okapi


def preprocess_text(text: str) -> list:
    """Normaliza o texto: minúsculas e remove caracteres especiais."""
    if not isinstance(text, str):
        return []
    text = text.lower().strip()  # Converte para minúsculas e remove espaços extras
    text = re.sub(r"[^\w\s]", "", text)  # Remove pontuação
    return text.split()  # Tokeniza o texto


def calcular_bm25_e_atualizar_sql(conn: sqlite3.Connection, tabela_texto: str, tabela_analises: str, coluna_texto: str,
                                  palavra_chave: str) -> None:
    """
    Calcula o BM25 de uma palavra-chave para cada texto da coluna especificada
    e armazena os valores em uma nova tabela do banco de dados SQLite.

    Args:
        conn (sqlite3.Connection): Conexão com o banco SQLite.
        tabela_texto (str): Nome da tabela contendo os textos originais.
        tabela_analises (str): Nome da tabela onde os resultados serão armazenados.
        coluna_texto (str): Nome da coluna contendo os textos.
        palavra_chave (str): Palavra-chave a ser avaliada.
    """
    cursor = conn.cursor()

    # Verifica se a coluna existe na tabela
    cursor.execute(f"PRAGMA table_info({tabela_texto})")
    colunas_existentes = [coluna[1] for coluna in cursor.fetchall()]
    if coluna_texto not in colunas_existentes:
        raise ValueError(f"A coluna '{coluna_texto}' não existe na tabela '{tabela_texto}'.")

    # Criar a tabela DiscursosAnalises se não existir
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {tabela_analises} (
            CodigoPronunciamento INTEGER PRIMARY KEY,
            BM25_{palavra_chave} REAL,
            FOREIGN KEY (CodigoPronunciamento) REFERENCES {tabela_texto} (CodigoPronunciamento)
        )
    """)
    conn.commit()

    # Verifica se a coluna BM25_{palavra_chave} existe, caso contrário, adiciona
    cursor.execute(f"PRAGMA table_info({tabela_analises})")
    colunas_analises = [coluna[1] for coluna in cursor.fetchall()]
    nome_coluna_bm25 = f"BM25_{palavra_chave}"

    if nome_coluna_bm25 not in colunas_analises:
        cursor.execute(f"ALTER TABLE {tabela_analises} ADD COLUMN {nome_coluna_bm25} REAL")
        conn.commit()

    # Obter os textos e os IDs dos discursos
    df = pd.read_sql_query(f"SELECT CodigoPronunciamento, {coluna_texto} FROM {tabela_texto}", conn)

    if df.empty:
        return

    # Tokenizar os textos usando a função de normalização
    df[coluna_texto] = df[coluna_texto].fillna("")  # Substitui None ou NaN por string vazia
    textos_tokenizados = df[coluna_texto].apply(preprocess_text).tolist()

    # Inicializar BM25 com os textos tokenizados
    bm25 = BM25Okapi(textos_tokenizados)

    # Normalizar e tokenizar a palavra-chave
    palavra_tokenizada = preprocess_text(palavra_chave)

    # **Nova lógica: Se a palavra não está no texto, BM25 é 0 antes do cálculo**
    bm25_scores = []
    for texto in textos_tokenizados:
        if any(p in texto for p in palavra_tokenizada):
            bm25_scores.append(bm25.get_scores(palavra_tokenizada)[textos_tokenizados.index(texto)])
        else:
            bm25_scores.append(0.0)  # **Força BM25 = 0**

    # Inserir ou atualizar valores na tabela DiscursosAnalises
    for i, codigo in enumerate(df["CodigoPronunciamento"]):
        cursor.execute(f"""
            INSERT INTO {tabela_analises} (CodigoPronunciamento, {nome_coluna_bm25})
            VALUES (?, ?)
            ON CONFLICT(CodigoPronunciamento) DO UPDATE SET {nome_coluna_bm25} = excluded.{nome_coluna_bm25}
        """, (codigo, float(bm25_scores[i])))  # Converte explicitamente para float

    conn.commit()
