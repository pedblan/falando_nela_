import sqlite3
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer


def calcular_tfidf_e_atualizar_sql(
        conn: sqlite3.Connection,
        tabela_texto: str,
        tabela_analises: str,
        coluna_texto: str,
        palavra_chave: str
) -> None:
    """
    Calcula o TF-IDF de uma palavra-chave para cada texto da coluna especificada
    e armazena os valores em uma nova tabela do banco de dados SQLite.

    Args:
        conn (sqlite3.Connection): Conexão com o banco SQLite.
        tabela_texto (str): Nome da tabela contendo os textos originais.
        tabela_analises (str): Nome da tabela onde os resultados serão armazenados.
        coluna_texto (str): Nome da coluna contendo os textos.
        palavra_chave (str): Palavra-chave a ser avaliada.
    """
    cursor = conn.cursor()

    # Verifica se a coluna existe na tabela de textos
    cursor.execute(f"PRAGMA table_info({tabela_texto})")
    colunas_existentes = [coluna[1] for coluna in cursor.fetchall()]
    if coluna_texto not in colunas_existentes:
        raise ValueError(f"A coluna '{coluna_texto}' não existe na tabela '{tabela_texto}'.")

    # Criar a tabela DiscursosAnalises se não existir
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {tabela_analises} (
            CodigoPronunciamento INTEGER PRIMARY KEY,
            TFIDF_{palavra_chave} REAL,
            FOREIGN KEY (CodigoPronunciamento) REFERENCES {tabela_texto} (CodigoPronunciamento)
        )
    """)
    conn.commit()

    # Verifica se a coluna TFIDF_{palavra_chave} existe, caso contrário, adiciona
    cursor.execute(f"PRAGMA table_info({tabela_analises})")
    colunas_analises = [coluna[1] for coluna in cursor.fetchall()]
    nome_coluna_tfidf = f"TFIDF_{palavra_chave}"

    if nome_coluna_tfidf not in colunas_analises:
        cursor.execute(f"ALTER TABLE {tabela_analises} ADD COLUMN {nome_coluna_tfidf} REAL")
        conn.commit()

    # Obter os textos e os IDs dos discursos
    df = pd.read_sql_query(f"SELECT CodigoPronunciamento, {coluna_texto} FROM {tabela_texto}", conn)

    if df.empty:
        return

    # Tokenização e cálculo do TF-IDF
    vectorizer = TfidfVectorizer(lowercase=True, stop_words=None)
    df[coluna_texto] = df[coluna_texto].fillna("")  # Substitui None ou NaN por string vazia
    tfidf_matrix = vectorizer.fit_transform(df[coluna_texto])

    # Obtém os índices das palavras no vocabulário
    vocab = vectorizer.vocabulary_

    # Se a palavra-chave não estiver no vocabulário, define TF-IDF como 0
    if palavra_chave.lower() not in vocab:
        tfidf_scores = np.zeros(len(df))
    else:
        idx_palavra = vocab[palavra_chave.lower()]
        tfidf_scores = tfidf_matrix[:, idx_palavra].toarray().flatten()

    # Inserir ou atualizar valores na tabela DiscursosAnalises
    for i, codigo in enumerate(df["CodigoPronunciamento"]):
        cursor.execute(f"""
            INSERT INTO {tabela_analises} (CodigoPronunciamento, {nome_coluna_tfidf})
            VALUES (?, ?)
            ON CONFLICT(CodigoPronunciamento) DO UPDATE SET {nome_coluna_tfidf} = excluded.{nome_coluna_tfidf}
        """, (codigo, float(tfidf_scores[i])))  # Converte explicitamente para float

    conn.commit()
