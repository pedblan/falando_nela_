import sqlite3
import pandas as pd
import numpy as np
import re
from rank_bm25 import BM25Okapi

def conectar_banco(caminho_banco: str) -> sqlite3.Connection:
    """
    Conecta ao banco de dados SQLite.

    Args:
        caminho_banco (str): Caminho para o arquivo .sqlite.

    Returns:
        sqlite3.Connection: Objeto de conexão com o banco.
    """
    return sqlite3.connect(caminho_banco)


def obter_discursos_com_constituicao(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Executa uma query para buscar discursos que contenham a palavra "Constituição".

    Args:
        conn (sqlite3.Connection): Conexão ativa com o banco de dados.

    Returns:
        pd.DataFrame: DataFrame com colunas 'CodigoPronunciamento' e 'texto'.
    """
    query = """
    SELECT CodigoPronunciamento, TextoIntegral
    FROM Discursos
    WHERE TextoIntegral LIKE '%Constituição%'
    """
    return pd.read_sql_query(query, conn)


def tokenize(texto: str) -> list[str]:
    """
    Tokeniza um texto em palavras, preservando acentos.

    Args:
        texto (str): Texto de entrada.

    Returns:
        list[str]: Lista de tokens extraídos do texto.
    """
    return re.findall(r'\b\w+\b', texto)


def calcular_bm25(df: pd.DataFrame, termo: str) -> pd.DataFrame:
    """
    Calcula a pontuação BM25 para cada texto do DataFrame com base em um termo de consulta.

    Args:
        df (pd.DataFrame): DataFrame com colunas 'CodigoPronunciamento' e 'texto'.
        termo (str): Palavra a ser usada como termo de busca no BM25.

    Returns:
        pd.DataFrame: Mesmo DataFrame, com uma nova coluna 'BM25_<termo>'.
    """
    df["tokens"] = df["texto"].apply(tokenize)
    corpus = df["tokens"].tolist()
    bm25 = BM25Okapi(corpus)

    query = [termo]
    scores = bm25.get_scores(query)
    df[f"BM25_{termo}"] = scores
    return df


def atualizar_tabela_analise(conn: sqlite3.Connection, df: pd.DataFrame, coluna_bm25: str) -> None:
    """
    Atualiza ou insere a coluna BM25 na tabela Analise do banco de dados.

    Args:
        conn (sqlite3.Connection): Conexão ativa com o banco.
        df (pd.DataFrame): DataFrame com colunas 'CodigoPronunciamento' e a coluna BM25.
        coluna_bm25 (str): Nome da coluna com os valores BM25 a serem inseridos.

    Returns:
        None
    """
    # Cria a tabela Analise se ela não existir
    conn.execute("""
    CREATE TABLE IF NOT EXISTS Analise (
        CodigoPronunciamento INTEGER PRIMARY KEY
    )
    """)

    # Verifica se a coluna BM25_Constituicao existe
    cursor = conn.execute("PRAGMA table_info(Analise)")
    colunas_existentes = [row[1] for row in cursor.fetchall()]
    if coluna_bm25 not in colunas_existentes:
        conn.execute(f"ALTER TABLE Analise ADD COLUMN {coluna_bm25} REAL")

    # Atualiza ou insere os valores no banco
    for _, row in df.iterrows():
        codigo = int(row["CodigoPronunciamento"])
        score = float(row[coluna_bm25])
        conn.execute(f"""
        INSERT INTO Analise (CodigoPronunciamento, {coluna_bm25})
        VALUES (?, ?)
        ON CONFLICT(CodigoPronunciamento)
        DO UPDATE SET {coluna_bm25}=excluded.{coluna_bm25}
        """, (codigo, score))

    conn.commit()



def main():
    """
    Executa o pipeline de cálculo BM25 para o termo "Constituição" e atualiza o banco de dados.

    Notas:
        - A função busca somente os discursos que contenham a palavra "Constituição" com C maiúsculo e acento.
        - A tokenização preserva acentos e pontuações não são incluídas nos tokens.
        - A tabela 'Analise' é criada ou atualizada conforme necessário.
        - Se um 'CodigoPronunciamento' já existir na tabela, o valor de BM25 será atualizado.
    """
    db_path = "DiscursosSenadores_02_05_2025_analisado.sqlite"
    conn = conectar_banco(db_path)

    df = obter_discursos_com_constituicao(conn)
    df = calcular_bm25(df, termo="Constituição")
    atualizar_tabela_analise(conn, df, coluna_bm25="BM25_Constituição")

    conn.close()


if __name__ == "__main__":
    main()
