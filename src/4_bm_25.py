import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
import re
from rank_bm25 import BM25Okapi

# Caminho para o banco original
db_path = "DiscursosSenadores_02_05_2025_analisado.sqlite"

# --- Funções BM25 ---
def tokenize(texto: str) -> list[str]:
    return re.findall(r'\b\w+\b', texto)

def calcular_bm25(df: pd.DataFrame, termo: str, nome_coluna: str) -> pd.DataFrame:
    df["tokens"] = df["TextoIntegral"].apply(tokenize)
    corpus = df["tokens"].tolist()

    if not corpus:
        print("⚠️ Nenhum texto encontrado para cálculo de BM25.")
        return df

    bm25 = BM25Okapi(corpus)
    df[nome_coluna] = bm25.get_scores([termo])
    return df

def atualizar_tabela_analise(conn: sqlite3.Connection, df: pd.DataFrame, coluna_bm25: str) -> None:
    cursor = conn.execute("PRAGMA table_info(AnaliseCorpusTodo)")
    colunas_existentes = [row[1] for row in cursor.fetchall()]
    if coluna_bm25 not in colunas_existentes:
        conn.execute(f"ALTER TABLE AnaliseCorpusTodo ADD COLUMN {coluna_bm25} REAL")

    for _, row in df.iterrows():
        codigo = int(row["CodigoPronunciamento"])
        score = float(row[coluna_bm25])
        conn.execute(f"""
        INSERT INTO AnaliseCorpusTodo (CodigoPronunciamento, {coluna_bm25})
        VALUES (?, ?)
        ON CONFLICT(CodigoPronunciamento)
        DO UPDATE SET {coluna_bm25} = excluded.{coluna_bm25}
        """, (codigo, score))

    conn.commit()

def executar_bm25():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT CodigoPronunciamento, TextoIntegral
        FROM Discursos
        WHERE TextoIntegral LIKE '%Constituição%'
    """, conn)

    coluna_bm25 = "BM25_Constituição"
    df = calcular_bm25(df, termo="Constituição", nome_coluna=coluna_bm25)
    if coluna_bm25 in df.columns:
        atualizar_tabela_analise(conn, df, coluna_bm25)
        print("✅ BM25 atualizado com sucesso.")
    else:
        print("⚠️ BM25 não calculado. Nenhuma coluna adicionada.")

    conn.close()

if __name__ == "__main__":
    executar_bm25()
    print("\n🏁 Processo concluído.")
