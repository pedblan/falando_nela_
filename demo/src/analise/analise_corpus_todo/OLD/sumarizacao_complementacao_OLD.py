from openai import OpenAI
import os
import json
import sqlite3
import pandas as pd
from .auxiliar import instrucao, exemplos

api_key = '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'
client = OpenAI(api_key=api_key)

import sqlite3
import pandas as pd

def identificar_pendencias(df_filtrado, db_path):
    """
    Verifica se os discursos já foram analisados pelo LLM consultando a tabela DiscursosNarrativas
    e se todas as colunas relevantes foram preenchidas.

    Args:
        df_filtrado (pd.DataFrame): DataFrame contendo discursos a serem verificados.
        db_path (str): Caminho do banco de dados SQLite.

    Returns:
        pd.DataFrame: DataFrame contendo apenas discursos ainda não completamente analisados pelo LLM.
    """
    colunas_llm = [
        "MencionaConstituicao",
        "NormPredicacao", "NormImplicacao", "NormConclusao", "NormTrecho",
        "AvalPredicacao", "AvalImplicacao", "AvalConclusao", "AvalTrecho"
    ]

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Verifica se a tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='DiscursosNarrativas'")
        tabela_existe = cursor.fetchone()

        if not tabela_existe:
            return df_filtrado  # Se a tabela não existe, todos os discursos são pendentes

        # Obtém as colunas existentes na tabela
        cursor.execute("PRAGMA table_info(DiscursosNarrativas)")
        colunas_existentes = {col[1] for col in cursor.fetchall()}  # col[1] é o nome da coluna

        # Se alguma das colunas do LLM não existe, considerar todos os discursos como pendentes
        if not all(col in colunas_existentes for col in colunas_llm):
            return df_filtrado

        # Buscar discursos já analisados (todos os campos relevantes devem estar preenchidos)
        query = f"""
            SELECT CodigoPronunciamento FROM DiscursosNarrativas 
            WHERE {' AND '.join(f'{col} IS NOT NULL AND {col} != ""' for col in colunas_llm)}
        """
        df_analisados = pd.read_sql(query, conn)

    if df_analisados.empty:
        return df_filtrado  # Se a tabela estiver vazia, todos os discursos são pendentes

    discursos_completos = df_analisados["CodigoPronunciamento"].astype(int).tolist()
    df_pendentes = df_filtrado[~df_filtrado["CodigoPronunciamento"].astype(int).isin(discursos_completos)]

    return df_pendentes


def salvar_no_banco(df_resultados, db_path):
    """
    Salva os resultados da análise no banco de dados SQLite sem sobrescrever colunas existentes.
    Se as colunas não existirem, elas serão criadas automaticamente.
    Se um discurso já existir no banco, ele será atualizado em vez de inserido novamente.

    Args:
        df_resultados (pd.DataFrame): DataFrame contendo os resultados da análise.
        db_path (str): Caminho do banco de dados SQLite.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Criar a tabela se não existir
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DiscursosNarrativas (
                CodigoPronunciamento INTEGER PRIMARY KEY,
                MencionaConstituicao BOOLEAN,
                NormPredicacao TEXT,
                NormImplicacao TEXT,
                NormConclusao TEXT,
                NormTrecho TEXT,
                AvalPredicacao TEXT,
                AvalImplicacao TEXT,
                AvalConclusao TEXT,
                AvalTrecho TEXT
            )
        """)

        # Verificar colunas existentes
        cursor.execute("PRAGMA table_info(DiscursosNarrativas)")
        colunas_existentes = {col[1] for col in cursor.fetchall()}  # col[1] é o nome da coluna

        # Garantir que todas as colunas do LLM existam na tabela
        colunas_llm = [
            "MencionaConstituicao",
            "NormPredicacao", "NormImplicacao", "NormConclusao", "NormTrecho",
            "AvalPredicacao", "AvalImplicacao", "AvalConclusao", "AvalTrecho"
        ]
        colunas_faltantes = set(colunas_llm) - colunas_existentes

        for coluna in colunas_faltantes:
            cursor.execute(f"ALTER TABLE DiscursosNarrativas ADD COLUMN {coluna} TEXT")

        conn.commit()

        # Substituir NaN por None para evitar problemas no SQLite
        df_resultados = df_resultados.where(pd.notna(df_resultados), None)

        # Verificar quais discursos já existem no banco
        df_resultados["CodigoPronunciamento"] = df_resultados["CodigoPronunciamento"].astype(int)
        codigos_existentes = pd.read_sql("SELECT CodigoPronunciamento FROM DiscursosNarrativas", conn)
        codigos_existentes = codigos_existentes["CodigoPronunciamento"].astype(int).tolist()

        df_novos = df_resultados[~df_resultados["CodigoPronunciamento"].isin(codigos_existentes)]
        df_existentes = df_resultados[df_resultados["CodigoPronunciamento"].isin(codigos_existentes)]

        # Inserir novos discursos
        if not df_novos.empty:
            df_novos.to_sql("DiscursosNarrativas", conn, if_exists="append", index=False, method="multi")

        # Atualizar discursos existentes
        for _, row in df_existentes.iterrows():
            update_query = f"""
                UPDATE DiscursosNarrativas
                SET MencionaConstituicao = ?, 
                    NormPredicacao = ?, NormImplicacao = ?, NormConclusao = ?, NormTrecho = ?, 
                    AvalPredicacao = ?, AvalImplicacao = ?, AvalConclusao = ?, AvalTrecho = ?
                WHERE CodigoPronunciamento = ?
            """
            cursor.execute(update_query, (
                row["MencionaConstituicao"],
                row["NormPredicacao"], row["NormImplicacao"], row["NormConclusao"], row["NormTrecho"],
                row["AvalPredicacao"], row["AvalImplicacao"], row["AvalConclusao"], row["AvalTrecho"],
                row["CodigoPronunciamento"]
            ))

        conn.commit()
