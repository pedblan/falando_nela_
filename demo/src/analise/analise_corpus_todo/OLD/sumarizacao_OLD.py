from openai import OpenAI
import os
import json
import sqlite3
import pandas as pd
from .auxiliar import instrucao, exemplos

api_key = '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'
client = OpenAI(api_key=api_key)

def identificar_pendencias(df_filtrado, db_path):
    """
    Verifica se os discursos já foram analisados pelo LLM consultando a tabela DiscursosAnalises
    e se todas as colunas relevantes foram preenchidas.

    Args:
        df_filtrado (pd.DataFrame): DataFrame contendo discursos a serem verificados.
        db_path (str): Caminho do banco de dados SQLite.

    Returns:
        pd.DataFrame: DataFrame contendo apenas discursos ainda não completamente analisados pelo LLM.
    """
    colunas_llm = [
        "SentimentoGeral", "SentimentoConstituicao", "SumarioConstituicao",
        "TrechosConstituicao", "NovaConstituinteOuConstituicao"
    ]

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Verifica se a tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='DiscursosAnalises'")
        tabela_existe = cursor.fetchone()

        if not tabela_existe:
            return df_filtrado  # Se a tabela não existe, todos os discursos são pendentes

        # Obtém as colunas existentes na tabela
        cursor.execute("PRAGMA table_info(DiscursosAnalises)")
        colunas_existentes = {col[1] for col in cursor.fetchall()}  # col[1] é o nome da coluna

        # Se alguma das colunas do LLM não existe, considerar todos os discursos como pendentes
        if not all(col in colunas_existentes for col in colunas_llm):
            return df_filtrado

        # Buscar discursos já analisados
        query = f"SELECT CodigoPronunciamento FROM DiscursosAnalises WHERE {' OR '.join(f'{col} IS NOT NULL' for col in colunas_llm)}"
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
            CREATE TABLE IF NOT EXISTS DiscursosAnalises (
                CodigoPronunciamento INTEGER PRIMARY KEY,
                SentimentoGeral TEXT,
                SentimentoConstituicao TEXT,
                SumarioConstituicao TEXT,
                TrechosConstituicao TEXT,
                NovaConstituinteOuConstituicao_resposta TEXT,
                NovaConstituinteOuConstituicao_trecho TEXT
            )
        """)

        # Verificar colunas existentes
        cursor.execute("PRAGMA table_info(DiscursosAnalises)")
        colunas_existentes = {col[1] for col in cursor.fetchall()}  # col[1] é o nome da coluna

        # Desdobrar JSON da coluna "NovaConstituinteOuConstituicao"
        if "NovaConstituinteOuConstituicao" in df_resultados.columns:
            df_resultados = pd.concat([
                df_resultados.drop(columns=["NovaConstituinteOuConstituicao"]),
                df_resultados["NovaConstituinteOuConstituicao"].apply(pd.Series)
            ], axis=1)
            df_resultados.rename(columns={
                "resposta": "NovaConstituinteOuConstituicao_resposta",
                "trecho": "NovaConstituinteOuConstituicao_trecho"
            }, inplace=True)

        # Garantir que as novas colunas sejam criadas na tabela
        colunas_faltantes = set(df_resultados.columns) - colunas_existentes

        for coluna in colunas_faltantes:
            cursor.execute(f"ALTER TABLE DiscursosAnalises ADD COLUMN {coluna} TEXT")

        conn.commit()

        # Converter listas para strings JSON antes de salvar
        if "TrechosConstituicao" in df_resultados.columns:
            df_resultados["TrechosConstituicao"] = df_resultados["TrechosConstituicao"].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else x
            )

        # Substituir NaN por None para evitar problemas no SQLite
        df_resultados = df_resultados.where(pd.notna(df_resultados), None)

        # Verificar quais discursos já existem no banco
        df_resultados["CodigoPronunciamento"] = df_resultados["CodigoPronunciamento"].astype(int)
        codigos_existentes = pd.read_sql("SELECT CodigoPronunciamento FROM DiscursosAnalises", conn)
        codigos_existentes = codigos_existentes["CodigoPronunciamento"].astype(int).tolist()

        df_novos = df_resultados[~df_resultados["CodigoPronunciamento"].isin(codigos_existentes)]
        df_existentes = df_resultados[df_resultados["CodigoPronunciamento"].isin(codigos_existentes)]

        # Inserir novos discursos
        if not df_novos.empty:
            df_novos.to_sql("DiscursosAnalises", conn, if_exists="append", index=False, method="multi")

        # Atualizar discursos existentes
        for _, row in df_existentes.iterrows():
            update_query = f"""
                UPDATE DiscursosAnalises
                SET SentimentoGeral = ?, SentimentoConstituicao = ?, SumarioConstituicao = ?, 
                    TrechosConstituicao = ?, NovaConstituinteOuConstituicao_resposta = ?, 
                    NovaConstituinteOuConstituicao_trecho = ?
                WHERE CodigoPronunciamento = ?
            """
            cursor.execute(update_query, (
                row["SentimentoGeral"], row["SentimentoConstituicao"], row["SumarioConstituicao"],
                row["TrechosConstituicao"], row["NovaConstituinteOuConstituicao_resposta"],
                row["NovaConstituinteOuConstituicao_trecho"], row["CodigoPronunciamento"]
            ))

        conn.commit()

def analisar_texto(codigo_pronunciamento, texto):
    """
    Envia um discurso para análise do LLM e retorna os resultados estruturados.

    Args:
        codigo_pronunciamento (int): Código identificador do discurso.
        texto (str): Texto do discurso.

    Returns:
        dict: Resultado estruturado da análise.
    """
    mensagens = [
        {"role": "developer", "content": instrucao},
        {"role": "user", "content": f"Analise este discurso. CodigoPronunciamento: {codigo_pronunciamento}. TextoIntegral: {texto}"}
    ]

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=mensagens,
        temperature=0.2,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "discursos_schema",
                "schema": {
                    "type": "object",
                    "properties": {
                        "CodigoPronunciamento": {"type": "integer"},
                        "SentimentoGeral": {"type": "string", "enum": ["Positivo", "Negativo", "Neutro"]},
                        "SentimentoConstituicao": {
                            "type": ["string"],
                            "enum": ["Positivo", "Negativo", "Neutro", "Não se aplica"]
                        },
                        "SumarioConstituicao": {"type": ["string", "null"]},
                        "TrechosConstituicao": {"type": ["array", "null"], "items": {"type": "string"}},
                        "NovaConstituinteOuConstituicao": {
                            "type": "object",
                            "properties": {
                                "resposta": {"type": "string", "enum": ["Convocar Constituinte", "Emenda ou reforma",
                                                                        "Deixá-la como está", "Não tenho certeza", "Não se aplica"]},
                                "trecho": {"type": "string"}
                            },
                            "required": ["resposta"]
                        }
                    },
                    "required": ["CodigoPronunciamento", "SentimentoGeral", "NovaConstituinteOuConstituicao"]
                }
            }
        }
    )

    resultado = response.choices[0].message.content
    return resultado
