import sqlite3
import os
import json
import faiss
import numpy as np
from langchain_openai import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.schema import Document
from .auxiliar import logar_openai
import pandas as pd

api_key = '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'

# 🔹 Caminhos do banco de dados e diretório de embeddings
DB_PATH = "../data/DiscursosSenadores.sqlite"
EMBEDDINGS_DIR = "../data/discursos/embeddings"

# 🔹 Garantir que o diretório de embeddings exista
os.makedirs(EMBEDDINGS_DIR, exist_ok=True)

# 🔹 Configurar o modelo de embeddings
embeddings_model = OpenAIEmbeddings(model="text-embedding-3-large", openai_api_key=api_key)

# 🔹 Conectar ao banco e recuperar os dados
def carregar_dados():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    query = """SELECT CodigoPronunciamento, SumarioConstituicao, TrechosConstituicao, NovaConstituinteOuConstituicao_trecho 
               FROM DiscursosAnalises"""
    cursor.execute(query)
    rows = cursor.fetchall()

    conn.close()

    # Processar os dados
    dados = []
    for row in rows:
        codigo, sumario, trechos, nova_constituinte = row

        # Se `TrechosConstituicao` for uma string de lista, converter para lista real
        if isinstance(trechos, str) and trechos.startswith("["):
            try:
                trechos = json.loads(trechos)  # Converter de string para lista
            except json.JSONDecodeError:
                trechos = [trechos]  # Caso falhe, manter como lista unitária

        # Unir os trechos em um único texto
        trechos_unidos = " ".join(trechos) if isinstance(trechos, list) else trechos

        dados.append({
            "CodigoPronunciamento": codigo,
            "SumarioConstituicao": sumario or "",
            "TrechosConstituicao": trechos_unidos or "",
            "NovaConstituinteOuConstituicao_trecho": nova_constituinte or ""
        })

    return dados


# 🔹 Criar e salvar embeddings no FAISS
def criar_index_faiss(dados):
    indices = {}  # Dicionário para armazenar FAISS por coluna

    for coluna in ["SumarioConstituicao", "TrechosConstituicao", "NovaConstituinteOuConstituicao_trecho"]:
        textos = [d[coluna] for d in dados]
        codigos = [d["CodigoPronunciamento"] for d in dados]

        # 🔹 Criar embeddings
        embeddings = embeddings_model.embed_documents(textos)
        embeddings_np = np.array(embeddings).astype("float32")

        # 🔹 Normalizar os embeddings para garantir similaridade de cosseno
        faiss.normalize_L2(embeddings_np)

        # 🔹 Criar índice FAISS para produto interno (≈ similaridade de cosseno)
        index = faiss.IndexFlatIP(embeddings_np.shape[1])
        index.add(embeddings_np)

        # 🔹 Salvar índice FAISS e metadados
        faiss.write_index(index, os.path.join(EMBEDDINGS_DIR, f"{coluna}.faiss"))
        with open(os.path.join(EMBEDDINGS_DIR, f"{coluna}_metadata.json"), "w") as f:
            json.dump(codigos, f)

        indices[coluna] = index

    return indices

# 🔹 Função para pesquisa semântica
def buscar_no_faiss(query, coluna, palavra_chave):
    if coluna not in ["SumarioConstituicao", "TrechosConstituicao", "NovaConstituinteOuConstituicao_trecho"]:
        raise ValueError("Coluna inválida. Escolha uma das disponíveis.")

    # Caminhos dos arquivos FAISS e metadados
    index_path = os.path.join(EMBEDDINGS_DIR, f"{coluna}.faiss")
    metadata_path = os.path.join(EMBEDDINGS_DIR, f"{coluna}_metadata.json")

    if not os.path.exists(index_path) or not os.path.exists(metadata_path):
        raise FileNotFoundError(f"O índice para '{coluna}' não foi encontrado. Gere os embeddings primeiro.")

    # Carregar FAISS e metadados
    index = faiss.read_index(index_path)

    with open(metadata_path, "r") as f:
        codigos = json.load(f)

    # Gerar embedding da query e normalizar
    query_embedding = embeddings_model.embed_query(query)
    query_embedding_np = np.array(query_embedding).astype("float32").reshape(1, -1)
    faiss.normalize_L2(query_embedding_np)

    # Buscar no FAISS
    scores, indices = index.search(query_embedding_np, len(codigos))  # Busca todos os discursos

    # Criar DataFrame com CodigoPronunciamento e similaridade
    df_resultado = pd.DataFrame({
        "CodigoPronunciamento": [codigos[i] for i in indices[0] if i < len(codigos)],
        f"Similaridade_{palavra_chave}": scores[0]  # Nome dinâmico da coluna baseado na palavra-chave
    })

    return df_resultado


def salvar_banco_de_dados(resultados):
    """Salva os resultados da pesquisa semântica no banco de dados SQLite.

    A função cria a tabela `DiscursosPesquisa` caso não exista, adiciona novas colunas conforme necessário
    e insere os resultados do DataFrame no banco de dados.

    Args:
        resultados (pd.DataFrame): DataFrame contendo `CodigoPronunciamento` e colunas de similaridade.

    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 🔹 Criar a tabela se não existir
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DiscursosPesquisa (
        CodigoPronunciamento INTEGER PRIMARY KEY
    )
    """)

    # 🔹 Verificar colunas existentes na tabela
    cursor.execute("PRAGMA table_info(DiscursosPesquisa)")
    colunas_existentes = {col[1] for col in cursor.fetchall()}  # col[1] é o nome da coluna

    # 🔹 Identificar colunas faltantes no DataFrame
    colunas_faltantes = set(resultados.columns) - colunas_existentes

    # 🔹 Adicionar colunas faltantes na tabela como REAL (número decimal)
    for coluna in colunas_faltantes:
        if coluna != "CodigoPronunciamento":  # Evita tentar adicionar a chave primária
            cursor.execute(f"ALTER TABLE DiscursosPesquisa ADD COLUMN {coluna} REAL")

    conn.commit()

    # 🔹 Substituir NaN por None para evitar problemas no SQLite
    resultados = resultados.where(pd.notna(resultados), None)

    # 🔹 Inserir dados do DataFrame na tabela, evitando duplicatas
    for _, row in resultados.iterrows():
        codigo = row["CodigoPronunciamento"]
        valores = {col: row[col] for col in resultados.columns if col != "CodigoPronunciamento"}

        # Construir query dinâmica para inserção ou atualização
        colunas_str = ", ".join(valores.keys())
        valores_str = ", ".join(["?"] * len(valores))
        update_str = ", ".join([f"{col} = ?" for col in valores.keys()])

        query = f"""
        INSERT INTO DiscursosPesquisa (CodigoPronunciamento, {colunas_str})
        VALUES (?, {valores_str})
        ON CONFLICT(CodigoPronunciamento) DO UPDATE SET {update_str}
        """

        cursor.execute(query, (codigo, *valores.values(), *valores.values()))

    conn.commit()
    conn.close()
    print("✅ Resultados salvos com sucesso no banco de dados!")






# 🔹 Executar tudo
if __name__ == "__main__":
    print("🔄 Carregando dados do banco...")
    dados = carregar_dados()

    print("🔄 Criando índices FAISS...")
    criar_index_faiss(dados)

    print("✅ Processamento concluído! Você pode usar 'buscar_no_faiss(query, coluna)' para pesquisar.")

