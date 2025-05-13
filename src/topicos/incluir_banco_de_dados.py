import sqlite3
import pandas as pd
from pathlib import Path

def inserir_tabelas_topicos_em_lote(
    pasta_principal: str,
    colunas: list[str],
    caminho_banco: str
) -> None:
    """
    Itera sobre subpastas com arquivos topicos_{coluna}.csv e insere cada um deles
    como uma nova tabela Topicos{coluna} no banco de dados SQLite.

    Args:
        pasta_principal (str): Caminho à pasta que contém as subpastas temáticas.
        colunas (list[str]): Lista de colunas que representam os nomes das subpastas.
        caminho_banco (str): Caminho ao banco de dados SQLite.

    Returns:
        None
    """
    conn = sqlite3.connect(caminho_banco)

    for coluna in colunas:
        subpasta = Path(pasta_principal) / coluna
        arquivo_csv = subpasta / f"documentos_topicos_{coluna}.csv"

        if not arquivo_csv.exists():
            print(f"⚠️ Arquivo não encontrado: {arquivo_csv}")
            continue

        try:
            df = pd.read_csv(arquivo_csv)
            # manter apenas as colunas essenciais
            colunas_desejadas = ["CodigoPronunciamento", "Topico", "Probs"]
            df = df[[col for col in colunas_desejadas if col in df.columns]]


            nome_tabela = f"Topicos{coluna}"
            df.to_sql(nome_tabela, conn, if_exists="replace", index=False)

            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{nome_tabela}_CodigoPronunciamento ON {nome_tabela}(CodigoPronunciamento);")

            print(f"✅ Tabela criada: {nome_tabela} com {len(df)} linhas.")

        except Exception as e:
            print(f"❌ Erro ao processar {arquivo_csv.name}: {e}")

    conn.close()
    print("\n🏁 Inserção finalizada.")


colunas = [
    "AvalCombinado", "AvalConclusao", "AvalImplicacao", "AvalPredicacao", "AvalTrecho",
    "Indexacao", "NormCombinado", "NormConclusao", "NormImplicacao", "NormPredicacao",
    "NormTrecho", "NovaConstituinteOuConstituicao_trecho", "SumarioConstituicao", "TextoResumo"
]

inserir_tabelas_topicos_em_lote(
    pasta_principal=".",  # ex: "./resultados_modelagem"
    colunas=colunas,
    caminho_banco="../DiscursosSenadores_02_05_2025_analisado.sqlite"
)
