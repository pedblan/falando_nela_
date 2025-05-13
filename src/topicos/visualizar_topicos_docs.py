import pandas as pd
from pathlib import Path

colunas = [
    "AvalCombinado", "AvalConclusao", "AvalImplicacao", "AvalPredicacao", "AvalTrecho",
    "NormCombinado", "NormConclusao", "NormImplicacao", "NormPredicacao",
    "NormTrecho", "NovaConstituinteOuConstituicao_trecho", "SumarioConstituicao", "TextoResumo"
]


import pandas as pd
from pathlib import Path

def gerar_visualizacoes_topicos(
    pasta_principal: str,
    colunas: list[str]
) -> None:
    """
    Gera arquivos CSV simplificados com CodigoPronunciamento, Topico, o campo analisado e o rótulo do tópico,
    ordenados por Topico, para facilitar a inspeção no Excel/Numbers. Os arquivos gerados
    são salvos na mesma subpasta onde está o arquivo original.

    Args:
        pasta_principal (str): Caminho à pasta que contém as subpastas temáticas.
        colunas (list[str]): Lista das colunas modeladas (cada uma com subpasta e CSV correspondente).

    Returns:
        None
    """
    for coluna in colunas:
        subpasta = Path(pasta_principal) / coluna
        arquivo = subpasta / f"documentos_topicos_{coluna}.csv"
        rotulos = subpasta / f"rotulos_{coluna}.csv"

        if not arquivo.exists():
            print(f"⚠️ Arquivo não encontrado: {arquivo}")
            continue

        try:
            df = pd.read_csv(arquivo)
            colunas_desejadas = ["CodigoPronunciamento", "Topico", coluna]

            if not all(c in df.columns for c in colunas_desejadas):
                print(f"⚠️ Colunas esperadas ausentes em {arquivo.name}. Pulando.")
                continue

            df_filtrado = df[colunas_desejadas].sort_values(by="Topico")

            if rotulos.exists():
                df_rotulos = pd.read_csv(rotulos)
                if "Topic" in df_rotulos.columns and "Rotulo" in df_rotulos.columns:
                    df_rotulos = df_rotulos.rename(columns={"Topic": "Topico", "Rotulo": "Representacao"})
                    df_filtrado = df_filtrado.merge(df_rotulos, on="Topico", how="left")
                    df_filtrado = df_filtrado[["CodigoPronunciamento", "Topico", coluna, "Representacao"]]

                else:
                    print(f"⚠️ Arquivo de rótulos encontrado, mas sem colunas esperadas: {rotulos}")

            nome_saida = f"visualizacao_topicos_{coluna}.csv"
            caminho_saida = subpasta / nome_saida
            df_filtrado.to_csv(caminho_saida, index=False)

            print(f"✅ Arquivo salvo: {caminho_saida}")

        except Exception as e:
            print(f"❌ Erro ao processar {arquivo.name}: {e}")

    print("\n🏁 Visualizações geradas com sucesso.")



def main():
    gerar_visualizacoes_topicos(
        pasta_principal=".",
        colunas=colunas
    )

if __name__ == "__main__":
    main()