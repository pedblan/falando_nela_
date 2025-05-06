import pandas as pd


def identificar():
    # Caminho do arquivo Excel
    arquivo_excel = "../../../data/dados_analisados_2025-05-03.xlsx"

    # Carrega as planilhas em DataFrames separados
    df_discursos = pd.read_excel(arquivo_excel, sheet_name="Discursos")
    df_analise_corpus = pd.read_excel(arquivo_excel, sheet_name="PesquisaSemantica")

    # Obtém conjuntos de CodigoPronunciamento únicos das duas planilhas
    codigos_discursos = set(df_discursos["CodigoPronunciamento"].dropna().unique())
    codigos_analise_corpus = set(df_analise_corpus["CodigoPronunciamento"].dropna().unique())

    # Calcula as pendências: códigos presentes em Discursos, mas ausentes em AnaliseCorpusTodo
    pendencias = codigos_discursos - codigos_analise_corpus

    # Exibe resultados
    print(f"Quantidade de pendências: {len(pendencias)}")
    return pendencias
