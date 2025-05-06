import pandas as pd


def identificar(arquivo_excel):

    # Carrega as planilhas em DataFrames separados
    df_discursos = pd.read_excel(arquivo_excel, sheet_name="Discursos")
    df_analise_corpus = pd.read_excel(arquivo_excel, sheet_name="AnaliseCorpusTodo")

    # Obtém conjuntos de CodigoPronunciamento únicos das duas planilhas
    codigos_discursos = set(df_discursos["CodigoPronunciamento"].dropna().unique())
    codigos_analise_corpus = set(df_analise_corpus["CodigoPronunciamento"].dropna().unique())

    # Calcula as pendências: códigos presentes em Discursos, mas ausentes em AnaliseCorpusTodo
    pendencias = codigos_discursos - codigos_analise_corpus

    # Exibe resultados
    print(f"Quantidade de pendências: {len(pendencias)}")
    return pendencias
