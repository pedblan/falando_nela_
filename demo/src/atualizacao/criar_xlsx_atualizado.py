import pandas as pd
import sqlite3
import os
from datetime import datetime

# Caminhos dos arquivos
caminho_banco = '../../data/DiscursosSenadores.sqlite'
caminho_xlsx_original = '../../data/dados_analisados.xlsx'
data_hoje = datetime.today().strftime('%Y-%m-%d')
caminho_xlsx_novo = f'../../data/dados_analisados_{data_hoje}.xlsx'
caminho_temporario = '../../data/senadores_temp.xlsx'

# Conexão com SQLite
conn = sqlite3.connect(caminho_banco)

# --- Atualização da planilha Discursos ---
colunas_discursos = [
    'CodigoPronunciamento', 'CodigoParlamentar', 'DataPronunciamento',
    'TextoResumo', 'Indexacao', 'SiglaPartidoParlamentarNaData',
    'UfParlamentarNaData', 'SiglaCasaPronunciamento', 'Forma', 'TextoIntegral'
]

query_discursos = f'SELECT {", ".join(colunas_discursos)} FROM Discursos'
df_discursos = pd.read_sql_query(query_discursos, conn)

# --- Atualização da planilha Senadores ---
colunas_senadores = [
    'CodigoParlamentar', 'NomeParlamentar', 'SexoParlamentar',
    'SiglaPartidoParlamentar', 'UfParlamentar', 'IndicadorAtividadePrincipal'
]
df_senadores = pd.read_sql_query(f'SELECT {", ".join(colunas_senadores)} FROM Senadores', conn)
df_senadores.to_excel(caminho_temporario, index=False)

tabelas_relacionadas = [
    ('SenadoresCargos', ['CodigoParlamentar', 'NomeCargo', 'DataInicio', 'DataFim', 'Orgao']),
    ('SenadoresHistoricoAcademico', ['CodigoParlamentar', 'NomeCurso'])
]

for tabela, colunas in tabelas_relacionadas:
    print(f"Processando tabela: {tabela}")

    df_senadores_temp = pd.read_excel(caminho_temporario)
    df_aux = pd.read_sql_query(f'SELECT {", ".join(colunas)} FROM {tabela}', conn)

    df_merge = df_senadores_temp.merge(df_aux, on='CodigoParlamentar', how='left')

    del df_aux, df_senadores_temp
    df_merge.to_excel(caminho_temporario, index=False)
    del df_merge

# Escrever resultados finais no NOVO arquivo Excel
with pd.ExcelWriter(caminho_xlsx_novo, engine='xlsxwriter') as writer:
    # Planilhas atualizadas
    df_discursos.to_excel(writer, sheet_name='Discursos', index=False)

    df_senadores_final = pd.read_excel(caminho_temporario)
    df_senadores_final.to_excel(writer, sheet_name='Senadores', index=False)
    del df_senadores_final

    # Copiar demais planilhas do arquivo original
    if os.path.exists(caminho_xlsx_original):
        for sheet_name in pd.ExcelFile(caminho_xlsx_original, engine='openpyxl').sheet_names:
            if sheet_name not in ['Discursos', 'Senadores']:
                df_extra = pd.read_excel(caminho_xlsx_original, sheet_name=sheet_name)
                df_extra.to_excel(writer, sheet_name=sheet_name, index=False)
                del df_extra

# Remover arquivo temporário
os.remove(caminho_temporario)

# Fechar conexão com o banco
del df_discursos
conn.close()

print(f'Novo arquivo {caminho_xlsx_novo} criado e atualizado com sucesso.')
