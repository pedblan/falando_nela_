import sqlite3
from pathlib import Path
import shutil
import pandas as pd
import numpy as np
import re
from rank_bm25 import BM25Okapi

# Caminhos dos bancos de dados
original = "DiscursosSenadores_02_05_2025_analisado.sqlite"
backup = original.replace(".sqlite", "_backup.sqlite")
recriado = original.replace(".sqlite", "_recriado.sqlite")

# Backup de segurança
Path(backup).unlink(missing_ok=True)
shutil.copy(original, backup)
print(f"🗂️ Backup criado em: {backup}")

# Conexão única e anexando banco original
conn_new = sqlite3.connect(recriado)
conn_new.execute(f"ATTACH DATABASE '{original}' AS antigo")
cur_new = conn_new.cursor()

# Recriar tabelas com chaves compostas corretamente
cur_new.execute("""
CREATE TABLE IF NOT EXISTS Discursos (
    CodigoPronunciamento INTEGER,
    CodigoParlamentar INTEGER,
    DataPronunciamento DATETIME,
    TextoResumo TEXT,
    TipoUsoPalavra TEXT,
    UrlTexto TEXT,
    Indexacao TEXT,
    SiglaPartidoParlamentarNaData TEXT,
    UfParlamentarNaData TEXT,
    SiglaCasaPronunciamento TEXT,
    Forma TEXT,
    CaminhoArquivo TEXT,
    DownloadConcluido BOOL,
    TextoIntegral TEXT,
    PRIMARY KEY (CodigoPronunciamento, CodigoParlamentar)
)
""")

cur_new.execute("""
CREATE TABLE IF NOT EXISTS AnaliseCorpusTodo (
    CodigoPronunciamento INTEGER,
    AvalConclusao TEXT,
    AvalImplicacao TEXT,
    AvalPredicacao TEXT,
    AvalTrecho TEXT,
    MencionaConstituicao TEXT,
    NormConclusao TEXT,
    NormImplicacao TEXT,
    NormPredicacao TEXT,
    NormTrecho TEXT,
    NovaConstituinteOuConstituicao_resposta TEXT,
    NovaConstituinteOuConstituicao_trecho TEXT,
    SentimentoConstituicao TEXT,
    SentimentoGeral TEXT,
    SumarioConstituicao TEXT,
    TopicosConstituicao TEXT,
    TrechosConstituicao TEXT,
    AvalCombinado FLOAT,
    NormCombinado FLOAT,
    PRIMARY KEY (CodigoPronunciamento)
)
""")

cur_new.execute("""
CREATE TABLE IF NOT EXISTS AnaliseAnual (
    CodigoPronunciamento INTEGER,
    Trecho_Anual TEXT,
    Topic TEXT,
    Topico_Anual TEXT,
    Representation TEXT,
    Representative_Docs TEXT,
    Top_n_words TEXT,
    Probability TEXT,
    Representative_document TEXT,
    Ano DATETIME,
    Tipo_Anual TEXT,
    BM25_anual FLOAT,
    PRIMARY KEY (CodigoPronunciamento, Tipo_Anual, Ano)
)
""")

cur_new.execute("""
CREATE TABLE IF NOT EXISTS Senadores (
    CodigoParlamentar INTEGER PRIMARY KEY,
    NomeParlamentar TEXT,
    NomeCompletoParlamentar TEXT,
    SexoParlamentar TEXT,
    SiglaPartidoParlamentar TEXT,
    UfParlamentar TEXT,
    UrlFotoParlamentar TEXT,
    EmailParlamentar TEXT,
    NomeProfissao TEXT,
    IndicadorAtividadePrincipal TEXT
)
""")

# Copiar dados usando banco anexado
cur_new.execute("INSERT INTO Discursos SELECT CodigoPronunciamento, CodigoParlamentar, DataPronunciamento, TextoResumo, TipoUsoPalavra, UrlTexto, Indexacao, SiglaPartidoParlamentarNaData, UfParlamentarNaData, SiglaCasaPronunciamento, Forma, CaminhoArquivo, DownloadConcluido, TextoIntegral FROM antigo.Discursos")
cur_new.execute("INSERT INTO AnaliseCorpusTodo SELECT CodigoPronunciamento, AvalConclusao, AvalImplicacao, AvalPredicacao, AvalTrecho, MencionaConstituicao, NormConclusao, NormImplicacao, NormPredicacao, NormTrecho, NovaConstituinteOuConstituicao_resposta, NovaConstituinteOuConstituicao_trecho, SentimentoConstituicao, SentimentoGeral, SumarioConstituicao, TopicosConstituicao, TrechosConstituicao, AvalCombinado, NormCombinado FROM antigo.AnaliseCorpusTodo")
cur_new.execute("INSERT INTO AnaliseAnual SELECT CodigoPronunciamento, Trecho_Anual, Topic, Topico_Anual, Representation, Representative_Docs, Top_n_words, Probability, Representative_document, Ano, Tipo_Anual, BM25_anual FROM antigo.AnaliseAnual")
cur_new.execute("INSERT INTO Senadores SELECT CodigoParlamentar, NomeParlamentar, NomeCompletoParlamentar, SexoParlamentar, SiglaPartidoParlamentar, UfParlamentar, UrlFotoParlamentar, EmailParlamentar, NomeProfissao, IndicadorAtividadePrincipal FROM antigo.Senadores")

conn_new.commit()
conn_new.close()

print(f"✅ Banco recriado com chaves em: {recriado}")
