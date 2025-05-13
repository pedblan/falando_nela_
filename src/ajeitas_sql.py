import sqlite3
from pathlib import Path

# Caminho para o banco original
db_original = "DiscursosSenadores_02_05_2025_analisado.sqlite"
db_novo = db_original.replace(".sqlite", "_com_chave.sqlite")

# Remove versão anterior do novo banco, se existir
Path(db_novo).unlink(missing_ok=True)

# Conecta ao banco original e ao novo
conn_old = sqlite3.connect(db_original)
conn_new = sqlite3.connect(db_novo)

cur_old = conn_old.cursor()
cur_new = conn_new.cursor()

# --- Criar tabelas manualmente com chaves primárias compostas ---

cur_new.execute("""
CREATE TABLE IF NOT EXISTS "Discursos" (
    CodigoPronunciamento INTEGER,
    CodigoParlamentar INTEGER,
    DataPronunciamento DATE,
    TextoResumo TEXT,
    TipoUsoPalavra TEXT,
    UrlTexto TEXT,
    Indexacao TEXT,
    SiglaPartidoParlamentarNaData TEXT,
    UfParlamentarNaData TEXT,
    SiglaCasaPronunciamento TEXT,
    Forma TEXT,
    CaminhoArquivo TEXT,
    DownloadConcluido BOOLEAN,
    TextoIntegral TEXT,
    PRIMARY KEY (CodigoPronunciamento, CodigoParlamentar)
)
""")

cur_new.execute("""
CREATE TABLE IF NOT EXISTS "Senadores" (
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

cur_new.execute("""
CREATE TABLE IF NOT EXISTS "Analise" (
    CodigoPronunciamento INTEGER,
    CodigoParlamentar INTEGER,
    AvalConclusao TEXT,
    AvalImplicacao TEXT,
    AvalPredicacao TEXT,
    AvalTrecho TEXT,
    MencionaConstituicao BOOLEAN,
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
    AvalCombinado REAL,
    NormCombinado REAL,
    BM25_Constituição REAL,
    PRIMARY KEY (CodigoPronunciamento, CodigoParlamentar)
)
""")

# --- Copiar os dados diretamente ---
cur_new.execute("INSERT INTO Discursos SELECT * FROM main.Discursos")
cur_new.execute("INSERT INTO Analise SELECT * FROM main.Analise")
cur_new.execute("INSERT INTO Senadores SELECT * FROM main.Senadores")

# Finaliza
conn_new.commit()
conn_old.close()
conn_new.close()

print(f"\n✅ Banco recriado com chaves em: {db_novo}")
