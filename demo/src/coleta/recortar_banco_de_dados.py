import sqlite3

# Caminho do banco de dados original e do novo banco simplificado
banco_original = '../../data/DiscursosSenadores.sqlite'
banco_reduzido = '../../data/DiscursosSenadores_reduzido.sqlite'

# Conectar ao banco original e criar o banco reduzido
conn_orig = sqlite3.connect(banco_original)
conn_reduz = sqlite3.connect(banco_reduzido)

# Cursor para operações
cursor_reduz = conn_reduz.cursor()

# Criar tabelas no banco reduzido (apenas as necessárias)
cursor_reduz.executescript('''
CREATE TABLE IF NOT EXISTS Discursos (
    CodigoPronunciamento INTEGER,
    CodigoParlamentar INTEGER,
    DataPronunciamento TEXT,
    TextoResumo TEXT,
    TipoUsoPalavra TEXT,
    UrlTexto TEXT,
    Indexacao TEXT,
    SiglaPartidoParlamentarNaData TEXT,
    UfParlamentarNaData TEXT,
    SiglaCasaPronunciamento TEXT,
    Forma TEXT,
    CaminhoArquivo TEXT,
    DownloadConcluido INTEGER,
    TextoIntegral TEXT
);

CREATE TABLE IF NOT EXISTS Senadores (
    CodigoParlamentar INTEGER,
    NomeParlamentar TEXT,
    SexoParlamentar TEXT,
    SiglaPartidoParlamentar TEXT,
    UfParlamentar TEXT,
    IndicadorAtividadePrincipal TEXT
);

CREATE TABLE IF NOT EXISTS SenadoresCargos (
    IdCargo INTEGER,
    CodigoParlamentar INTEGER,
    NomeCargo TEXT,
    DataInicio TEXT,
    DataFim TEXT,
    Orgao TEXT
);

CREATE TABLE IF NOT EXISTS SenadoresHistoricoAcademico (
    IdHistoricoAcademico INTEGER,
    CodigoParlamentar INTEGER,
    NomeCurso TEXT,
    NivelCurso TEXT,
    InstituicaoEnsino TEXT,
    AnoConclusao TEXT
);

CREATE TABLE IF NOT EXISTS SenadoresLiderancas (
    IdLideranca INTEGER,
    CodigoParlamentar INTEGER,
    UnidadeLideranca TEXT,
    DescricaoTipoLideranca TEXT,
    DataInicio TEXT,
    DataFim TEXT
);
''')

# Função para copiar dados

def copiar_dados(tabela, colunas):
    cols = ', '.join(colunas)
    dados = conn_orig.execute(f'SELECT {cols} FROM {tabela}').fetchall()
    placeholders = ', '.join(['?' for _ in colunas])
    cursor_reduz.executemany(f'INSERT INTO {tabela} ({cols}) VALUES ({placeholders})', dados)

# Copiar dados das tabelas desejadas
copiar_dados('Discursos', [
    'CodigoPronunciamento', 'CodigoParlamentar', 'DataPronunciamento',
    'TextoResumo', 'TipoUsoPalavra', 'UrlTexto', 'Indexacao',
    'SiglaPartidoParlamentarNaData', 'UfParlamentarNaData', 'SiglaCasaPronunciamento',
    'Forma', 'CaminhoArquivo', 'DownloadConcluido', 'TextoIntegral'
])

copiar_dados('Senadores', [
    'CodigoParlamentar', 'NomeParlamentar', 'SexoParlamentar',
    'SiglaPartidoParlamentar', 'UfParlamentar',
    'IndicadorAtividadePrincipal'
])

copiar_dados('SenadoresCargos', [
    'IdCargo', 'CodigoParlamentar', 'NomeCargo', 'DataInicio', 'DataFim', 'Orgao'
])

copiar_dados('SenadoresHistoricoAcademico', [
    'IdHistoricoAcademico', 'CodigoParlamentar', 'NomeCurso',
    'NivelCurso', 'InstituicaoEnsino', 'AnoConclusao'
])

copiar_dados('SenadoresLiderancas', [
    'IdLideranca', 'CodigoParlamentar', 'UnidadeLideranca',
    'DescricaoTipoLideranca', 'DataInicio', 'DataFim'
])

# Confirmar e fechar conexões
conn_reduz.commit()
conn_orig.close()
conn_reduz.close()

print(f'Banco reduzido criado com sucesso em {banco_reduzido}.')
