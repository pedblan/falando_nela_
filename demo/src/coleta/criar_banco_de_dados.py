import sqlite3

BANCO_DE_DADOS ='../../data/DiscursosSenadores.sqlite'


def criar_banco_dados(BANCO_DE_DADOS):
    # Conecta ao banco de dados (ou cria um novo arquivo .sqlite)
    conexao = sqlite3.connect(BANCO_DE_DADOS)
    conexao.close()
    print(f"Banco de dados '{BANCO_DE_DADOS}' criado com sucesso!")


def criar_tabela_senadores(BANCO_DE_DADOS):
    conexao = sqlite3.connect(BANCO_DE_DADOS)
    cursor = conexao.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Senadores (
            CodigoParlamentar INTEGER PRIMARY KEY,
            NomeParlamentar TEXT NOT NULL,
            NomeCompletoParlamentar TEXT,
            SexoParlamentar TEXT,
            SiglaPartidoParlamentar TEXT,
            UfParlamentar TEXT,
            UrlFotoParlamentar TEXT,
            EmailParlamentar TEXT,
            NomeProfissao TEXT,
            IndicadorAtividadePrincipal TEXT
        );
    ''')
    conexao.commit()
    conexao.close()
    print("Tabela 'Senadores' criada com sucesso!")


def criar_tabela_senadores_liderancas(BANCO_DE_DADOS):
    conexao = sqlite3.connect(BANCO_DE_DADOS)
    cursor = conexao.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS SenadoresLiderancas (
            IdLideranca INTEGER PRIMARY KEY AUTOINCREMENT,
            CodigoParlamentar INTEGER,
            DescricaoLideranca TEXT,
            DataInicio DATE,
            DataFim DATE,
            FOREIGN KEY (CodigoParlamentar) REFERENCES Senadores (CodigoParlamentar)
        );
    ''')
    conexao.commit()
    conexao.close()
    print("Tabela 'SenadoresLiderancas' criada com sucesso!")


def criar_tabela_senadores_cargos(BANCO_DE_DADOS):
    conexao = sqlite3.connect(BANCO_DE_DADOS)
    cursor = conexao.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS SenadoresCargos (
            IdCargo INTEGER PRIMARY KEY AUTOINCREMENT,
            CodigoParlamentar INTEGER,
            NomeCargo TEXT,
            DataInicio DATE,
            DataFim DATE,
            FOREIGN KEY (CodigoParlamentar) REFERENCES Senadores (CodigoParlamentar)
        );
    ''')
    conexao.commit()
    conexao.close()
    print("Tabela 'SenadoresCargos' criada com sucesso!")


def criar_tabela_senadores_comissoes(BANCO_DE_DADOS):
    conexao = sqlite3.connect(BANCO_DE_DADOS)
    cursor = conexao.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS SenadoresComissoes (
            IdComissao INTEGER PRIMARY KEY AUTOINCREMENT,
            CodigoParlamentar INTEGER,
            NomeComissao TEXT,
            CargoComissao TEXT,
            DataInicio DATE,
            DataFim DATE,
            FOREIGN KEY (CodigoParlamentar) REFERENCES Senadores (CodigoParlamentar)
        );
    ''')
    conexao.commit()
    conexao.close()
    print("Tabela 'SenadoresComissoes' criada com sucesso!")


def criar_tabela_senadores_filiacoes(BANCO_DE_DADOS):
    conexao = sqlite3.connect(BANCO_DE_DADOS)
    cursor = conexao.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS SenadoresFiliacoes (
            IdFiliacao INTEGER PRIMARY KEY AUTOINCREMENT,
            CodigoParlamentar INTEGER,
            SiglaPartido TEXT,
            DataInicio DATE,
            DataFim DATE,
            FOREIGN KEY (CodigoParlamentar) REFERENCES Senadores (CodigoParlamentar)
        );
    ''')
    conexao.commit()
    conexao.close()
    print("Tabela 'SenadoresFiliacoes' criada com sucesso!")


def criar_tabela_senadores_historico_academico(BANCO_DE_DADOS):
    conexao = sqlite3.connect(BANCO_DE_DADOS)
    cursor = conexao.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS SenadoresHistoricoAcademico (
            IdHistoricoAcademico INTEGER PRIMARY KEY AUTOINCREMENT,
            CodigoParlamentar INTEGER,
            NomeCurso TEXT,
            NivelCurso TEXT,
            InstituicaoEnsino TEXT,
            AnoConclusao TEXT,
            FOREIGN KEY (CodigoParlamentar) REFERENCES Senadores (CodigoParlamentar)
        );
    ''')
    conexao.commit()
    conexao.close()
    print("Tabela 'SenadoresHistoricoAcademico' criada com sucesso!")

def criar_tabelas_discursos(BANCO_DE_DADOS):

    # Conexão com o banco de dados
    conn = sqlite3.connect(BANCO_DE_DADOS)
    cursor = conn.cursor()

    # Criar as tabelas principais
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Discursos (
        CodigoPronunciamento INTEGER PRIMARY KEY,
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
        DownloadConcluido INTEGER,
        FOREIGN KEY (CodigoParlamentar) REFERENCES Senadores(CodigoParlamentar)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DiscursosAparteantes (
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        CodigoPronunciamento INTEGER,
        CodigoParlamentar INTEGER,
        NomeAparteante TEXT,
        FOREIGN KEY (CodigoPronunciamento) REFERENCES Discursos(CodigoPronunciamento),
        FOREIGN KEY (CodigoParlamentar) REFERENCES Senadores(CodigoParlamentar)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DiscursosPublicacoes (
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        CodigoPronunciamento INTEGER,
        DescricaoVeiculoPublicacao TEXT,
        DataPublicacao DATE,
        UrlDiario TEXT,
        FOREIGN KEY (CodigoPronunciamento) REFERENCES Discursos(CodigoPronunciamento)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS DiscursosSessoes (
        CodigoSessao INTEGER PRIMARY KEY,
        CodigoPronunciamento INTEGER,
        NomeCasaSessao TEXT,
        DataSessao DATE,
        HoraInicioSessao TIME,
        FOREIGN KEY (CodigoPronunciamento) REFERENCES Discursos(CodigoPronunciamento)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Senadores (
        CodigoParlamentar INTEGER PRIMARY KEY,
        NomeParlamentar TEXT,
        Partido TEXT,
        UF TEXT
    )
    ''')

    # Salvar alterações e fechar a conexão
    conn.commit()
    conn.close()

    print(f"Banco de dados criado ou atualizado em: {BANCO_DE_DADOS}")


# Executa as funções para criar o banco de dados e tabelas
if __name__ == "__main__":
    criar_banco_dados(BANCO_DE_DADOS)
    criar_tabela_senadores(BANCO_DE_DADOS)
    criar_tabela_senadores_liderancas(BANCO_DE_DADOS)
    criar_tabela_senadores_cargos(BANCO_DE_DADOS)
    criar_tabela_senadores_comissoes(BANCO_DE_DADOS)
    criar_tabela_senadores_filiacoes(BANCO_DE_DADOS)
    criar_tabela_senadores_historico_academico(BANCO_DE_DADOS)
    criar_tabelas_discursos(BANCO_DE_DADOS)
