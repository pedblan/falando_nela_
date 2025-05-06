import sqlite3
import json
import os

# Caminhos
diretorio_json = '../../data/discursos/requisicoes'
banco_dados = '../../data/DiscursosSenadores.sqlite'

# Lista para armazenar erros
erros = []

# Contador para discursos adicionados
total_discursos_adicionados = 0

# Conexão com o banco de dados
conn = sqlite3.connect(banco_dados)
cursor = conn.cursor()

# Recuperar códigos de pronunciamentos já no banco
cursor.execute("SELECT CodigoPronunciamento FROM Discursos")
discursos_existentes = set(int(row[0]) for row in cursor.fetchall())


def processar_discurso(discurso, CodigoParlamentar):
    global total_discursos_adicionados
    codigo_pronunciamento = int(discurso['CodigoPronunciamento'])

    if codigo_pronunciamento in discursos_existentes:
        return

    try:
        cursor.execute('''
            INSERT INTO Discursos (
                CodigoPronunciamento, CodigoParlamentar, DataPronunciamento, TextoResumo,
                TipoUsoPalavra, UrlTexto, Indexacao, SiglaPartidoParlamentarNaData,
                UfParlamentarNaData, SiglaCasaPronunciamento, Forma, CaminhoArquivo, DownloadConcluido
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            codigo_pronunciamento,
            CodigoParlamentar,
            discurso['DataPronunciamento'],
            discurso.get('TextoResumo'),
            discurso.get('TipoUsoPalavra'),
            discurso.get('UrlTexto'),
            discurso.get('Indexacao'),
            discurso.get('SiglaPartidoParlamentarNaData'),
            discurso.get('UfParlamentarNaData'),
            discurso.get('SiglaCasaPronunciamento'),
            discurso.get('Forma'),
            discurso.get('CaminhoArquivo'),
            discurso.get('DownloadConcluido', 0),
        ))

        discursos_existentes.add(codigo_pronunciamento)
        total_discursos_adicionados += 1
        print(f"Discurso adicionado: CódigoPronunciamento {codigo_pronunciamento}")

    except Exception as e:
        erros.append({
            "CodigoPronunciamento": codigo_pronunciamento,
            "erro": str(e)
        })


def processar_estrutura(dados, CodigoParlamentar):
    if isinstance(dados, list):
        for discurso in dados:
            processar_discurso(discurso, CodigoParlamentar)
    elif isinstance(dados, dict):
        for value in dados.values():
            processar_estrutura(value, CodigoParlamentar)


# Iterar por todos os arquivos JSON no diretório especificado
for arquivo in os.listdir(diretorio_json):
    if arquivo.endswith('.json') and arquivo.startswith('discursos_'):
        try:
            CodigoParlamentar = int(arquivo.split('_')[1].replace('.json', ''))
            caminho_arquivo = os.path.join(diretorio_json, arquivo)

            with open(caminho_arquivo, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                processar_estrutura(dados, CodigoParlamentar)

        except Exception as e:
            erros.append({"arquivo": arquivo, "erro": str(e)})

# Salvar alterações no banco de dados e fechar conexão
conn.commit()
conn.close()

# Exibir arquivos e discursos que geraram erros
print("Erros encontrados:")
for erro in erros:
    print(erro)

# Exibir total de discursos adicionados
print(f"Total de discursos adicionados: {total_discursos_adicionados}")