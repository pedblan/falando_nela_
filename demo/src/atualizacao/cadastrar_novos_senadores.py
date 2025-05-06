import json
import sqlite3
import os

# Configuração dos caminhos
JSON_PATH = '../../data/senadores/senadores_legislatura_57.json'
SQLITE_DB = '../../data/DiscursosSenadores.sqlite'

# Carregar senadores do JSON
with open(JSON_PATH, 'r', encoding='utf-8') as file:
    senadores_json = json.load(file)

novos_senadores = []

# Conectar ao banco de dados SQLite
conn = sqlite3.connect(SQLITE_DB)
cursor = conn.cursor()

# Buscar senadores já cadastrados no banco
cursor.execute("SELECT CodigoParlamentar FROM Senadores")
senadores_banco = set(row[0] for row in cursor.fetchall())

# Conferir e adicionar novos senadores
for senador in senadores_json:
    identificacao = senador['IdentificacaoParlamentar']
    codigo_parlamentar = int(identificacao['CodigoParlamentar'])

    if codigo_parlamentar not in senadores_banco:
        cursor.execute("""
            INSERT INTO Senadores (
                CodigoParlamentar, NomeParlamentar, NomeCompletoParlamentar,
                SexoParlamentar, SiglaPartidoParlamentar, UfParlamentar,
                UrlFotoParlamentar, EmailParlamentar, NomeProfissao,
                IndicadorAtividadePrincipal
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            codigo_parlamentar,
            identificacao.get('NomeParlamentar', ''),
            identificacao.get('NomeCompletoParlamentar', ''),
            identificacao.get('SexoParlamentar', ''),
            identificacao.get('SiglaPartidoParlamentar', ''),
            identificacao.get('UfParlamentar', ''),
            identificacao.get('UrlFotoParlamentar', ''),
            identificacao.get('EmailParlamentar', ''),
            identificacao.get('NomeProfissao', ''),
            identificacao.get('IndicadorAtividadePrincipal', '')
        ))

        novos_senadores.append(identificacao['NomeParlamentar'])

# Commit das mudanças e fechamento da conexão
conn.commit()
conn.close()

# Exibir resultados
if novos_senadores:
    print("Novos senadores adicionados ao banco:")
    for senador in novos_senadores:
        print(f"- {senador}")
else:
    print("Nenhum senador novo foi adicionado.")

