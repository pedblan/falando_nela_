import sqlite3
import os

# Caminhos
pasta_textos = '../../data/discursos/txt'
banco_dados = '../../data/DiscursosSenadores.sqlite'

# Conectar ao banco de dados
conn = sqlite3.connect(banco_dados)
cursor = conn.cursor()

# Iterar pelos arquivos de texto na pasta
for nome_arquivo in os.listdir(pasta_textos):
    if nome_arquivo.endswith('.txt'):
        if '_' in nome_arquivo:
            codigo_pronunciamento = int(nome_arquivo.split('_')[0])
        else:
            codigo_pronunciamento = int(nome_arquivo.replace('.txt', ''))

        caminho_arquivo = os.path.join(pasta_textos, nome_arquivo)

        try:
            cursor.execute("SELECT DownloadConcluido FROM Discursos WHERE CodigoPronunciamento = ?", (codigo_pronunciamento,))
            resultado = cursor.fetchone()


            with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
                texto_integral = arquivo.read()

            cursor.execute("""
                UPDATE Discursos
                SET TextoIntegral = ?
                WHERE CodigoPronunciamento = ?
            """, (texto_integral, codigo_pronunciamento))

            print(f"Texto integral do discurso {codigo_pronunciamento} inserido com sucesso.")

        except Exception as e:
            print(f"Erro ao inserir texto do discurso {codigo_pronunciamento}: {e}")

# Salvar alterações e fechar conexão
conn.commit()
conn.close()

print("Processo concluído.")
