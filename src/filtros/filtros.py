import sqlite3
import pandas as pd

def substituir_expressoes(
        conn: sqlite3.Connection,
        tabela: str,
        coluna: str,
        substituicoes: dict[str, str]
) -> None:
    """
    Substitui expressoes dentro dos textos do banco de dados SQLite, garantindo padronizacao.

    Args:
        conn (sqlite3.Connection): Conexao ativa com o banco de dados.
        tabela (str): Nome da tabela onde os textos estao armazenados.
        coluna (str): Nome da coluna onde serao feitas as substituicoes.
        substituicoes (dict[str, str]): Dicionario com as substituicoes (chave -> valor).

    Returns:
        None: A funcao faz as substituicoes diretamente no banco de dados.
    """
    cursor = conn.cursor()

    for original, substituto in substituicoes.items():
        query = f"""
        UPDATE {tabela}
        SET {coluna} = REPLACE({coluna}, ?, ?)
        """
        cursor.execute(query, (original, substituto))

    conn.commit()


def filtrar(
        conn: sqlite3.Connection,
        tabela: str,
        coluna: str,
        palavras_chave: list[str]
) -> pd.DataFrame:
    """
    Cria um DataFrame com os discursos filtrados com base em palavras-chave, normalizando a pesquisa para lowercase.

    Args:
        conn (sqlite3.Connection): Conexao ativa com o banco de dados.
        tabela (str): Nome da tabela onde os textos estao armazenados.
        coluna (str): Nome da coluna onde o filtro vai atuar.
        palavras_chave (list[str]): Lista de palavras-chave que devem estar presentes.

    Returns:
        pd.DataFrame: DataFrame contendo apenas os discursos que incluem pelo menos uma palavra-chave.
    """
    if not palavras_chave:
        raise ValueError("A lista de palavras-chave não pode estar vazia.")

    # Normaliza a busca aplicando LOWER() na query
    filtros_inclusao = " OR ".join([f"LOWER({coluna}) LIKE ?" for _ in palavras_chave])

    query = f"SELECT * FROM {tabela} WHERE {filtros_inclusao}"

    # Criar os parâmetros para o LIKE (convertendo palavras-chave para lowercase)
    parametros = [f"%{palavra.lower()}%" for palavra in palavras_chave]

    # Executar a query e converter para DataFrame
    df = pd.read_sql_query(query, conn, params=parametros)

    return df