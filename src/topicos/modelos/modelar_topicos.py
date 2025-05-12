import sqlite3
import numpy as np
import pandas as pd
import faiss
from sklearn.feature_extraction.text import CountVectorizer
from umap import UMAP
from hdbscan import HDBSCAN
from bertopic.representation import PartOfSpeech
from bertopic.representation import MaximalMarginalRelevance
from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
import os
import openai
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
from bertopic.representation import OpenAI as bertopic_OpenAI
from typing import Tuple, List, Optional, Dict, Any, Union



def carregar_dados() -> pd.DataFrame:
    """
    Carrega os dados dos discursos do Senado a partir do banco de dados SQLite.

    A função realiza uma junção entre as tabelas AnaliseCorpusTodo, Discursos e Senadores,
    filtrando apenas os discursos da Casa Legislativa 'SF' (Senado Federal).

    Returns:
        pd.DataFrame: Um DataFrame contendo os discursos e metadados associados.
    """
    caminho_banco = "../../DiscursosSenadores_02_05_2025_analisado.sqlite"
    conexao = sqlite3.connect(caminho_banco)

    consulta = """
    SELECT
        a.*,
        d.TextoResumo,
        d.Indexacao,
        d.DataPronunciamento,
        d.SiglaCasaPronunciamento,
        s.NomeParlamentar,
        s.CodigoParlamentar,
        d.SiglaPartidoParlamentarNaData,
        d.UfParlamentarNaData
    FROM AnaliseCorpusTodo AS a
    LEFT JOIN Discursos AS d ON a.CodigoPronunciamento = d.CodigoPronunciamento
    LEFT JOIN Senadores AS s ON d.CodigoParlamentar = s.CodigoParlamentar
    WHERE d.SiglaCasaPronunciamento = 'SF'
    """

    df_discursos = pd.read_sql_query(consulta, conexao)
    conexao.close()

    return df_discursos


def carregar_embeddings(coluna: str) -> Tuple[np.ndarray, np.ndarray]:
    """
    Carrega os embeddings e os códigos de documentos a partir de arquivos FAISS e NumPy.

    Os arquivos são carregados com base no nome da coluna especificada, que define o sufixo
    dos arquivos de embedding.

    Args:
        coluna (str): Nome da coluna usada para identificar os arquivos de embedding.

    Returns:
        Tuple[np.ndarray, np.ndarray]: Um par contendo os códigos dos documentos (IDs) e os embeddings correspondentes.
    """
    caminho_index = f"../../../data/discursos/embeddings/discursos_{coluna}.index"
    caminho_codigos = f"../../../data/discursos/embeddings/codigos_{coluna}.npy"

    codigos = np.load(caminho_codigos)
    faiss_index = faiss.read_index(caminho_index)
    embeddings = faiss_index.reconstruct_n(0, faiss_index.ntotal)

    return codigos, embeddings

def verificar_embeddings(
    codigos: np.ndarray,
    embeddings: np.ndarray,
    df_discursos: pd.DataFrame
) -> Tuple[pd.DataFrame, np.ndarray]:
    """
    Alinha os embeddings com os dados do DataFrame de discursos com base no CódigoPronunciamento.

    Cria um DataFrame auxiliar com os códigos dos embeddings e seus índices, e realiza a junção
    com o DataFrame dos discursos. Em seguida, seleciona apenas os embeddings válidos e alinhados.

    Args:
        codigos (np.ndarray): Array com os códigos dos pronunciamentos (IDs dos documentos).
        embeddings (np.ndarray): Matriz de embeddings correspondente aos códigos.
        df_discursos (pd.DataFrame): DataFrame contendo os discursos e metadados.

    Returns:
        Tuple[pd.DataFrame, np.ndarray]:
            - DataFrame com os discursos válidos e alinhados com os embeddings.
            - Matriz de embeddings alinhada com o DataFrame.
    """
    print("Shape dos embeddings:", embeddings.shape)
    print("Quantidade de códigos:", codigos.shape)

    df_embeddings = pd.DataFrame({
        "CodigoPronunciamento": codigos,
        "embedding_index": np.arange(len(codigos))
    })

    df_final_valido = pd.merge(
        df_discursos,
        df_embeddings,
        on="CodigoPronunciamento",
        how="inner"
    )

    embeddings_validos = embeddings[df_final_valido["embedding_index"].values]

    print(f"Nº de documentos válidos: {len(df_final_valido)}")
    print(f"Shape dos embeddings válidos: {embeddings_validos.shape}")

    return df_final_valido, embeddings_validos


def formatar_datas(df: pd.DataFrame, anos: List[int]) -> pd.DataFrame:
    """
    Converte a coluna 'DataPronunciamento' para formato datetime e filtra os anos desejados.

    A função também reporta quantas datas não puderam ser convertidas.
    Se o parâmetro anos não for ['todos'], a função filtra os discursos pelos anos informados.

    Args:
        df (pd.DataFrame): DataFrame contendo a coluna 'DataPronunciamento'.
        anos (List[int]): Lista de anos a manter no DataFrame ou ['todos'] para não filtrar.

    Returns:
        pd.DataFrame: DataFrame com datas tratadas e possivelmente filtrado por ano.
    """
    df["DataPronunciamento"] = pd.to_datetime(df["DataPronunciamento"], errors="coerce")
    problemas = df["DataPronunciamento"].isnull().sum()
    print(f"Problemas de datas: {problemas}")

    if anos and anos[0] != 'todos':
        df = df[df["DataPronunciamento"].dt.year.isin(anos)]

    return df

def salvar_dataset_embeddings(
    df: pd.DataFrame,
    embeddings: np.ndarray,
    coluna: str
) -> None:
    """
    Salva o DataFrame com os discursos alinhados e os embeddings correspondentes em disco.

    O DataFrame é salvo no formato Parquet, e os embeddings são salvos como arquivo .npy.

    Args:
        df (pd.DataFrame): DataFrame com os discursos e metadados já alinhados aos embeddings.
        embeddings (np.ndarray): Matriz de embeddings correspondente.
        coluna (str): Nome da coluna usada como sufixo nos nomes dos arquivos salvos.

    Returns:
        None
    """
    caminho_parquet = f"./dataset_preparado_{coluna}.parquet"
    caminho_npy = f"./embeddings_validos_{coluna}.npy"

    df.to_parquet(caminho_parquet, index=False)
    np.save(caminho_npy, embeddings)

    print(f"Arquivos salvos: {caminho_parquet} e {caminho_npy}")


def preparar_docs_bertopic(df: pd.DataFrame, coluna: str) -> List[str]:
    """
    Extrai e valida os documentos de uma coluna específica do DataFrame para uso no BERTopic.

    Substitui entradas inválidas ou vazias por um marcador neutro ('texto_vazio').

    Args:
        df (pd.DataFrame): DataFrame contendo a coluna de texto a ser analisada.
        coluna (str): Nome da coluna que contém os textos.

    Returns:
        List[str]: Lista de textos validados e prontos para modelagem.
    """
    docs = df[coluna].tolist()
    print(f"Total de documentos extraídos: {len(docs)}")

    docs_validados = [
        doc if isinstance(doc, str) and doc.strip() else "texto_vazio"
        for doc in docs
    ]

    return docs_validados


def treinar_vetorizador() -> CountVectorizer:
    """
    Cria e retorna um modelo de vetorização com CountVectorizer adaptado para textos em português.

    O modelo utiliza n-gramas de 1 a 4 palavras e uma lista personalizada de stopwords
    para eliminar ruídos comuns em discursos parlamentares.

    Returns:
        CountVectorizer: Instância configurada do vetorizador.
    """
    stopwords_extras = [
        "texto_vazio", "textovazio", "nenhuma", "nenhum", "null", "não aplicável",
        "resposta não informada", "Por causa disso, o orador", "Por", "causa", "disso",
        "o", "os", "as", "orador", "de", "que", "para", "do", "em", "ao", "dos", "das",
        "da", "seu", "sua", "por", "no", "na", "um", "uma", "pelo", "pela", "aos", "às",
        "isso significa que o", "isso significa que a", "é", "são",
        "constituição de", "constituição do", "constituição da",
        "suas", "seus", "com", "como", "ser", "se", "isso", "Isso", "Eu", "eu",
        "sobre", "conforme previsto", "conforme estabelecido"
    ]

    vectorizer_model = CountVectorizer(
        stop_words=stopwords_extras,
        strip_accents=None,
        lowercase=False,
        token_pattern=r'(?u)\b[A-Za-zÀ-ÖØ-öø-ÿ]{2,}\b',
        ngram_range=(1, 4),
        min_df=10
    )

    return vectorizer_model


def reduzir_dim_embeddings(embeddings: np.ndarray) -> np.ndarray:
    """
    Reduz a dimensionalidade dos embeddings utilizando UMAP.

    A redução é feita para 100 dimensões com métrica de cosseno, mantendo vizinhanças
    locais. Isso é útil para clustering com HDBSCAN e visualizações posteriores.

    Args:
        embeddings (np.ndarray): Matriz de embeddings original (alta dimensionalidade).

    Returns:
        np.ndarray: Matriz de embeddings com dimensionalidade reduzida.
    """
    umap_model = UMAP(
        n_neighbors=15,
        n_components=100,
        min_dist=0.0,
        metric="cosine",
        random_state=42
    )

    reduced_embeddings = umap_model.fit_transform(embeddings)
    return umap_model, reduced_embeddings


def clusterizar(docs: List[str]) -> HDBSCAN:
    """
    Cria um modelo HDBSCAN para clusterização dos documentos com base na quantidade total.

    O tamanho mínimo do cluster é definido como 0,25% do total de documentos,
    com métrica euclidiana e método de seleção de cluster 'eom'.

    Args:
        docs (List[str]): Lista de documentos validados.

    Returns:
        HDBSCAN: Modelo configurado para clusterização hierárquica densa.
    """
    total_documentos = len(docs)
    percentual = 0.0025
    min_cluster_size = int(total_documentos * percentual)

    hdbscan_model = HDBSCAN(
        min_cluster_size=min_cluster_size,
        metric="euclidean",
        cluster_selection_method="eom",
        prediction_data=True
    )

    return hdbscan_model


def definir_prompt(coluna: str) -> Optional[str]:
    """
    Define o prompt personalizado para geração de representações de tópicos via LLM,
    de acordo com a coluna analisada.

    Cada prompt é adaptado ao tipo de raciocínio constitucional extraído:
    predicação normativa, conclusão ou ambos combinados.

    Args:
        coluna (str): Nome da coluna utilizada na análise (ex: 'NormPredicacao', 'NormConclusao').

    Returns:
        Optional[str]: Texto do prompt correspondente ao tipo de coluna, ou None se não reconhecida.
    """

    if coluna == 'NormPredicacao':
        return """
Estou fazendo um estudo sistemático de discursos feitos por senadores no plenário do Senado Federal.
Resolvi modelar os tópicos do meu banco de dados.
Este tópico diz respeito aos aspectos normativos da Constituição que são citados nos discursos dos senadores.
O tópico contém os seguintes documentos:
[DOCUMENTS]
O tópico é descrito pelas seguintes palavras-chave: [KEYWORDS]

Com base nisso, extraia um rótulo curto para o tópico no seguinte formato:
topic: <rótulo do tópico>
Baseie o rótulo nos aspectos normativos citados, e não na interpretação deles. 
Exemplos: 'Princípios e objetivos fundamentais', 'Separação de poderes', 'Direitos sociais'.
"""

    elif coluna == 'NormImplicacao':
        return """
Estou fazendo um estudo sistemático de discursos feitos por senadores no plenário do Senado Federal.
Resolvi modelar os tópicos do meu banco de dados.
Este tópico diz respeito às implicação dos aspectos normativos da Constituição citados nos discursos dos senadores.
Em outras palavras, trata-se da etapa entre a predicação e a conclusão – o que os oradores pensam a respeito dos aspectos normativos.
O tópico contém os seguintes documentos:
[DOCUMENTS]
O tópico é descrito pelas seguintes palavras-chave: [KEYWORDS]

Com base nisso, extraia um rótulo curto para o tópico no seguinte formato:
topic: <rótulo do tópico>
Exemplos: 'Valorização profissional', 'Defesa do meio ambiente'.
"""

    elif coluna == 'NormConclusao':
        return """
Estou fazendo um estudo sistemático de discursos feitos por senadores no plenário do Senado Federal.
Resolvi modelar os tópicos do meu banco de dados.
Este tópico diz respeito às conclusões dos raciocínios normativos baseados na Constituição que são citados nos discursos.
O tópico contém os seguintes documentos:
[DOCUMENTS]
O tópico é descrito pelas seguintes palavras-chave: [KEYWORDS]

Com base nisso, extraia um rótulo curto para o tópico no seguinte formato:
topic: <rótulo do tópico>
Exemplos: 'Pelo impeachment de presidente da República', 'Contra impeachment de ministro do STF'.
"""

    elif coluna == 'NormCombinado':
        return """
Estou fazendo um estudo sistemático de discursos feitos por senadores no plenário do Senado Federal.
Resolvi modelar os tópicos do meu banco de dados.
Este tópico diz respeito aos raciocínios normativos baseados na Constituição que são citados nos discursos dos senadores.
O tópico contém os seguintes documentos:
[DOCUMENTS]
O tópico é descrito pelas seguintes palavras-chave: [KEYWORDS]

Com base nisso, extraia um rótulo curto para o tópico no seguinte formato:
topic: <rótulo do tópico>
"""

    return None


def representacao(prompt: str) -> Tuple[Dict[str, object], SentenceTransformer]:
    """
    Cria o dicionário de modelos de representação de tópicos para o BERTopic,
    incluindo uma representação principal via OpenAI GPT-4o e uma representação gramatical baseada em POS.

    Args:
        prompt (str): Prompt customizado a ser enviado à API da OpenAI para nomear os tópicos.

    Returns:
        Tuple[Dict[str, object], SentenceTransformer]:
            - Dicionário com representações ('Main', 'Aspect1')
            - Modelo de embeddings de sentenças a ser usado no BERTopic
    """
    openai_model = bertopic_OpenAI(
        client=client,
        model="gpt-4o",
        prompt=prompt,
        nr_docs=10,
        delay_in_seconds=3
    )

    main_representation = openai_model

    pos_model = PartOfSpeech("pt_core_news_lg")
    pos_model.pos = ["NOUN", "PROPN", "ADJ"]

    representation_model = {
        "Main": main_representation,
        "Aspect1": pos_model
    }

    embedding_model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

    return representation_model, embedding_model

def modelar(
    vectorizer_model: CountVectorizer,
    umap_model: UMAP,
    hdbscan_model: HDBSCAN,
    representation_model: Dict[str, Any],
    embedding_model: SentenceTransformer,
    docs_validados: List[str],
    reduced_embeddings: np.ndarray
) -> Tuple[BERTopic, List[int], np.ndarray]:
    """
    Cria e treina um modelo BERTopic completo com base em documentos e embeddings fornecidos.

    Integra os componentes: vetorizador, redução de dimensionalidade (UMAP), clusterização (HDBSCAN),
    representação via LLM (OpenAI) e POS tagging, além do modelo de embedding semântico.

    Args:
        vectorizer_model (CountVectorizer): Vetorizador de texto.
        umap_model (UMAP): Modelo UMAP configurado para redução de dimensionalidade.
        hdbscan_model (HDBSCAN): Modelo de clusterização HDBSCAN.
        representation_model (Dict[str, Any]): Dicionário com modelos de representação semântica.
        embedding_model (SentenceTransformer): Modelo de embedding semântico.
        docs_validados (List[str]): Lista de documentos validados.
        reduced_embeddings (np.ndarray): Embeddings reduzidos com UMAP.

    Returns:
        Tuple[BERTopic, List[int], np.ndarray]:
            - Modelo BERTopic treinado
            - Lista de tópicos atribuídos
            - Distribuição de probabilidades por tópico
    """
    topic_model = BERTopic(
        vectorizer_model=vectorizer_model,
        language="multilingual",
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        representation_model=representation_model,
        embedding_model=embedding_model
    )

    topics, probs = topic_model.fit_transform(docs_validados, reduced_embeddings)

    return topic_model, topics, probs


def grafico_topicos(topic_model: BERTopic) -> None:
    """
    Gera e exibe a visualização interativa dos tópicos modelados pelo BERTopic.

    Args:
        topic_model (BERTopic): Modelo de tópicos treinado.

    Returns:
        None
    """
    fig = topic_model.visualize_topics()
    fig.show()


def lista_topicos(topic_model: BERTopic) -> None:
    """
    Exibe a tabela de informações dos tópicos gerados pelo modelo BERTopic.

    Inclui número do tópico, quantidade de documentos e palavras representativas.

    Args:
        topic_model (BERTopic): Modelo de tópicos treinado.

    Returns:
        None
    """
    print(topic_model.get_topic_info())


def reduzir_topicos(
    topic_model: BERTopic,
    docs: list[str],
    nr_topics: Union[int, str] = "auto"
) -> None:
    """
    Reduz o número de tópicos do modelo BERTopic com base em agrupamento semântico.

    Essa redução pode ser automática ("auto") ou definida manualmente (com um número inteiro).
    A função informa a quantidade de tópicos antes e depois da operação.

    Args:
        topic_model (BERTopic): Modelo BERTopic já treinado.
        docs (list[str]): Lista de documentos originais.
        nr_topics (Union[int, str], optional): Número alvo de tópicos ou "auto". Default é "auto".

    Returns:
        None
    """
    qtd_antes = len(topic_model.get_topic_info())
    topic_model.reduce_topics(docs, nr_topics)
    qtd_depois = len(topic_model.get_topic_info())

    print(f"Tópicos reduzidos de {qtd_antes} para {qtd_depois}.")


def salvar_modelo(topic_model: BERTopic, coluna: str, reduzido: bool = False) -> None:
    """
    Salva o modelo BERTopic treinado em disco com um nome de arquivo baseado na coluna analisada.

    Se a versão for reduzida, um sufixo '_reduzido' é adicionado ao nome do arquivo.

    Args:
        topic_model (BERTopic): Instância do modelo BERTopic treinado.
        coluna (str): Nome da coluna usada na análise (identifica o arquivo).
        reduzido (bool, optional): Define se o modelo salvo é a versão reduzida. Default é False.

    Returns:
        None
    """
    sufixo = "_reduzido" if reduzido else ""
    caminho = f"baseline_model_{coluna}{sufixo}"
    topic_model.save(caminho)
    print(f"Modelo salvo em: {caminho}")


def visualizar_historico(topic_model: BERTopic, df: pd.DataFrame, coluna: str) -> None:
    """
    Gera e exibe a visualização da evolução temporal dos tópicos ao longo dos anos.

    A função extrai o ano de cada discurso e usa a lista de tópicos gerada pelo modelo
    para construir o gráfico interativo.

    Args:
        topic_model (BERTopic): Modelo de tópicos já treinado.
        df (pd.DataFrame): DataFrame contendo os discursos com a coluna 'DataPronunciamento'.
        coluna (str): Nome da coluna textual usada como base para os documentos.

    Returns:
        None
    """
    timestamps = df["DataPronunciamento"].dt.year.tolist()
    topics = topic_model.topics_

    topics_over_time = topic_model.topics_over_time(
        docs=df[coluna].astype(str).tolist(),
        topics=topics,
        timestamps=timestamps,
        global_tuning=True
    )

    fig = topic_model.visualize_topics_over_time(topics_over_time)
    fig.show()


def salvar_resultados(
    topic_model: BERTopic,
    df: pd.DataFrame,
    docs: List[str],
    probs: np.ndarray,
    coluna: str,
    reduzido: bool = False
) -> None:
    """
    Salva os principais resultados da modelagem de tópicos, incluindo:
    - Lista de tópicos (get_topic_info)
    - Distribuição de tópicos por documento
    - Evolução temporal dos tópicos

    Args:
        topic_model (BERTopic): Modelo BERTopic treinado.
        df (pd.DataFrame): DataFrame com os discursos válidos.
        docs (List[str]): Lista de documentos utilizados na modelagem.
        probs (np.ndarray): Distribuição de probabilidade por tópico.
        coluna (str): Nome da coluna analisada.
        reduzido (bool, optional): Indica se os resultados são da versão reduzida. Default é False.

    Returns:
        None
    """
    sufixo = "_reduzido" if reduzido else ""

    # 1. Salvar info dos tópicos
    df_topicos = topic_model.get_topic_info()
    caminho_topicos = f"topicos_{coluna}{sufixo}.csv"
    df_topicos.to_csv(caminho_topicos, index=False, encoding="utf-8")

    # 2. Salvar tópicos por documento
    topicos = topic_model.topics_
    df_docs = df.copy()
    df_docs["Topico"] = topicos
    df_docs["Probs"] = probs.tolist() if probs is not None else None
    caminho_docs = f"documentos_topicos_{coluna}{sufixo}.csv"
    df_docs.to_csv(caminho_docs, index=False, encoding="utf-8")

    # 3. Salvar evolução temporal
    timestamps = df["DataPronunciamento"].dt.year.tolist()
    topics_over_time = topic_model.topics_over_time(
        docs=docs,
        topics=topicos,
        timestamps=timestamps,
        global_tuning=True
    )
    df_tempo = pd.DataFrame(topics_over_time)
    caminho_tempo = f"topicos_{coluna}_tempo{sufixo}.csv"
    df_tempo.to_csv(caminho_tempo, index=False, encoding="utf-8")

    print(f"\n📁 Resultados salvos com sucesso:")
    print(f"→ {caminho_topicos}")
    print(f"→ {caminho_docs}")
    print(f"→ {caminho_tempo}")


def main() -> None:
    """
    Executa o pipeline completo de modelagem de tópicos com BERTopic.
    Inclui carregamento, processamento, modelagem, redução, visualização e exportação.
    """
    print("\n📥 Início do pipeline BERTopic\n")

    coluna = input("Digite a coluna de embeddings a ser analisada: ").strip()
    entrada_anos = input("Digite os anos a serem pesquisados, separados por vírgula, ou 'todos': ").strip()

    if entrada_anos.lower() == "todos":
        anos = ['todos']
    else:
        anos = [int(ano.strip()) for ano in entrada_anos.split(",") if ano.strip().isdigit()]

    print("\n[1/5] Carregando dados e embeddings...")
    df_discursos = carregar_dados()
    codigos, embeddings = carregar_embeddings(coluna)
    df_valido, embeddings_validos = verificar_embeddings(codigos, embeddings, df_discursos)
    df_valido = formatar_datas(df_valido, anos)
    salvar_dataset_embeddings(df_valido, embeddings_validos, coluna)

    print("\n[2/5] Preparando documentos, vetorizador e UMAP...")
    docs_validados = preparar_docs_bertopic(df_valido, coluna)
    vectorizer_model = treinar_vetorizador()
    umap_model, reduced_embeddings = reduzir_dim_embeddings(embeddings_validos)
    hdbscan_model = clusterizar(docs_validados)
    prompt = definir_prompt(coluna)
    representation_model, embedding_model = representacao(prompt)

    print("\n[3/5] Modelando tópicos com BERTopic + GPT-4o...")
    topic_model, topics, probs = modelar(
        vectorizer_model,
        umap_model,
        hdbscan_model,
        representation_model,
        embedding_model,
        docs_validados,
        reduced_embeddings
    )

    print("\n[4/5] Visualização antes da redução de tópicos:")
    grafico_topicos(topic_model)
    lista_topicos(topic_model)
    salvar_resultados(topic_model, df_valido, docs_validados, probs, coluna)
    visualizar_historico(topic_model, df_valido, coluna)

    print("\n[5/5] Redução e visualização final:")
    reduzir_topicos(topic_model, docs_validados)
    grafico_topicos(topic_model)
    lista_topicos(topic_model)
    salvar_resultados(topic_model, df_valido, docs_validados, probs, coluna, reduzido=True)
    visualizar_historico(topic_model, df_valido, coluna)

    print("\n✅ Pipeline finalizado com sucesso!\n")


if __name__ == "__main__":
    main()









