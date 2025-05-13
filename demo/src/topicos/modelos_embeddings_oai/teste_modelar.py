import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from modelar_topicos import (
    carregar_dados,
    carregar_embeddings,
    verificar_embeddings,
    preparar_docs_bertopic,
    treinar_vetorizador,
    reduzir_dim_embeddings
)

@pytest.fixture(scope="module")
def df_discursos() -> pd.DataFrame:
    return carregar_dados()

@pytest.fixture(scope="module")
def coluna() -> str:
    return "NormCombinado"

@pytest.fixture(scope="module")
def embeddings_data(coluna):
    return carregar_embeddings(coluna)

def test_carregamento_dados(df_discursos):
    assert isinstance(df_discursos, pd.DataFrame)
    assert not df_discursos.empty
    assert "CodigoPronunciamento" in df_discursos.columns

def test_carregar_embeddings(embeddings_data):
    codigos, embeddings = embeddings_data
    assert isinstance(codigos, np.ndarray)
    assert isinstance(embeddings, np.ndarray)
    assert codigos.shape[0] == embeddings.shape[0]

def test_verificar_embeddings(df_discursos, embeddings_data):
    codigos, embeddings = embeddings_data
    df_valido, embeddings_validados = verificar_embeddings(codigos, embeddings, df_discursos)
    assert not df_valido.empty
    assert embeddings_validados.shape[0] == df_valido.shape[0]

def test_preparar_docs(df_discursos, coluna):
    docs = preparar_docs_bertopic(df_discursos, coluna)
    assert isinstance(docs, list)
    assert all(isinstance(doc, str) for doc in docs)
    assert len(docs) == df_discursos.shape[0]

def test_treinar_vetorizador():
    vectorizer = treinar_vetorizador()
    assert hasattr(vectorizer, "fit_transform")

def test_reduzir_dim_embeddings(embeddings_data):
    _, embeddings = embeddings_data
    reduced = reduzir_dim_embeddings(embeddings)
    assert isinstance(reduced, np.ndarray)
    assert reduced.shape[0] == embeddings.shape[0]
    assert reduced.shape[1] == 100
