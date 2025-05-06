from openai import OpenAI
import os

api_key = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=api_key)


def analisar_texto_sumarizacao_classificacao(codigo_pronunciamento, texto, instrucao):
    """
    Envia um discurso para análise do LLM e retorna os resultados estruturados.

    Args:
        codigo_pronunciamento (int): Código identificador do discurso.
        texto (str): Texto do discurso.

    Returns:
        dict: Resultado estruturado da análise.
    """
    api_key = os.getenv("OPENAI_API_KEY")

    mensagens = [
        {"role": "developer", "content": instrucao},
        {"role": "user", "content": f"Analise este discurso. CodigoPronunciamento: {codigo_pronunciamento}. TextoIntegral: {texto}"}
    ]

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=mensagens,
        temperature=0.2,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "discursos_schema",
                "schema": {
                    "type": "object",
                    "properties": {
                        "CodigoPronunciamento": {"type": "integer"},
                        "SentimentoGeral": {"type": "string", "enum": ["Positivo", "Negativo", "Neutro"]},
                        "SentimentoConstituicao": {
                            "type": ["string"],
                            "enum": ["Positivo", "Negativo", "Neutro", "Não se aplica"]
                        },
                        "SumarioConstituicao": {"type": ["string", "null"]},
                        "TrechosConstituicao": {"type": ["array", "null"], "items": {"type": "string"}},
                        "NovaConstituinteOuConstituicao": {
                            "type": "object",
                            "properties": {
                                "resposta": {"type": "string", "enum": ["Convocar Constituinte", "Emenda ou reforma",
                                                                        "Deixá-la como está", "Não tenho certeza", "Não se aplica"]},
                                "trecho": {"type": "string"}
                            },
                            "required": ["resposta"]
                        }
                    },
                    "required": ["CodigoPronunciamento", "SentimentoGeral", "NovaConstituinteOuConstituicao"]
                }
            }
        }
    )

    resultado = response.choices[0].message.content
    return resultado

def analisar_texto_arg_linguistica(codigo_pronunciamento, texto, instrucao):
    mensagens = [
        {"role": "developer", "content": instrucao},
        {"role": "user",
         "content": f"Analise este discurso. CodigoPronunciamento: {codigo_pronunciamento}. TextoIntegral: {texto}"}
    ]
    try:
        response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=mensagens,
                    temperature=0.2,
                    response_format={
            "type": "json_schema",
            "json_schema":
             {"name": "resposta_schema",
              "schema":{ "type": "object",
                "properties": {
                    "CodigoPronunciamento": {"type": "integer"},
                    "MencionaConstituicao": {
                        "type": "boolean",
                        "enum": [True, False]
                    },
                    "NormPredicacao": {"type": ["string", "null"]},
                    "NormImplicacao": {"type": ["string", "null"]},
                    "NormConclusao": {"type": ["string", "null"]},
                    "NormTrecho": {
                        "type": ["string", "null"],
                        "description": "Trecho ou aspecto implícito do discurso que justifica a interpretação."
                    },
                    "AvalPredicacao": {"type": ["string", "null"]},
                    "AvalImplicacao": {"type": ["string", "null"]},
                    "AvalConclusao": {"type": ["string", "null"]},
                    "AvalTrecho": {
                        "type": ["string", "null"],
                        "description": "Trecho ou aspecto implícito do discurso que justifica a interpretação."
                    }
                },
                "required": [
                    "CodigoPronunciamento",
                    "MencionaConstituicao",
                    "NormPredicacao",
                    "NormImplicacao",
                    "NormConclusao",
                    "NormTrecho",
                    "AvalPredicacao",
                    "AvalImplicacao",
                    "AvalConclusao",
                    "AvalTrecho"
                ]
            }}}

                )

        resultado = response.choices[0].message.content  # Retorna a resposta como string JSON
        return resultado

    except Exception as e:
        print(f"Erro ao analisar o discurso {codigo_pronunciamento}: {e}")
        return None