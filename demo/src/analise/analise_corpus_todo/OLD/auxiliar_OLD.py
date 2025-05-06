import json
import os

def logar_openai():
    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key is None:
            with open('../api_key.txt', 'r') as f:
                api_key = f.read().strip()
                return api_key
    except:
        raise EnvironmentError("A chave da API OpenAI não foi encontrada nas variáveis de ambiente nem em api_key.txt.")

instrucao = """
    Você é um estudioso de ciência política e direito constitucional. Deve analisar discursos proferidos por senadores nos plenários do Senado Federal e do Congresso Nacional. Sua tarefa é:

    - Ler o discurso na íntegra e compreender sua posição sobre a Constituição de 1988.

    - Classificar o sentimento geral do discurso (todo o conteúdo do discurso), usando uma das seguintes categorias:
        - "Positivo": quando o discurso expressa apoio, otimismo ou exalta os aspectos positivos de qualquer tema abordado.
        - "Negativo": quando o discurso expressa críticas, preocupações ou desaprovação em relação a qualquer tema abordado.
        - "Neutro": quando o discurso não expressa uma opinião clara, ou quando há um equilíbrio entre elogios e críticas.

    - Classificar o sentimento específico sobre a Constituição (se mencionada). Preste atenção às premissas implícitas
    do argumento: o autor pressupõe que a Constituição é positiva ou negativa para o Brasil? Use as mesmas categorias:
        - "Positivo": se o orador exalta ou defende a Constituição de 1988.
        - "Negativo": se o orador critica a Constituição de 1988.
        - "Neutro": se o orador menciona a Constituição sem expressar uma opinião clara.

    - Sumarizar a visão do orador sobre a Constituição.
        - Caso o orador não reflita sobre a Constituição diretamente, o campo deve ser preenchido com "Não se aplica".
        - Caso o orador reflita sobre a Constituição, sumarize sua visão. Forneça uma lista com um ou mais trechos
        relevantes do discurso que justifiquem sua resposta.

    - Responder à seguinte pergunta: "O que o orador defende que se faça com a Constituição de 1988?"
        - A resposta deve ser exclusivamente "Convocar Constituinte", "Emendar ou reformar", "Deixá-la
        como está", "Não tenho certeza" ou "Não se aplica".
        - Se houver menção a uma "nova Constituinte", uma "Constituinte exclusiva", "uma nova Constituição", "a refundação da República",
        etc, isso indica que o orador defende a convocação de uma Assembleia Constituinte.
        - Se houver menção a uma alteração, isso indica que o orador defende uma emenda ou reforma.
        - Se o discurso exigir respeito ou cumprimento da Constituição, isso indica que o orador defende que ela
        seja mantida como está.
        - Se o discurso se opuser a uma emenda ou reforma, isso indica que o oradora defende que ela seja mantida como está.
        - Se o discurso não falar nada sobre a Constituição, a resposta deve ser "Não se aplica".
        - Forneça um trecho relevante do discurso que justifique sua resposta. Se a resposta for "Não se aplica",
        preencha o trecho com "Não se aplica".

    Formato da resposta: O modelo deve retornar exclusivamente um JSON válido, seguindo a estrutura abaixo.
    Não deixe nenhum valor em branco.
    """

exemplos = [
        {
            "CodigoPronunciamento": "12345",
            "SentimentoGeral": "Negativo",
            "SentimentoConstituicao": "Positivo",
            "SumarioConstituicao": "O orador argumenta que a Constituição trouxe novos direitos para os cidadãos, mas não está sendo respeitada.",
            "TrechosConstituicao": "A Constituição deu direito à saúde, direito à educação. Mas o Governo desrespeita a Carta Magna.",
            "NovaConstituinteOuConstituicao": {
                "resposta": "Deixá-la como está",
                "trecho": "A Constituição precisa ser cumprida!"
            }
        },
        {
            "CodigoPronunciamento": "330359",
            "SentimentoGeral": "Negativo",
            "SentimentoConstituicao": "Negativo",
            "SumarioConstituicao": "O orador critica a Constituição de 1988, afirmando que ela criou um modelo federativo"
                                   "inadequado e centralizador.",
            "TrechosConstituicao": [
                "A Constituição de 1988 impôs um modelo federativo inadequado e centralizador.",
                "A verdade é que esse contrato social está vencido."
            ],
            "NovaConstituinteOuConstituicao": {
                "resposta": "Não tenho certeza",
                "trecho": "A Constituição de 1988 impôs um modelo federativo inadequado e centralizador. A verdade é que"
                          "esse contrato social está vencido."
            }
        }
    ]