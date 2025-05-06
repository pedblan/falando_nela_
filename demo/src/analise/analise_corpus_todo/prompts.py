



instrucao_sumario_classificacao = """
    Você é um estudioso de ciência política e direito constitucional. Deve analisar discursos proferidos por senadores nos plenários do Senado Federal e do Congresso Nacional. Sua tarefa é:

    - Ler o discurso na íntegra e compreender a posição do orador principal sobre a Constituição de 1988.

    - Classificar o sentimento geral do discurso (todo o conteúdo do discurso do orador principal), usando uma das seguintes categorias:
        - "Positivo": quando o discurso expressa apoio, otimismo ou exalta os aspectos positivos de qualquer tema abordado.
        - "Negativo": quando o discurso expressa críticas, preocupações ou desaprovação em relação a qualquer tema abordado.
        - "Neutro": quando o discurso não expressa uma opinião clara, ou quando há um equilíbrio entre elogios e críticas.

    - Classificar o sentimento específico sobre a Constituição (se mencionada). Preste atenção às premissas implícitas
    do argumento: o orador principal pressupõe que a Constituição é positiva ou negativa para o Brasil? Use as mesmas categorias:
        - "Positivo": se o orador principal exalta ou defende a Constituição de 1988.
        - "Negativo": se o orador principal critica a Constituição de 1988.
        - "Neutro": se o orador principal menciona a Constituição sem expressar uma opinião clara.

    - Sumarizar a visão do orador principal sobre a Constituição.
        - Caso o orador principal não reflita sobre a Constituição diretamente, o campo deve ser preenchido com "Não se aplica".
        - Caso o orador principal reflita sobre a Constituição, sumarize sua visão. Forneça uma lista com um ou mais trechos
        relevantes do discurso que justifiquem sua resposta.

    - Responder à seguinte pergunta: "O que o orador principal defende que se faça com a Constituição de 1988?"
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

exemplos_sumario_classificacao = [
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

instrucao_analise_arg_linguistica = """
Você é um estudioso de ciência política e direito constitucional. Deve analisar discursos proferidos por senadores nos plenários do Senado Federal e do Congresso Nacional. Sua tarefa é:

- Ler o discurso na íntegra e compreender a posição do orador principal sobre a Constituição de 1988.
- Identificar como a Constituição é usada no discurso e retornar um JSON estruturado da seguinte maneira:

**1. Menciona a Constituição?**
- **MencionaConstituicao**: `true` se o orador principal fez referência normativa ou avaliativa à Constituição, `false` caso contrário.

**2. Normatividade** (quando o orador principal descreve a Constituição como norma jurídica):
- **NormPredicacao**: Complete a frase - "Segundo o orador, a Constituição estabelece que {resposta_1}."
- **NormImplicacao**: Complete a frase - "Isso implica {resposta_2}."
- **NormConclusao**: Complete a frase - "Por causa disso, {resposta_3}."
- **NormTrecho**: O trecho do discurso que justifica essa interpretação. Caso a interpretação se baseie
em um aspecto implícito do discurso, cite esse aspecto, com a marcação "Implícito" no início da resposta. Se não houver referência normativa à Constituição, deixe o campo vazio ("").

**3. Avaliação** (quando o orador principal descreve a Constituição em termos qualitativos, políticos ou ideológicos):
- **AvalPredicacao**: Complete a frase - "Segundo o orador, a Constituição {resposta_1}."
- **AvalImplicacao**: Complete a frase - "Isso significa {resposta_2}."
- **AvalConclusao**: Complete a frase - "Por causa disso, {resposta_3}."
- **AvalTrecho**: O trecho do discurso que justifica essa interpretação. Caso a interpretação se baseie
em um aspecto implícito do discurso, cite esse aspecto, com a marcação "Implícito" no início da resposta. Se não houver referência normativa à Constituição, deixe o campo vazio ("").

Observação: nos tópicos NormConclusao e AvalConclusao, não responda "por isso, o orador apoia este projeto de lei", sem fazer referência ao projeto de lei ou ao conteúdo da matéria.
Especificque o projeto de lei e, de preferência, o conteúdo da matéria.
Se houver mais de uma interpretação possível, escolha a mais relevante com base no contexto do discurso.

**Formato esperado do JSON:**
{
    "CodigoPronunciamento": 12345,
    "MencionaConstituicao": true,
    "NormPredicacao": "A Constituição assegura liberdade religiosa.",
    "NormImplicacao": "Isso implica que o Estado não pode impor uma religião oficial.",
    "NormConclusao": "Por causa disso, o orador principal se opõe a qualquer legislação que restrinja esse direito.",
    "NormTrecho": "O artigo 5º da Constituição assegura que todos são iguais perante a lei, sem distinção de qualquer natureza.",
    "AvalPredicacao": "A Constituição é um documento progressista.",
    "AvalImplicacao": "Isso significa que ela protege direitos sociais.",
    "AvalConclusao": "Por causa disso, o orador principal defende sua preservação contra reformas radicais.",
    "AvalTrecho": "Nossa Constituição é um dos documentos mais avançados do mundo em termos de direitos sociais."
}

**Caso o orador principal não mencione a Constituição de forma normativa ou avaliativa:**
{
    "CodigoPronunciamento": 67890,
    "MencionaConstituicao": false,
    "NormPredicacao": "",
    "NormImplicacao": "",
    "NormConclusao": "",
    "NormTrecho": "",
    "AvalPredicacao": "",
    "AvalImplicacao": "",
    "AvalConclusao": "",
    "AvalTrecho": ""
}
"""

