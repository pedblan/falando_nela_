import os

# Caminho do arquivo original e pasta de destino
arquivo_base = "../data/graficos/2007.twb"

# Lê o conteúdo do arquivo base
with open(arquivo_base, "r", encoding="utf-8") as f:
    conteudo_original = f.read()

# Gera arquivos para os anos de 2008 a 2024
for ano in range(2008, 2025):
    novo_conteudo = conteudo_original.replace("2007", str(ano))
    novo_caminho = f"../data/graficos/{ano}.twb"
    with open(novo_caminho, "w", encoding="utf-8") as f:
        f.write(novo_conteudo)
    print(f"✅ Gerado: {ano}.twb")
