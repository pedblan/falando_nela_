import shutil
import os

# Caminho base
pasta_origem = "data/graficos"
arquivo_base = "2007.twb"

# Criar cópias para 2008 a 2024
for ano in range(2008, 2025):
    origem = os.path.join(pasta_origem, arquivo_base)
    destino = os.path.join(pasta_origem, f"{ano}.twb")
    shutil.copyfile(origem, destino)

print("✅ Cópias criadas com sucesso de 2008 a 2024.")
