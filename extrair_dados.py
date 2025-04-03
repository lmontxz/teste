import os
import pdfplumber
import pandas as pd

input_pdf = "downloads/Anexo_1.pdf"
output_csv = "Teste_nayara.csv"

def extrair_tabela_do_pdf(pdf_path):
    dados = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tabelas = page.extract_tables()
            for tabela in tabelas:
                for linha in tabela:
                    dados.append(linha)
    return dados

dados_extraidos = extrair_tabela_do_pdf(input_pdf)
df = pd.DataFrame(dados_extraidos)
df.to_csv(output_csv, index=False, encoding="utf-8")
print(f"Dados extraídos e salvos em {output_csv}")