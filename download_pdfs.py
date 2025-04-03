import os
import requests
import zipfile
from bs4 import BeautifulSoup

URL = "https://www.gov.br/ans/pt-br/acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos"
download_dir = "downloads"
os.makedirs(download_dir, exist_ok=True)

def baixar_pdf(url, nome_arquivo):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        caminho_arquivo = os.path.join(download_dir, nome_arquivo)
        with open(caminho_arquivo, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"Download concluído: {nome_arquivo}")
    else:
        print(f"Erro ao baixar {nome_arquivo}")

response = requests.get(URL)
soup = BeautifulSoup(response.text, "html.parser")
links = soup.find_all("a", href=True)

anexos = [link["href"] for link in links if "Anexo" in link["href"] and link["href"].endswith(".pdf")]

for i, anexo_url in enumerate(anexos, start=1):
    nome_arquivo = f"Anexo_{i}.pdf"
    baixar_pdf(anexo_url, nome_arquivo)

zip_path = "anexos.zip"
with zipfile.ZipFile(zip_path, "w") as zipf:
    for file in os.listdir(download_dir):
        if file.endswith(".pdf"):
            zipf.write(os.path.join(download_dir, file), file)
print(f"Arquivos compactados em {zip_path}")
