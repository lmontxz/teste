import pandas as pd
import mysql.connector

# 📌 Configurações do banco de dados
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "12345",  # Substitua pela sua senha
    "database": "ans_db",
}

# 📌 Nome do arquivo CSV
CSV_FILE = "operadoras.csv"

try:
    # ✅ Conectar ao MySQL
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ Conectado ao MySQL!")

    # 📌 Identifica automaticamente o delimitador correto (; ou ,)
    with open(CSV_FILE, "r", encoding="utf-8") as file:
        first_line = file.readline()
    delimiter = ";" if ";" in first_line else ","
    print(f"📌 Separador detectado: '{delimiter}'")

    # ✅ Carregar CSV com o delimitador detectado
    df = pd.read_csv(CSV_FILE, delimiter=delimiter, dtype=str)
    print("✅ CSV carregado com sucesso!")

    # 📌 Exibir colunas encontradas no CSV
    colunas_encontradas = set(df.columns)
    print("📌 Colunas encontradas:", colunas_encontradas)

    # 📌 Lista de colunas esperadas no banco
    colunas_esperadas = {
        "Registro_ANS", "CNPJ", "Razao_Social", "Nome_Fantasia", "Modalidade",
        "Logradouro", "Complemento", "Bairro", "Cidade", "UF", "CEP",
        "DDD", "Telefone", "Fax", "Endereco_eletronico", "Representante",
        "Cargo_Representante", "Regiao_de_Comercializacao", "Data_Registro_ANS"
    }

    # 🚨 Verifica se todas as colunas esperadas estão no CSV
    if not colunas_esperadas.issubset(colunas_encontradas):
        colunas_faltando = colunas_esperadas - colunas_encontradas
        raise KeyError(f"❌ ERRO: As colunas do CSV não correspondem! Faltando: {colunas_faltando}")

    # 📌 Converter a data para o formato correto (YYYY-MM-DD)
    df["Data_Registro_ANS"] = pd.to_datetime(df["Data_Registro_ANS"], errors="coerce", format="%d/%m/%Y").dt.strftime("%Y-%m-%d")

    # ✅ Query para inserção no MySQL
    insert_query = """
        INSERT INTO operadoras (
            registro_ans, cnpj, razao_social, nome_fantasia, modalidade,
            logradouro, complemento, bairro, cidade, uf, cep,
            ddd, telefone, fax, endereco_eletronico, representante,
            cargo_representante, regiao_de_comercializacao, data_registro_ans
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # ✅ Inserir os dados linha por linha
    linhas_inseridas = 0
    erros = []

    for _, row in df.iterrows():
        try:
            cursor.execute(insert_query, (
                row["Registro_ANS"], row["CNPJ"], row["Razao_Social"], row["Nome_Fantasia"], row["Modalidade"],
                row["Logradouro"], row["Complemento"], row["Bairro"], row["Cidade"], row["UF"], row["CEP"],
                row["DDD"], row["Telefone"], row["Fax"], row["Endereco_eletronico"], row["Representante"],
                row["Cargo_Representante"], row["Regiao_de_Comercializacao"], row["Data_Registro_ANS"]
            ))
            conn.commit()
            linhas_inseridas += 1
        except Exception as e:
            erros.append(f"❌ Erro ao inserir linha {row['Registro_ANS']}: {e}")

    print(f"✅ {linhas_inseridas} linhas inseridas com sucesso!")

    # 📌 Exibir erros, se houver
    if erros:
        print("⚠️ Erros encontrados:")
        for erro in erros:
            print(erro)

except Exception as e:
    print(f"❌ ERRO GERAL: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("🔌 Conexão fechada.")
