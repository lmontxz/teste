import pymysql
import pandas as pd
import numpy as np

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "12345",
    "database": "ans_db",
    "port": 3306
}

def conectar_bd():
    try:
        return pymysql.connect(**DB_CONFIG)
    except pymysql.MySQLError as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None

def criar_tabelas():
    conexao = conectar_bd()
    if not conexao:
        return
    cursor = conexao.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS operadoras (
        id INT AUTO_INCREMENT PRIMARY KEY,
        registro_ans VARCHAR(20) UNIQUE,
        cnpj VARCHAR(20),
        razao_social VARCHAR(255),
        nome_fantasia VARCHAR(255),
        modalidade VARCHAR(50),
        logradouro VARCHAR(255),
        numero VARCHAR(20),
        complemento VARCHAR(255),
        bairro VARCHAR(100),
        cidade VARCHAR(100),
        uf CHAR(2),
        cep VARCHAR(15),
        ddd VARCHAR(5),
        telefone VARCHAR(20),
        fax VARCHAR(20),
        endereco_eletronico VARCHAR(255),
        representante VARCHAR(255),
        cargo_representante VARCHAR(100),
        regiao_de_comercializacao VARCHAR(255),
        data_registro DATE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS demonstrativos_contabeis (
        id INT AUTO_INCREMENT PRIMARY KEY,
        registro_ans VARCHAR(20),
        ano INT,
        trimestre INT,
        receita_total DECIMAL(15,2),
        despesas_eventos DECIMAL(15,2),
        FOREIGN KEY (registro_ans) REFERENCES operadoras(registro_ans)
    );
    """)

    conexao.commit()
    cursor.close()
    conexao.close()
    print("✅ Tabelas criadas com sucesso!")

def importar_operadoras(nome_arquivo):
    try:
        conexao = conectar_bd()
        if not conexao:
            return

        cursor = conexao.cursor()

        df = pd.read_csv(nome_arquivo, sep=";", dtype=str)
        print(f"📂 Colunas do CSV Operadoras: {df.columns.tolist()}")

        df.rename(columns={"Registro_ANS": "registro_ans"}, inplace=True)

        colunas_validas = [
            "registro_ans", "CNPJ", "Razao_Social", "Nome_Fantasia", "Modalidade",
            "Logradouro", "Numero", "Complemento", "Bairro", "Cidade", "UF", "CEP",
            "DDD", "Telefone", "Fax", "Endereco_eletronico", "Representante",
            "Cargo_Representante", "Regiao_de_Comercializacao", "Data_Registro_ANS"
        ]

        df = df[colunas_validas]

        df.replace({np.nan: None}, inplace=True)

        for _, row in df.iterrows():
            try:
                placeholders = ", ".join(["%s"] * len(colunas_validas))
                query = f"""
                    INSERT INTO operadoras ({', '.join(colunas_validas)})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE nome_fantasia = VALUES(nome_fantasia);
                """
                cursor.execute(query, tuple(row))
            except pymysql.MySQLError as e:
                print(f"❌ Erro ao inserir linha {row.get('registro_ans', 'N/A')}: {e}")

        conexao.commit()
        cursor.close()
        conexao.close()

        print("✅ Importação de operadoras concluída!")

    except Exception as e:
        print(f"❌ Erro ao importar CSV de operadoras: {e}")

def importar_despesas(nome_arquivo):
    try:
        conexao = conectar_bd()
        if not conexao:
            return

        cursor = conexao.cursor()

        df = pd.read_csv(nome_arquivo, sep=";", dtype=str)
        print(f"📂 Colunas do CSV Despesas: {df.columns.tolist()}")

        colunas_trimestrais = [col for col in df.columns if col.replace(",", "").isdigit()]

        if not colunas_trimestrais:
            print("❌ Erro: Arquivo não contém colunas trimestrais!")
            return

        df["trimestre"] = df[colunas_trimestrais].apply(pd.to_numeric, errors="coerce").idxmax(axis=1)
        df["trimestre"] = df["trimestre"].fillna(0).astype(str).str.extract(r'(\d+)').astype(float) // 3 + 1
        df["trimestre"] = df["trimestre"].fillna(0).astype(int)

        df.rename(columns={"Registro_ANS": "registro_ans"}, inplace=True)

        colunas_necessarias = ["registro_ans", "trimestre"]
        colunas_presentes = [col for col in colunas_necessarias if col in df.columns]

        if len(colunas_presentes) < len(colunas_necessarias):
            print(f"❌ Erro: Colunas ausentes no CSV de despesas! Encontradas: {colunas_presentes}")
            return

        df = df[colunas_presentes]
        df.replace({np.nan: None}, inplace=True)

        for _, row in df.iterrows():
            try:
                placeholders = ", ".join(["%s"] * len(colunas_presentes))
                query = f"""
                    INSERT INTO demonstrativos_contabeis ({', '.join(colunas_presentes)})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE trimestre = VALUES(trimestre);
                """
                cursor.execute(query, tuple(row))
            except pymysql.MySQLError as e:
                print(f"❌ Erro ao inserir linha {row.get('registro_ans', 'N/A')}: {e}")

        conexao.commit()
        cursor.close()
        conexao.close()

        print("✅ Importação de despesas concluída!")

    except Exception as e:
        print(f"❌ Erro ao importar CSV de despesas: {e}")

if __name__ == "__main__":
    criar_tabelas()
    importar_operadoras("operadoras.csv")  
    importar_despesas("despesas.csv")
