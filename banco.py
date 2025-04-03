import pymysql
import pandas as pd

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "12345",
    "database": "ans_db",
    "port": 3306
}

def conectar_bd():
    """Estabelece conexão com o banco de dados MySQL."""
    try:
        return pymysql.connect(**DB_CONFIG)
    except pymysql.MySQLError as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None

def criar_tabelas():
    """Cria as tabelas no banco de dados."""
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
    """Importa os dados do CSV de operadoras para o banco de dados."""
    try:
        conexao = conectar_bd()
        if not conexao:
            return

        cursor = conexao.cursor()

        # 📌 Carregando CSV
        df = pd.read_csv(nome_arquivo, sep=";", dtype=str)
        print(f"📂 Colunas do CSV Operadoras: {df.columns.tolist()}")

        # 📌 Renomeando colunas para combinar com o banco
        df.rename(columns={"Registro_ANS": "registro_ans"}, inplace=True)

        colunas_validas = [
            "registro_ans", "CNPJ", "Razao_Social", "Nome_Fantasia", "Modalidade",
            "Logradouro", "Numero", "Complemento", "Bairro", "Cidade", "UF", "CEP",
            "DDD", "Telefone", "Fax", "Endereco_eletronico", "Representante",
            "Cargo_Representante", "Regiao_de_Comercializacao", "Data_Registro_ANS"
        ]

        df = df[colunas_validas]

        # 📌 Inserindo no banco de dados
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO operadoras (
                        registro_ans, cnpj, razao_social, nome_fantasia, modalidade,
                        logradouro, numero, complemento, bairro, cidade, uf, cep,
                        ddd, telefone, fax, endereco_eletronico, representante,
                        cargo_representante, regiao_de_comercializacao, data_registro
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE nome_fantasia = VALUES(nome_fantasia);
                """, tuple(row))
            except pymysql.MySQLError as e:
                print(f"❌ Erro ao inserir linha {row['registro_ans']}: {e}")

        conexao.commit()
        cursor.close()
        conexao.close()

        print("✅ Importação de operadoras concluída!")

    except Exception as e:
        print(f"❌ Erro ao importar CSV de operadoras: {e}")

if __name__ == "__main__":
    criar_tabelas()
    importar_operadoras("operadoras.csv")  # Nome do arquivo CSV com os dados das operadoras
