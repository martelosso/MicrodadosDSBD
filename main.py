import numpy as np
import pandas as pd
import csv
import sqlite3

# ----------------------------------------
# 1. Preencha o banco de dados com as escolas

# Cria um dataframe lendo os dados microdados_1k.csv

def InputEstado():
    dados = pd.DataFrame()
    with open('microdados_1k.csv', encoding="ISO-8859-1") as dados_csv:
        leitor = csv.DictReader(dados_csv, delimiter=";")
        for linha in leitor:
            new_row = pd.DataFrame({
                "codigo":linha["CO_ENTIDADE"],
                "nome":linha["NO_ENTIDADE"],
                "cod_municipio":linha["CO_MUNICIPIO"],
                "endereco":linha["DS_ENDERECO"],
                "compl_endereco":linha["DS_COMPLEMENTO"],
                "bairro":linha["NO_BAIRRO"],
                "cod_loc_dif":linha["TP_LOCALIZACAO_DIFERENCIADA"]
            }, index=[0])
            dados = pd.concat([dados, new_row])
        return dados
dt = InputEstado()

# Conecta com a base de dados dsbd.db
con = sqlite3.connect("dsbd.db")

# Converte o dataframe para uma base sql (caso exista entao faca um append)
dt.to_sql(name="escola", con=con, if_exists="append", index=False)

# Verifica
pd.read_sql("select * from escola", con)

# Fecha a coneccao
con.close()


# ----------------------------------------
# 2. Preencha o banco de dados com os telefones das escolas (mesmos passos do ex.1)

con = sqlite3.connect("dsbd.db")

def InputTelefone():
    dados = pd.DataFrame()
    with open('microdados_1k.csv', encoding="ISO-8859-1") as dados_csv:
        leitor = csv.DictReader(dados_csv, delimiter=";")
        for linha in leitor:
            new_row = pd.DataFrame({
                "cod_escola":linha["CO_ENTIDADE"],
                "ddd":linha["NU_DDD"],
                "numero":linha["NU_TELEFONE"]
            }, index=[0])
            dados = pd.concat([dados, new_row])
        return dados

dt = InputTelefone()

con = sqlite3.connect("dsbd.db")

dt.to_sql(name="telefone", con=con, if_exists="append", index=False)

pd.read_sql("select * from telefone", con)

con.close()

# ----------------------------------------
# 3. Preencha com alguns dos vinculos validos

con = sqlite3.connect("dsbd.db")

# Cria cursor
cur = con.cursor()

# Insere linhas na tabela vinculo
# Ja existem 3 linhas na tabela vinculo, portanto acrescentei apenas a linha relacionada a "OUTRO_ORGAO"
cur.execute("INSERT INTO vinculo (codigo, nome,descricao) VALUES (4, 'Outro orgao', 'Orgao ao qual a escola publica esta vinculada - Outro orgao da administracao publica.')")

pd.read_sql("select * from vinculo", con)

con.close()

# ----------------------------------------
# 4. Preencha o banco de dados com pelo menos 5 medicoes

# Carrega colunas adicionais ao dataframe
def InputEscolaMedidas():
    dados = pd.DataFrame()
    with open('microdados_1k.csv', encoding="ISO-8859-1") as dados_csv:
        leitor = csv.DictReader(dados_csv, delimiter=";")
        for linha in leitor:
            new_row = pd.DataFrame({
                "codigo":linha["CO_ENTIDADE"],
                "qt_mas_bas":linha["QT_MAT_BAS"],
                "qt_mas_bas_fem":linha["QT_MAT_BAS_FEM"],
                "qt_mas_bas_masc":linha["QT_MAT_BAS_MASC"],
                "qt_salas_utilizadas":linha["QT_SALAS_UTILIZADAS"],
            }, index=[0])
            dados = pd.concat([dados, new_row])
        return dados
dt = InputEscolaMedidas()

con = sqlite3.connect("dsbd.db")
cur = con.cursor()

# Drop se existe
cur.execute(
    """
        DROP TABLE IF EXISTS EscolaMedidas;
    """
)

# Cria tabela adicional
cur.execute(
    """
        CREATE TABLE EscolaMedidas (
            codigo integer PRIMARY KEY,
            qt_mas_bas integer NULL,
            qt_mas_bas_fem integer NULL,
            qt_mas_bas_masc integer NULL, 
            qt_salas_utilizadas integer NULL
        );
    """
)

# Popula a tabela
dt.to_sql(name="EscolaMedidas", con=con, if_exists="append", index=False)
pd.read_sql("select * from EscolaMedidas", con)

# Algumas medidas interessantes
pd.read_sql("""
    select 
        *,
        cast(qt_mas_bas_fem as REAL)/qt_mas_bas
            as PCT_FEM,
        cast(qt_mas_bas_masc as REAL)/qt_mas_bas
            as PCT_MAS,
        cast(qt_mas_bas as REAL)/qt_salas_utilizadas
            as QT_ALUNO_SALA
    from EscolaMedidas;""",
    con
)
