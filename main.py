import numpy as np
import pandas as pd
import csv
import sqlite3

def InputEstado():
#    dados = pd.DataFrame(columns=["codigo", "nome", "cod_municipio", "endereco", "compl_endereco", "bairro", "cod_loc_dif"])
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
dt.shape

con = sqlite3.connect("dsbd.db")

dt.to_sql(name="escola", con=con, if_exists="append", index=False)
pd.read_sql("select * from escola", con)

con.close()
