import pandas as pd
import os
import glob

# local = 'data'
def extrair_arq(local:str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(local,'*.json')) # Armazena todos os arquivos em uma lista
    df_list = [pd.read_json(arquivos) for arquivos in arquivos_json] # percorre cada item e o transforma em data frame
    df = pd.concat(df_list,ignore_index=True)
    return df


def calcular_kpi(df:pd.DataFrame) -> pd.DataFrame:
    df['Total'] = df['Venda']*df['Quantidade']
    return df

def carregar_dados(df:pd.DataFrame, saida:list):
    for tipo in saida:
        if tipo == "parquet":
            df.to_parquet('dados.parquet',index=False)
        if tipo == "csv":
            df.to_csv("dados.csv",index=False)


def pipeline_calcular(pasta:str, formato:list):
    df_inicial = extrair_arq(pasta)
    df_calculado = calcular_kpi(df_inicial)
    carregar_dados(df_calculado,formato)

pipeline_calcular('data',['csv','parquet'])