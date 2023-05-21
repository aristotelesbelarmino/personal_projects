from logging import log
import openpyxl
import database_connection as dbconn
import database_insert as dbins
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import time
import pandas.tseries.offsets as ts
from datetime import datetime as dt
import requests
import pandas as pd
import pyodbc
load_dotenv()

connection = dbconn.microsoftsql_conn(f"{os.environ['driver']}",f"{os.environ['servidor']}",f"{os.environ['bancodedados']}",f"{os.environ['senha']}",f"{os.environ['usuario']}")

def work_day():
    connection = dbconn.microsoftsql_conn(f"{os.environ['driver']}",f"{os.environ['servidor']}",f"{os.environ['bancodedadosdias']}",f"{os.environ['senha']}",f"{os.environ['usuario']}")
    SELECT = ('''SELECT "data" FROM "dias_uteis"''')
    SELECT = connection.execute(SELECT)
    SELECT = SELECT.fetchall()
    return SELECT

def generating_day_work(days):

    work_day_list:list = work_day()
    date_list = []
 
    for i in work_day_list:

        date_list.append(str(i[0]))
    today = pd.to_datetime("today") + pd.DateOffset(days=days)
    day = today.strftime("%d")
    mounth = today.strftime("%m")
    year = today.strftime("%Y")

    date = f"{year}-{mounth}-{day}"
    while True:
        if date in date_list:
            return date
        else:

            try:
                today = pd.to_datetime(today) - ts.BDay()
                day = today.strftime("%d")
                mounth = today.strftime("%m")
                year = today.strftime("%Y")
                date = f"{year}-{mounth}-{day}"    
            except:

                return "Date not found on database"

def pegando_quantidade_de_paginas(data):
    
    try:
        dia:str = data[8:10]
        mes:str = data[5:7]
        ano:str = data[0:4]
        url = f"https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=15&s=10&l=10&o%5B0%5D%5BdataEntrega%5D=asc&idCategoriaDocumento=0&idTipoDocumento=0&idEspecieDocumento=0&dataInicial={dia}%2F{mes}%2F{ano}&dataFinal={dia}%2F{mes}%2F{ano}&_=1669387095451"
        payload={}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload)
        request = response.json()
        response.close()
        quantidade_de_paginas:int = request["recordsTotal"]
        print("Lendo quantidade de fundos")
        time.sleep(0.01)
    except:
        pass

    return quantidade_de_paginas

def lendo_fundos(quantidade_de_paginas,data):
    try:
        dicionario = {}
        lista = []
        lista_cópia = []
        lista_suporte = []

        for pagina in range(0, quantidade_de_paginas):
            dia:str = data[8:10]
            mes:str = data[5:7]
            ano:str = data[0:4]
            url = f"https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=0&s={pagina}&l=200&o%5B0%5D%5BdataEntrega%5D=asc&idCategoriaDocumento=0&idTipoDocumento=0&idEspecieDocumento=0&dataInicial={dia}%2F{mes}%2F{ano}&dataFinal={dia}%2F{mes}%2F{ano}&_=1669387095451"
            response = requests.request("GET", url)
            request = response.json()
            response.close()
            time.sleep(0.01)


            dicionario["linkArq"] = request["data"][0]["id"]
            dicionario["denomSocial"] = request["data"][0]["descricaoFundo"]
            dicionario["categoriaDoc"] = request["data"][0]["categoriaDocumento"]
            dicionario["tipoDoc"] = request["data"][0]["tipoDocumento"]
            dicionario["especieDocumento"] = request["data"][0]["especieDocumento"]
            dtEntrega = request["data"][0]["dataEntrega"]
            dicionario["dtRecebido"] = request["data"][0]["dataReferencia"]

            dicionario["dtEntrega"] = str(f"{dtEntrega[6:10]}-{dtEntrega[3:5]}-{dtEntrega[0:2]}")
            dicionario["status"] = request["data"][0]["status"]
            dicionario["descStatus"] = request["data"][0]["descricaoStatus"]
            dicionario["analisado"] = request["data"][0]["analisado"]
            dicionario["situacaoDoc"] = request["data"][0]["situacaoDocumento"]
            dicionario["altaPrioridade"] = request["data"][0]["altaPrioridade"]
            dicionario["formatoDtReferencia"] = request["data"][0]["formatoDataReferencia"]
            dicionario["versao"] = request["data"][0]["versao"]
            dicionario["modalidade"] = request["data"][0]["modalidade"]
            dicionario["descModalidade"] = request["data"][0]["descricaoModalidade"]
            dicionario["nomePregao"] = request["data"][0]["nomePregao"] 
            dicionario["infAdicionais"] = request["data"][0]["informacoesAdicionais"]
            dicionario["cnpjFundo"] = request["data"][0]["cnpjFundo"]

            lista_suporte = dicionario
            lista_cópia = lista_suporte.copy()
            lista.append(lista_cópia)
            lista_suporte.clear()
            print(f"{pagina + 1}º fundo adicionado a memória volátio")  

        print("Processo de captura de fundos finalizado")
    except:
        pass
    return lista

def gravando_link_dos_arquivos(fundos):    
    for id in fundos:
        id["linkArq"] = f'https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento?id={id["linkArq"]}'
        print(f"Adicionando o link de download dos arquivos")

def escrevendo_xlsx_temporário(fundos, data):
    dataframe = pd.DataFrame(fundos)
    dataframe.to_excel(f"fundos_net_{data}.xlsx", index=None)
    print("Escrevendo Excel temporário")
    return dataframe

def capturando_cnpj(fundos):
    try:
        urls = []
        cnpjs = []
        for url in fundos:
            urls.append(url["linkArq"])
        contador = 1
        for url in urls:    

            payload={}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
            request = response.headers
            response.close()
            time.sleep(0.01)

            cnpj = request["Content-Disposition"][22:36]
            cnpjs.append(cnpj)
            print(f"Buscando CNPJ do {contador}º fundo")
            contador = contador + 1
    except:
        pass
    return cnpjs

def gravando_cnpjs(cnpjs, df_fundos):
    dataframe = pd.DataFrame(cnpjs)
    df_fundos["cnpjFundo"] = dataframe
    print("Escrevendo dataframe definitivo")
    return df_fundos

def escrevendo_xlsx_definitivo(fundos, data):
    dataframe = pd.DataFrame(fundos)
    dataframe.to_excel(f"fundos_net_{data}.xlsx", index=None)
    print("Excel temoporário substituido pelo excel definitivo")

def gerando_time_stamp(): 
    hoje = dt.now()
    dia = hoje.strftime("%d")
    mes = hoje.strftime("%m")
    ano = hoje.strftime("%Y")
    data = str(f"{ano}-{mes}-{dia}")
    mensagem = f"Data de referência da busca: {data}"
    log(mensagem)
    return data

print("Carregando funções")
engine_dias_uteis = connection
print("Criando conexão com a base de dados")
data = generating_day_work(-1)
print(f"Data de busca: {data}")
quantidade_de_paginas = pegando_quantidade_de_paginas(data)
print(f"Quantidade de paginas iteradas {quantidade_de_paginas}")
fundos = lendo_fundos(quantidade_de_paginas,data)
print("Leitura de fundos")
gravando_link_dos_arquivos(fundos)
df_fundos = escrevendo_xlsx_temporário(fundos, data)
cnpjs = capturando_cnpj(fundos)
dataframe = gravando_cnpjs(cnpjs, df_fundos)
escrevendo_xlsx_definitivo(dataframe, data)
print("Processo finalizado")

def inserindo_na_base(CONEXAO, dataframe):

    dataframe = pd.DataFrame(dataframe, index=None)
    for linha in dataframe.index:

        SQL = (f'''insert into fundoNET(
         linkArq
        ,denomSocial
        ,categoriaDoc
        ,tipoDoc
        ,especieDocumento
        ,dtEntrega
        ,dtRecebido
        ,status
        ,descStatus
        ,analisado
        ,situacaoDoc
        ,altaPrioridade
        ,formatoDtReferencia
        ,versao
        ,modalidade
        ,descModalidade
        ,nomePregao 
        ,infAdicionais
        ,cnpjFundo)
        values(
         {dbins.db_str(str(dataframe["linkArq"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["denomSocial"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["categoriaDoc"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["tipoDoc"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["especieDocumento"].iloc[linha]))}
        ,{dbins.db_date(dataframe["dtEntrega"].iloc[linha])}
        ,{dbins.db_str(str(dataframe["dtRecebido"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["status"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["descStatus"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["analisado"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["situacaoDoc"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["altaPrioridade"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["formatoDtReferencia"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["versao"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["modalidade"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["descModalidade"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["nomePregao"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["infAdicionais"].iloc[linha]))}
        ,{dbins.db_str(str(dataframe["cnpjFundo"].iloc[linha]))});''')
        print(SQL)
        CONEXAO.execute(SQL)
        CONEXAO.commit()


inserindo_na_base(connection, dataframe)