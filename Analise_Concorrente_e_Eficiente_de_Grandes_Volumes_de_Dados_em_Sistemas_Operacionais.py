import pandas as pd
from multiprocessing import Process,cpu_count
import psutil
#from datetime import datetime

# Conta o total de transações por país
def contar_dados (dataframe):
    #time.time()
    print("-Total de transações por país:")
    print(dataframe.groupby('country').size().sort_values(ascending=False)) 

# Calcula a média de preço por produto
def media_preco_produto(dataframe):
    print("-Média de preço por produto:")
    print(dataframe.groupby('product_id')['price_per_unit'].mean().sort_values(ascending=False))

# Calcula o total de vendas por empresa
def total_venda_empresa(dataframe):
    print("-Total de vendas por empresa:")
    print(dataframe.groupby('company_id')['total_price'].sum().sort_values(ascending=False))

# Conta a quantidade de transações por método de pagamento
def qtd_transacoes_metodo_pagamento(dataframe):
    print("-Quantidade de transações por método de pagamento:")
    print(dataframe.groupby('payment_method').size().sort_values(ascending=False))

# Distribui as vendas por mês e ano
def distribuicao_venda_mes_ano(dataframe):
    print("-Distribuição de vendas por mês/ano:")
    dataframe['transaction_date'] = pd.to_datetime(dataframe['transaction_date'])
    dataframe['month_year'] = dataframe['transaction_date'].dt.to_period('M')
    print(dataframe.groupby('month_year')['total_price'].sum())

# Conta as transações mais comuns por cidade
def transacoes_comuns_cidade(dataframe):
    print("-Transações mais comuns por cidade:")
    print(dataframe.groupby('city').size().sort_values(ascending=False))

# Calcula a média de gastos por usuário
def media_gastos_usuario(dataframe):
    print("-Média de gastos por usuário:")
    print(dataframe.groupby('user_id')['total_price'].mean().sort_values(ascending=False))

# Calcula o total de vendas em cada moeda
def total_vendas_cada_moeda(dataframe):
    print("-Total de vendas em cada moeda:")
    print(dataframe.groupby('currency')['total_price'].sum().sort_values(ascending=False))

def main ():
    # Configuração da memória e do tamanho do chunk para a leitura do arquivo CSV
    memoria_disponivel = psutil.virtual_memory().total - 6e9 
    bytes_por_linha = 150
    chunk_size = (memoria_disponivel) * 0.85 / ((bytes_por_linha) * (cpu_count()))
    arquivo = pd.read_csv("large_dataset.csv",chunksize=int(chunk_size), decimal=',')

    processos = []
    #enquanto houver blocos a serem processados:
    bloco = arquivo.get_chunk()
    Processo p(processar_bloco, bloco)
    processos.append(p)
    p.start()

    for processo in processos:
     processo.join() # aqui estava errado

    resultado = agrupar_resultados(p)
    mostrar_resultado(resultado)
    
if __name__ == '__main__':
    main()