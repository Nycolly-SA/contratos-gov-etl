import pandas as pd
import time
from pathlib import Path
from datetime import datetime
import requests
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-6s | %(message)s",
    datefmt="%H:%M:%S"
)

# CONFIGURAÇÕES INICIAIS PARA EXTRAÇÃO DE DADOS
url = 'https://dadosabertos.compras.gov.br/'
endpoint_contratos = 'modulo-contratos/1_consultarContratos'
endpoint_uasg = 'modulo-uasg/1_consultarUasg'
endpoint_orgao = 'modulo-uasg/2_consultarOrgao'
data_dir = Path('data/raw')
data_dir.mkdir(parents=True, exist_ok=True)

# 1. Função genérica de extração
def extract_data(url, endpoint, max_records=None, params=None, max_retries=3):
    """
    Extrai dados de um endpoint da API até atingir max_records.
    Mantém paginação e retry em caso de falha.
    
    Args:
        url: URL base da API
        endpoint: Endpoint específico a ser consultado
        max_records: Número máximo de registros a serem extraídos
        params: Parâmetros da consulta (opcional). Se None, usa parâmetros padrão.
        max_retries: Número máximo de tentativas em caso de falha
    Returns:
        Lista de dicionários com os dados extraídos
    """
    params = {} if params is None else params.copy()
    params.setdefault("pagina", 1)
    params.setdefault("tamanhoPagina", 500)
    page_size = params["tamanhoPagina"]
    
    all_data = []
    total_records = 0
    current_page = 1

    logging.info(f"Iniciando extração de {endpoint}...")

    while True:
        params['pagina'] = current_page

        # Tentativas com retry em caso de falha
        for retry in range(max_retries):
            try:
                response = requests.get(url + endpoint, params=params, timeout=20)
                response.raise_for_status()
                data = response.json()
                results = data.get("resultado", [])

                if not results:
                    logging.info("Nenhum dado retornado. Fim da extração.")
                    return all_data
                
                # Adiciona resultados ao conjunto total
                all_data.extend(results)
                total_records += len(results)
                logging.info(f"Página {current_page}: {len(results)} registros (Total: {total_records})")

                # Se a página retornou menos que o tamanho, é a última
                if len(results) < page_size:
                    logging.info("Última página alcançada.")
                    return all_data
                
                time.sleep(0.5)
                break
            except requests.RequestException as e:
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 5 # Aumenta o tempo de espera a cada tentativa
                    logging.warning(f"Erro na requisição: {e}. Tentando novamente em {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Falha após {max_retries} tentativas: {e}")
                    return all_data
                
        # Verifica limite de registros se definido
        if max_records and total_records >= max_records:
            logging.info(f"Limite de {max_records} registros atingido.")
            return all_data

        current_page += 1

# 2. Função para extrair contratos por trimestre
def extract_contratos_por_trimestre(url, endpoint_contratos, save=True, ano=None, contratos_por_trimestre=None):
    """
    Extrai contratos divididos por trimestre, respeitando o limite.
    
    Args:
        url: URL base da API
        endpoint_contratos: Endpoint de contratos
        ano: Ano de referência (None = ano atual)
        contratos_por_trimestre: Número máximo de contratos por trimestre (None = sem limite)
        save: Se True, salva o DataFrame em CSV
    Returns:
        DataFrame com os contratos extraídos
    """
    if ano is None:
        ano = datetime.now().year - 1
        logging.info(f"Ano não especificado. Usando o ano anterior: {ano}")
        
    todos_contratos = []
    
    trimestres = [
        ("01-01", "03-31"),
        ("04-01", "06-30"),
        ("07-01", "09-30"),
        ("10-01", "12-31")
    ]
    
    for inicio_dia, fim_dia in trimestres:
        data_inicio = f"{ano}-{inicio_dia}"
        data_fim = f"{ano}-{fim_dia}"
        logging.info(f"-> Extraindo contratos de {data_inicio} até {data_fim}")
        
        params = {
            'tamanhoPagina': 500,
            'dataVigenciaInicialMin': data_inicio,
            'dataVigenciaInicialMax': data_fim,
        }
        contratos_trimestre = extract_data(url, endpoint_contratos, max_records=contratos_por_trimestre, params=params)
        logging.info(f"Trimestre {data_inicio} a {data_fim}: {len(contratos_trimestre)} contratos extraídos")
        todos_contratos.extend(contratos_trimestre)

    df = pd.DataFrame(todos_contratos)
    
    # Distribuição por trimestre
    if not df.empty and 'dataVigenciaInicial' in df.columns:
        try:
            df['data_inicio'] = pd.to_datetime(df['dataVigenciaInicial'])
            df['trimestre'] = df['data_inicio'].dt.quarter
            contagem = df['trimestre'].value_counts().sort_index()
            logging.info("\nDistribuição por trimestre:")
            for trim, count in contagem.items():
                logging.info(f"Trimestre {trim}: {count} contratos")
            logging.info(f"Total: {len(df)} contratos")
        except Exception as e:
            logging.error(f"Não foi possível verificar distribuição por trimestre: {e}")

    if save:
       df =  save_to_csv(df, f"contratos_amostra_{datetime.now().strftime('%Y-%m-%d')}.csv")
    return df

# 3. Função para extrair todos os dados do endpoint UASG
def extract_uasg(url, endpoint_uasg, save=True):
    """
    Extrai todos os UASGs usando paginação interna da API.
    
    Args:
        url: URL base da API
        endpoint_uasg: Endpoint de UASG
        save: Se True, salva os dados em CSV
    """
    logging.info("Iniciando extração de UASGs...")
    
    params = {
        'statusUasg': 'true',
        'tamanhoPagina': 500
    }
    # OBS: max_records=None garante que todos os registros sejam puxados
    uasg_data = extract_data(url, endpoint_uasg, max_records=None, params=params)
    
    logging.info(f"Extração concluída: {len(uasg_data)} UASGs")
    
    if save:
        return save_to_csv(uasg_data, f"uasg_{datetime.now().strftime('%Y-%m-%d')}.csv")
    return pd.DataFrame(uasg_data)

# 4. Função para extrair todos os dados do endpoint Órgão
def extract_orgao(url, endpoint_orgao, save=True):
    """
    Extrai todos os órgãos usando paginação interna da API.

    Args:
        url: URL base da API
        endpoint_orgao: Endpoint de órgãos
        save: Se True, salva os dados em CSV
    """
    logging.info("Iniciando extração de órgãos...")

    params = {
        'statusOrgao': 'true',
        'tamanhoPagina': 500
    }
    # OBS: max_records=None garante que todos os registros sejam puxados
    orgaos_data = extract_data(url, endpoint_orgao, max_records=None, params=params)

    logging.info(f"Extração concluída: {len(orgaos_data)} órgãos")

    if save:
        return save_to_csv(orgaos_data, f"orgao_{datetime.now().strftime('%Y-%m-%d')}.csv")
    return pd.DataFrame(orgaos_data)

# 5. Função para salvar dados de endpoints em CSV
def save_to_csv(data, filename, directory=data_dir):
    """
    Salva os dados em um arquivo CSV e retorna o DataFrame
    
    Args:
        data: Lista de dicionários ou DataFrame a ser salvo
        filename: Nome do arquivo CSV (sem extensão)
        directory: Diretório onde o arquivo será salvo
    """
    try:
        # Se os dados não forem DataFrame, converte
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            if not data: # lista vazia ou None
                logging.warning(f"Nenhum dado para salvar em {filename}")
                return pd.DataFrame()
            df = pd.DataFrame(data)

        # Garante extensão .csv
        if not filename.endswith(".csv"):
            filename += ".csv"
        
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / filename
        
        df.to_csv(file_path, index=False, encoding='utf-8')
        logging.info(f"Dados salvos em {file_path} ({len(df)} registros)")
        return df
    except Exception as e:
        logging.error(f"Erro ao salvar {filename}: {e}")
        return pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data

# EXECUÇÃO DIRETA PARA TESTES
if __name__ == "__main__":
    try:
        contratos_por_trimestre = 100  # amostra reduzida para teste
        uasg_max_paginas = 1
        orgao_max_paginas = 1
        logging.info("MODO TESTE: Usando amostra reduzida de dados")

        # 1️⃣ Extração de contratos
        print()
        logging.info("1. Extraindo dados de contratos...")
        contratos_df = extract_contratos_por_trimestre(url, endpoint_contratos, contratos_por_trimestre=contratos_por_trimestre)

        # 2️⃣ Extração de UASGs
        print()
        logging.info("2. Extraindo dados de UASGs...")
        uasg_df = extract_uasg(url, endpoint_uasg, max_pages=uasg_max_paginas)

        # 3️⃣ Extração de órgãos
        print()
        logging.info("3. Extraindo dados de órgãos...")
        orgao_df = extract_orgao(url, endpoint_orgao, max_pages=orgao_max_paginas)

        # 4️⃣ Resumo final
        print()
        logging.info("Extração completa!")
        logging.info(f"Contratos: {len(contratos_df)} registros")
        logging.info(f"UASGs: {len(uasg_df)} registros")
        logging.info(f"Órgãos: {len(orgao_df)} registros")

    except Exception as e:
        logging.critical(f"Erro crítico durante a execução do pipeline: {e}")