import pandas as pd
import time
from pathlib import Path
from datetime import datetime
import requests
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S")

url = 'https://dadosabertos.compras.gov.br/'
# Endpoints para extração de dados
endpoint_contratos = 'modulo-contratos/1_consultarContratos'
endpoint_uasg = 'modulo-uasg/1_consultarUasg'
endpoint_orgao = 'modulo-uasg/2_consultarOrgao'

# Diretório para salvar CSVs
data_dir = Path('data/raw')
data_dir.mkdir(parents=True, exist_ok=True)

# 1. Função genérica de extração
def extract_data(url, endpoint, max_records=10000, params=None, max_retries=3):
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
    max_pages = (max_records // page_size) + 1 # Quantas páginas serão necessárias para atingir o max_records

    logging.info(f"Iniciando extração de {endpoint}...")

    for current_page in range(1, max_pages + 1):
        params['pagina'] = current_page

        # Tentativas com retry em caso de falha
        for retry in range(max_retries):
            try:
                response = requests.get(url + endpoint, params=params, timeout=20)
                response.raise_for_status()  # Levanta um erro para códigos de status HTTP 4xx/5xx
                data = response.json()
                results = data.get("resultado", [])

                if not results:
                    logging.info("Nenhum dado retornado. Fim da extração.")
                    return all_data
                
                # Adicionar resultados ao conjunto total
                all_data.extend(results)
                total_records += len(results)
                logging.info(f"Página {current_page}/{max_pages}: {len(results)} registros (Total: {total_records})")

                # Se a página retornou menos que o tamanho, é a última
                if len(results) < page_size:
                    logging.info("Última página alcançada.")
                    return all_data
                
                time.sleep(0.5)
                break # sai do retry se funcionou
            except requests.RequestException as e:
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 5 # Aumenta o tempo de espera a cada tentativa
                    logging.warning(f"Erro na requisição: {e}. Tentando novamente em {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Falha após {max_retries} tentativas: {e}")
                    return all_data
        if total_records >= max_records:
            logging.info(f"Limite de {max_records} registros atingido.")
            break

    logging.info(f"Extração concluída: {total_records} registros obtidos.")
    return all_data

# 2. Função para extrair contratos por trimestre
def extract_contratos_por_trimestre(url, endpoint_contratos, ano=2024, contratos_por_trimestre=5000, save=True):
    """
    Extrai contratos divididos por trimestre, respeitando o limite por trimestre.
    
    Args:
        url: URL base da API
        endpoint_contratos: Endpoint de contratos
        ano: Ano de referência
        contratos_por_trimestre: Número máximo de contratos por trimestre
        save: Se True, salva o DataFrame em CSV
    Returns:
        DataFrame com os contratos extraídos
    """
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
        logging.info(f"Extraindo contratos de {data_inicio} até {data_fim}")
        
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
        except Exception as e:
            logging.error(f"Não foi possível verificar distribuição por trimestre: {e}")

    if save:
       df =  save_to_csv(df, f"contratos_amostra_{datetime.now().strftime('%Y-%m-%d')}.csv")
    return df

# 3. Função para extrair todos os dados do endpoint UASG
def extract_uasg(save=True, max_pages=None):
    """
    Extrai UASGs usando paginação da API.
    
    Args:
        save: Se True, salva os dados em CSV.
        max_pages: Número máximo de páginas a extrair (para teste). None = sem limite
    """
    uasg_data = []
    pagina = 1
    tamanho_pagina = 500  # máximo permitido pela API

    logging.info("Iniciando extração de UASGs...")
    
    while True:
        if max_pages and pagina > max_pages:
            logging.info(f"Limite de {max_pages} páginas atingido para teste.")
            break

        params = {
            'statusUasg': 'true',
            'pagina': pagina,
            'tamanhoPagina': tamanho_pagina
        }
        try:
            resultado = extract_data(url, endpoint_uasg, max_records=tamanho_pagina, params=params)
            if resultado:
                uasg_data.extend(resultado)
                logging.info(f"Página {pagina}: {len(resultado)} registros obtidos.")
            else:
                logging.info(f"Página {pagina}: nenhum registro encontrado.")
                break

            # Verifica se chegou à última página
            if len(resultado) < tamanho_pagina:
                logging.info("Última página alcançada.")
                break

            pagina += 1
            time.sleep(1)  # pausa leve para evitar sobrecarga

        except Exception as e:
            logging.warning(f"Erro na página {pagina}: {e}. Tentando novamente em 10s...")
            time.sleep(10)

    logging.info(f"Extração concluída: {len(uasg_data)} UASGs")

    if save:
        return save_to_csv(uasg_data, f"uasg_{datetime.now().strftime('%Y-%m-%d')}.csv")
    return pd.DataFrame(uasg_data)

# 4. Função para extrair dados do endpoint Órgão relacionados
def extract_orgao(save=True, max_pages=None):
    """
    Extrai órgãos específicos com base nos códigos encontrados nos contratos.

    Args:
        save: Se True, salva os dados em CSV
    """
    
    orgaos_data = []
    pagina = 1
    tamanho_pagina = 500 # máximo permitido pela API

    logging.info("Iniciando extração de órgãos...")

    while True:
        if max_pages and pagina > max_pages:
            logging.info(f"Limite de {max_pages} páginas atingido para teste.")
            break
         
        params = {
            'statusOrgao': 'true',
            'pagina': pagina,
            'tamanhoPagina': tamanho_pagina
        }

        try:
            resultado = extract_data(url, endpoint_orgao, max_records=tamanho_pagina, params=params)
            if resultado:
                orgaos_data.extend(resultado)
                logging.info(f"Página {pagina}: {len(resultado)} registros obtidos.")
            else:
                logging.info(f"Página {pagina}: nenhum registro encontrado.")
                break

            # Verifica se chegou à última página
            if len(resultado) < tamanho_pagina:
                logging.info("Última página alcançada.")
                break

            pagina += 1
            time.sleep(1)  # pausa leve para evitar sobrecarga

        except Exception as e:
            logging.warning(f"Erro na página {pagina}: {e}. Tentando novamente em 10s...")
            time.sleep(10)

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
        uasg_df = extract_uasg(max_pages=uasg_max_paginas)

        # 3️⃣ Extração de órgãos
        print()
        logging.info("3. Extraindo dados de órgãos...")
        orgao_df = extract_orgao(max_pages=orgao_max_paginas)

        # 4️⃣ Resumo final
        print()
        logging.info("Extração completa!")
        logging.info(f"Contratos: {len(contratos_df)} registros")
        logging.info(f"UASGs: {len(uasg_df)} registros")
        logging.info(f"Órgãos: {len(orgao_df)} registros")

    except Exception as e:
        logging.critical(f"Erro crítico durante a execução do pipeline: {e}")