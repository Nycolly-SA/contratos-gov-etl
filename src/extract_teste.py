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
endpoint_fornecedores = "modulo-fornecedor/1_consultarFornecedor"

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

# 3. Função para transformação básica de contratos
def transform_contratos_basico(df):
    """
    Realiza transformações básicas nos contratos para preparar para extração de dados relacionados.
    Foca apenas na limpeza e padronização dos IDs necessários para consultas.
    
    Args:
        df: DataFrame com os dados brutos de contratos
    Returns:
        DataFrame com transformações básicas aplicadas
    """
    contratos_df = df.copy()
    colunas_necessarias = ['codigoUnidadeRealizadoraCompra', 'codigoOrgao', 'niFornecedor']
    colunas_faltantes = [col for col in colunas_necessarias if col not in contratos_df.columns]
    if colunas_faltantes:
        logging.warning(f"Colunas faltando no DataFrame: {', '.join(colunas_faltantes)}")

    # Função de limpeza genérica
    def limpar_coluna(col):
        contratos_df[col] = contratos_df[col].astype(str).str.strip()
        contratos_df[col] = contratos_df[col].where(pd.notna(contratos_df[col]) & (contratos_df[col] != "") & (contratos_df[col] != "nan"))

    for col in ['codigoUnidadeRealizadoraCompra', 'codigoOrgao']:
        if col in contratos_df.columns:
            limpar_coluna(col)

    if 'niFornecedor' in contratos_df.columns:
        contratos_df['niFornecedor'] = contratos_df['niFornecedor'].astype(str).str.strip()
        contratos_df['niFornecedor'] = contratos_df['niFornecedor'].str.replace(r'[^\d]', '', regex=True)
        contratos_df['niFornecedor'] = contratos_df['niFornecedor'].where(pd.notna(contratos_df['niFornecedor']) & (contratos_df['niFornecedor'] != ""))

    # Informações sobre o que foi feito
    registros_originais = len(df)
    registros_finais = len(contratos_df.dropna())
    logging.info(f"Transformação básica concluída: {registros_originais} → {registros_finais} registros válidos")

    return contratos_df

# 4. Função principal para extrair dados relacionados ao endpoint contratos
def extract_dados_relacionados(contratos_df):
    """
    Extrai todos os dados relacionados aos contratos: UASGs, órgãos e fornecedores.
    
    Args:
        contratos_df: DataFrame com os contratos já transformados
    Returns:
        Tupla com DataFrames (uasg_df, orgao_df, fornecedores_df)
    """
    logging.info("Iniciando extração de dados relacionados...")

    if contratos_df is None or contratos_df.empty:
        logging.warning("DataFrame de contratos vazio ou nulo!")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Extração de UASGs relacionadas
    uasg_df = pd.DataFrame()
    if 'codigoUnidadeRealizadoraCompra' in contratos_df.columns:
        codigos_uasg = contratos_df['codigoUnidadeRealizadoraCompra'].dropna().unique().tolist()
        if codigos_uasg:  # verifica se a lista não está vazia
            logging.info(f"Extraindo {len(codigos_uasg)} UASGs únicas")
            uasg_df = extract_uasg_por_codigo(codigos_uasg)
        else:
            logging.info("Nenhum código UASG válido encontrado!")

    # Extração de órgãos relacionados
    orgao_df = pd.DataFrame()
    if 'codigoOrgao' in contratos_df.columns:
        codigos_orgao = contratos_df['codigoOrgao'].dropna().unique().tolist()
        if codigos_orgao:
            logging.info(f"Extraindo {len(codigos_orgao)} órgãos únicos")
            orgao_df = extract_orgao_por_codigo(codigos_orgao)
        else:
            logging.info("Nenhum código de órgão válido encontrado!")

    # Extração de fornecedores relacionados
    fornecedores_df = pd.DataFrame()
    if 'niFornecedor' in contratos_df.columns:
        nis_fornecedores = contratos_df['niFornecedor'].dropna().unique().tolist()
        if nis_fornecedores:
            logging.info(f"Extraindo {len(nis_fornecedores)} fornecedores únicos")
            fornecedores_df = extract_fornecedores_por_id(nis_fornecedores)
        else:
            logging.info("Nenhum identificador de fornecedor válido encontrado!")
    
    return uasg_df, orgao_df, fornecedores_df

# 4.1. Função para extrair dados do endpoint UASG relacionados
def extract_uasg_por_codigo(codigos_uasg, save=True):
    """
    Extrai UASGs usando paginação da API, podendo filtrar por códigos específicos.
    
    Args:
        codigos_uasg: Lista opcional de códigos UASG para filtrar.
        save: Se True, salva os dados em CSV.
    """
    uasg_data = []
    erros = 0
    pagina = 1
    tamanho_pagina = 500  # máximo permitido pela API

    logging.info("Iniciando extração de UASGs...")
    
    # Se tiver filtro por códigos, transformar em string separada por vírgula
    codigos_param = None
    if codigos_uasg:
        codigos_unicos = list(set(codigos_uasg))
        codigos_param = ','.join(map(str, codigos_unicos))
        logging.info(f"Filtrando por {len(codigos_unicos)} códigos UASG.")
    
    while True:
        params = {
            'statusUasg': 'true',
            'pagina': pagina,
            'tamanhoPagina': tamanho_pagina
        }
        if codigos_param:
            params['codigoUasg'] = codigos_param

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
            erros += 1
            logging.warning(f"Erro na página {pagina}: {e}. Tentando novamente em 10s...")
            time.sleep(10)

    logging.info(f"Extração concluída: {len(uasg_data)} UASGs (Erros: {erros})")

    if save:
        return save_to_csv(uasg_data, f"uasg_{datetime.now().strftime('%Y-%m-%d')}.csv")
    return pd.DataFrame(uasg_data)

#4.2. Função para extrair dados do endpoint Órgão relacionados
def extract_orgao_por_codigo(codigos_orgao, save=True):
    """
    Extrai órgãos específicos com base nos códigos encontrados nos contratos.

    Args:
        codigos_orgao: Lista de códigos de órgão para extrair
        save: Se True, salva os dados em CSV
    """
    if not codigos_orgao:
        logging.warning("Lista de códigos de órgãos vazia!")
        return pd.DataFrame()
    
    orgaos_data = []
    codigos_unicos = list(set(codigos_orgao))
    erros = 0

    logging.info(f"Extraindo dados de {len(codigos_unicos)} órgãos...")

    for i, codigo in enumerate(codigos_unicos, 1):
        if i % 50 == 0:
            logging.info(f"Progresso: {i}/{len(codigos_unicos)} órgãos consultados.")

        params = {
            'codigoOrgao': codigo,
            'statusOrgao': 'true',
        }

        if 'statusOrgao' not in params:
            raise ValueError("O parâmetro 'statusOrgao' é obrigatório para o endpoint de Órgão.")

        try:
            resultado = extract_data(url, endpoint_orgao, max_records=1, params=params)
            if resultado:
                orgaos_data.extend(resultado)
            else:
                logging.info(f"Nenhum dado encontrado para o órgão {codigo}")
            time.sleep(1)
        except Exception as e:
            erros += 1
            logging.warning(f"Erro ao consultar órgão {codigo}: {e}")

    logging.info(f"Extração concluída: {len(orgaos_data)} órgãos (Erros: {erros})")

    if save:
        return save_to_csv(orgaos_data, f"orgao_{datetime.now().strftime('%Y-%m-%d')}.csv")
    return pd.DataFrame(orgaos_data)

# 4.3 Função para extrair dados do endpoint Fornecedores relacionados
def extract_fornecedores_por_id(ni_fornecedores, save=True):
    """
    Extrai fornecedores específicos com base nos NIs encontrados nos contratos.
    Separa CPF de CNPJ baseado no tamanho do identificador.
    
    Args:
        ni_fornecedores: Lista de NIs (CPF/CNPJ) para extrair
        save: Se True, salva os dados em CSV
    """
    if not ni_fornecedores:
        logging.warning("Lista de identificadores de fornecedores vazia!")
        return pd.DataFrame()

    fornecedores_data = []
    nis_unicos = list(set(ni_fornecedores))
    cpfs, cnpjs, invalidos = [], [], []

    # Separação de CPF/CNPJ
    for ni in nis_unicos:
       ni_str = str(ni).strip()
       if not ni_str or ni_str == 'nan':
           invalidos.append(ni_str)
       elif len(ni_str) <= 11:
           cpfs.append(ni_str)
       else:
           cnpjs.append(ni_str)

    logging.info(f"Fornecedores: {len(cpfs)} CPFs, {len(cnpjs)} CNPJs, {len(invalidos)} inválidos")

    # Processamento de CPFs
    erros_cpf = 0
    for i, cpf in enumerate(cpfs):
        if i % 50 == 0:
            logging.info(f"Progresso: {i}/{len(cpfs)} CPFs consultados.")

        try:
            resultado = extract_data(url, endpoint_fornecedores, max_records=1, params={"cpf": cpf, "ativo": "true"})
            if resultado:
                fornecedores_data.extend(resultado)
            time.sleep(1)
        except Exception as e:
            erros_cpf += 1
            logging.warning(f"Erro ao consultar fornecedor CPF {cpf}: {e}")

    # Processamento de CNPJs
    erros_cnpj = 0
    for i, cnpj in enumerate(cnpjs):
        if i % 50 == 0:
            logging.info(f"Progresso: {i}/{len(cnpjs)} CNPJs consultados.")

        try:
            resultado = extract_data(url, endpoint_fornecedores, max_records=1, params={"cnpj": cnpj, "ativo": "true"})
            if resultado:
                fornecedores_data.extend(resultado)
            time.sleep(1)
        except Exception as e:
            erros_cnpj += 1
            logging.warning(f"Erro ao consultar fornecedor CNPJ {cnpj}: {e}")

    logging.info(f"Extração de fornecedores concluída: {len(fornecedores_data)} registros (Erros CPF: {erros_cpf}, Erros CNPJ: {erros_cnpj})")

    if save:
        return save_to_csv(fornecedores_data, f"fornecedores_{datetime.now().strftime('%Y-%m-%d')}.csv")
    return pd.DataFrame(fornecedores_data)

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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S")

    try:
        contratos_por_trimestre = 500
        logging.info("MODO TESTE: Usando amostra reduzida de dados")

        # 1️⃣ Extração de contratos
        logging.info("1. Extraindo dados de contratos...")
        contratos_df = extract_contratos_por_trimestre(url, endpoint_contratos, contratos_por_trimestre=contratos_por_trimestre
        )

        if contratos_df.empty:
            logging.error("Nenhum contrato extraído. O pipeline será interrompido.")
        else:
            # 2️⃣ Transformação básica
            logging.info("2. Realizando transformação básica nos contratos...")
            contratos_limpo_df = transform_contratos_basico(contratos_df)

            if contratos_limpo_df.empty:
                logging.error("Transformação resultou em DataFrame vazio. O pipeline será interrompido.")
            else:
                # 3️⃣ Extração de dados relacionados
                logging.info("3. Extraindo dados relacionados com base nos contratos...")
                uasg_df, orgao_df, fornecedores_df = extract_dados_relacionados(contratos_limpo_df)

                # 4️⃣ Resumo final
                logging.info("Extração completa!")
                logging.info(f"Contratos: {len(contratos_df)} registros")
                logging.info(f"Contratos após limpeza: {len(contratos_limpo_df)} registros")
                logging.info(f"UASGs relacionadas: {len(uasg_df)} registros")
                logging.info(f"Órgãos relacionados: {len(orgao_df)} registros")
                logging.info(f"Fornecedores relacionados: {len(fornecedores_df)} registros")

    except Exception as e:
        logging.critical(f"Erro crítico durante a execução do pipeline: {e}")