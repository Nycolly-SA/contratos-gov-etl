import pandas as pd
import time
from pathlib import Path
from datetime import datetime
import requests

url = 'https://dadosabertos.compras.gov.br/'
# Endpoints para extração de dados
endpoint_contratos = 'modulo-contratos/1_consultarContratos'
endpoint_uasg = 'modulo-uasg/1_consultarUasg'
endpoint_orgao = 'modulo-uasg/2_consultarOrgao'

# Criação de diretórios para salvar os dados (se não existirem)
data_dir = Path('data/raw')
data_dir.mkdir(parents=True, exist_ok=True)

# Função geral para extrair dados de um endpoint específico da API
def extract_data(url, endpoint, max_records=10000, params=None, max_retries=3):
    # Extrai dados de um endpoint até atingir o max_records
    """
    Args:
        url: URL base da API
        endpoint: Endpoint específico a ser consultado
        max_records: Número máximo de registros a serem extraídos
        params: Parâmetros da consulta (opcional). Se None, usa parâmetros padrão.
        max_retries: Número máximo de tentativas em caso de falha
    Returns:
        Lista de dicionários com os dados extraídos
    """

     # Se nenhum parâmetro for fornecido, inicializa um dicionário vazio
    if params is None:
        params = {}
    
    # Define página inicial se não estiver presente
    if 'pagina' not in params:
        params['pagina'] = 1
    
    # Define a paginação com base no endpoint
    if endpoint == endpoint_contratos:
        # Para o endpoint de contratos, usa tamanhoPagina
        if 'tamanhoPagina' not in params:
            params['tamanhoPagina'] = 500  # Valor padrão para tamanhoPagina
        page_size = params['tamanhoPagina']
    else:
        # Para outros endpoints que não usam tamanhoPagina
        # Define um valor fixo apenas para controle do loop
        page_size = 100  # Valor aproximado para controle interno

    # Se nenhum parâmetro for fornecido, usa parâmetros padrão básicos
    if params is None:
        params = {'tamanhoPagina': 500, 'pagina': 1}
    
    # Verifica se tamanhoPagina está presente (obrigatório para cálculo de páginas)
    if 'tamanhoPagina' not in params:
        params['tamanhoPagina'] = 500  # Valor padrão para tamanhoPagina
    
    all_data = []
    total_records = 0
    max_pages = (max_records // page_size) + 1 # Quantas páginas serão necessárias para atingir o max_records
    # OBS sobre o +1: caso o resultado da divisão seja não exata, será pego mais uma página para garantir que todos os registros foram pegos

    print(f"Iniciando extração de {endpoint}...")

    # Usando for com range para controlar páginas
    for current_page in range(1, max_pages + 1):
        params_copy = params.copy()
        params_copy['pagina'] = current_page

        # Tentativas com retry em caso de folha
        for retry in range(max_retries):
            try:
                print(f"Extraindo página {current_page}/{max_pages}...")
                response = requests.get(url + endpoint, params=params_copy, timeout=20)
                response.raise_for_status()  # Levanta um erro para códigos de status HTTP 4xx/5xx
                data = response.json()

                # Verificar se há resultados
                results = data.get('resultado', [])
                if not results:
                    print("Não há mais dados disponíveis.")
                    return all_data
                
                # Adicionar resultados ao conjunto total
                all_data.extend(results)
                records_count = len(results)
                total_records += records_count
                print(f"{records_count} registros obtidos (Total: {total_records})")

                # Verificar se chegamos ao fim dos dados (página não completa)
                # Para contratos, verifica com base no tamanhoPagina
                if endpoint == endpoint_contratos and records_count < params_copy['tamanhoPagina']:
                    print("Última página alcançada.")
                    return all_data
                # Para outros endpoints, verifica se recebemos menos que a estimativa
                elif endpoint != endpoint_contratos and records_count < page_size:
                    print("Última página alcançada.")
                    return all_data
                
                # Pausa curta para evitar sobrecarga na API
                time.sleep(0.5)
                break
            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 5 # Aumenta o tempo de espera a cada tentativa
                    print(f"Erro na requisição: {e}. Tentando novamente em {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"Falha após {max_retries} tentativas: {e}")
                    return all_data # Retorna o que foi possível extrair
        if total_records >= max_records:
            print(f"Limite de {max_records} registros atingido.")
            break

    print(f"Extração concluída: {total_records} registros obtidos.")
    return all_data

def save_to_csv(data, filename, directory=data_dir):
    """
    Salva os dados em um arquivo CSV e retorna o DataFrame
    
    Args:
        data: Lista de dicionários ou DataFrame a ser salvo
        filename: Nome do arquivo CSV (sem extensão)
        directory: Diretório onde o arquivo será salvo
    """
    # Verifica se os dados já estão em um DataFrame ou se precisam ser convertidos
    if isinstance(data, pd.DataFrame):
        df = data
    else:
        # Se for uma lista vazia ou None
        if not data:
            print(f"Nenhum dado para salvar em {filename}")
            return pd.DataFrame()
        df = pd.DataFrame(data)
    
    file_path = directory / filename
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"Dados salvos em {file_path} ({len(df)} registros)")
    return df

def extract_contratos_simples(max_records=10000, save=True):
    """
    Extrai dados de contratos sem estratificação temporal.
    
    Nota: Esta função fornece uma extração simples, mas no fluxo principal
    utilizamos a função extract_contratos_por_trimestre() para garantir 
    representatividade temporal dos dados.

    Args:
        max_records: Número máximo de registros a extrair
        save: Se True, salva os dados em CSV
    Returns:
        DataFrame com os contratos extraídos
    """
    
    params = {
    'tamanhoPagina': 500, #min é 10; max é 500
    'dataVigenciaInicialMin': '2024-01-01',
    'dataVigenciaInicialMax': '2024-12-31',
    }

    # Verifica se os parâmetros obrigatórios estão presentes para o endpoint de contratos
    if 'dataVigenciaInicialMin' not in params or 'dataVigenciaInicialMax' not in params:
        raise ValueError("Os parâmetros 'dataVigenciaInicialMin' e 'dataVigenciaInicialMax' são obrigatórios para o endpoint de contratos.")
    
    # Extrai dados de contratos e retorna um DataFrame
    timestamp = datetime.now().strftime('%Y-%m-%d')
    contratos_data = extract_data(url, endpoint_contratos, max_records, params=params)

    if save:
        return save_to_csv(contratos_data, f"contratos_{timestamp}.csv")
    return pd.DataFrame(contratos_data)

def extract_contratos_por_trimestre(ano=2024, contratos_por_trimestre=3000, save=True):
    """
    Extrai uma quantidade específica de contratos para cada trimestre do ano.
    Estratégia de amostragem temporal para manter representatividade.
    
    Args:
        ano: Ano de referência
        contratos_por_trimestre: Quantidade de contratos a extrair por trimestre
        save: Se True, salva os dados em CSV
    Returns:
        DataFrame com os contratos extraídos
    """
    todos_contratos = []
    
    # Define os trimestres
    trimestres = [
        ("01-01", "03-31", "T1"),  # 1º trimestre
        ("04-01", "06-30", "T2"),  # 2º trimestre
        ("07-01", "09-30", "T3"),  # 3º trimestre
        ("10-01", "12-31", "T4")   # 4º trimestre
    ]
    
    for inicio_dia, fim_dia, nome_trim in trimestres:
        data_inicio = f"{ano}-{inicio_dia}"
        data_fim = f"{ano}-{fim_dia}"
        
        print(f"Extraindo contratos do {nome_trim}/{ano} ({data_inicio} a {data_fim})...")
        
        params = {
            'tamanhoPagina': 500,
            'dataVigenciaInicialMin': data_inicio,
            'dataVigenciaInicialMax': data_fim,
        }
        
        contratos_trimestre = extract_data(url, endpoint_contratos, max_records=contratos_por_trimestre, params=params)
        
        todos_contratos.extend(contratos_trimestre)
        print(f"Extraídos {len(contratos_trimestre)} contratos do {nome_trim}/{ano}")
    
    # Converter para DataFrame aqui, antes do if save:
    df = pd.DataFrame(todos_contratos)
    
    # Verificar distribuição por trimestre
    if len(df) > 0:
        try:
            df['data_inicio'] = pd.to_datetime(df['dataVigenciaInicial'])
            df['trimestre'] = df['data_inicio'].dt.quarter
            
            contagem = df['trimestre'].value_counts().sort_index()
            
            print("\nVerificação da distribuição por trimestre:")
            for trim, count in contagem.items():
                print(f"Trimestre {trim}: {count} contratos")
        except Exception as e:
            print(f"Não foi possível verificar a distribuição: {e}")
    
    # Usar o DataFrame já criado para salvar e retornar
    timestamp = datetime.now().strftime('%Y-%m-%d')
    if save:
        return save_to_csv(df, f"contratos_amostra_{timestamp}.csv")
    return df

def extract_uasg(max_records=None, save=True):
    """
    Extrai todos os dados de UASGs.
    
    Args:
        max_records: Limite máximo de registros (None para todos os registros)
        save: Se True, salva os dados em CSV
    """
    params = {
    'statusUasg': 'true',
    }

    # Verifica se os parâmetros obrigatórios estão presentes para o endpoint de UASG
    if 'statusUasg' not in params:
        raise ValueError("O parâmetro 'statusUasg' é obrigatório para o endpoint de UASG.")

    # Define um limite muito alto para pegar todos os registros (caso max_records=None)
    if max_records is None:
        max_records = 1000000  # Um número muito alto para capturar todos os registros
        
    # Extrai dados de Unidades Administrativas de Serviços Gerais (UASG)
    timestamp = datetime.now().strftime('%Y-%m-%d')
    uasg_data = extract_data(url, endpoint_uasg, max_records, params=params)
    
    if save:
        return save_to_csv(uasg_data, f"uasg_{timestamp}.csv")
    return pd.DataFrame(uasg_data)

def extract_orgao(max_records=None, save=True):
    """
    Extrai todos os dados de Órgãos.
    
    Args:
        max_records: Limite máximo de registros (None para todos os registros)
        save: Se True, salva os dados em CSV
    """
    params = {
    'statusOrgao': 'true',
    }

    # Verifica se os parâmetros obrigatórios estão presentes para o endpoint de Órgão
    if 'statusOrgao' not in params:
        raise ValueError("O parâmetro 'statusOrgao' é obrigatório para o endpoint de Órgão.")
        
    # Define um limite muito alto para pegar todos os registros (caso max_records=None)
    if max_records is None:
        max_records = 1000000  # Um número muito alto para capturar todos os registros

    # Extrai dados de Órgãos
    timestamp = datetime.now().strftime('%Y-%m-%d')
    orgao_data = extract_data(url, endpoint_orgao, max_records, params=params)
    
    if save:
        return save_to_csv(orgao_data, f"orgao_{timestamp}.csv")
    return pd.DataFrame(orgao_data)

# Execução direta para testes
if __name__ == "__main__":
    # Amostra reduzida para teste
    limite_uasg = 500
    limite_orgao = 500
    contratos_por_trimestre = 100
    print("MODO TESTE: Usando amostra reduzida de dados")

    print("Iniciando extração de dados...")
    
    # Extração dos dados
    uasg_df = extract_uasg(max_records=limite_uasg)
    orgao_df = extract_orgao(max_records=limite_orgao)
    contratos_df = extract_contratos_por_trimestre(contratos_por_trimestre=contratos_por_trimestre)
    
    print("\nExtração concluída!")
    print(f"Contratos: {len(contratos_df)} registros")
    print(f"UASGs: {len(uasg_df)} registros")
    print(f"Órgãos: {len(orgao_df)} registros")