import pandas as pd
import time
from pathlib import Path
from datetime import datetime
import requests

url = 'https://dadosabertos.compras.gov.br/modulo-contratos/'
endpoint_contratos = '1_consultarContratos'
endpoint_contratos_itens = '2_consultarContratosItem'

# Criação de diretórios para salvar os dados (se não existirem)
data_dir = Path('data/raw')
data_dir.mkdir(parents=True, exist_ok=True)

default_params = {
    'tamanhoPagina': 500, #min é 10; max é 500
    'dataVigenciaInicialMin': '2024-01-01',
    'dataVigenciaInicialMax': '2024-12-31',
}

# Função geral para extrair dados de um endpoint específico da API
def extract_data(url, endpoint, max_records=10000, params=None, max_retries=3):
    # Extrai dados de um endpoint até atingir o max_records
    """
    Args:
        url: URL base da API
        endpoint: Endpoint específico a ser consultado
        max_records: Número máximo de registros a serem extraídos
        params: Parâmetros da consulta (opcional). Se None, usa default_params.
    """
    # Se nenhum parâmetro for fornecido, usa os parâmetros padrão
    if params is None:
        params = default_params.copy()

    # Verifica se os parâmetros obrigatórios estão presentes
    if 'dataVigenciaInicialMin' not in params and 'dataVigenciaInicialMax' not in params:
        raise ValueError("Os parâmetros 'dataVigenciaInicialMin' e 'dataVigenciaInicialMax' são obrigatórios.")

    all_data = []
    total_records = 0
    max_pages = (max_records // params['tamanhoPagina']) + 1 # Quantas páginas serão necessárias para atingir o max_records
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
                response = requests.get(url + endpoint, params=params_copy, timeout=15)
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
                if records_count < params_copy['tamanhoPagina']:
                    print("última página alcançada.")
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
    # Salva os dados extraídos em um arquivo CSV e retorna o DataFrame
    """
    Args:
        data: Lista de dicionários com os dados a serem salvos
        filename: Nome do arquivo CSV (sem extensão)
        directory: Diretório onde o arquivo será salvo
    """
    if not data:
        print("Nenhum dado para salvar em {filename}")
        return pd.DataFrame()
    df = pd.DataFrame(data)
    file_path = directory / filename
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"Dados salvos em {file_path} ({len(df)} registros)")
    return df

def extract_contratos(max_records=10000, save=True):
    # Extrai dados de contratos e retorna um DataFrame
    timestamp = datetime.now().strftime('%Y-%m-%d')
    contratos_data = extract_data(url, endpoint_contratos, max_records)

    if save:
        return save_to_csv(contratos_data, f"contratos_{timestamp}.csv")
    return pd.DataFrame(contratos_data)

def extract_contratos_itens(max_records=10000, save=True):
    # Extrai dados de itens de contratos e retorna um DataFrame
    timestamp = datetime.now().strftime('%Y-%m-%d')
    contratos_itens_data = extract_data(url, endpoint_contratos_itens, max_records)

    if save:
        return save_to_csv(contratos_itens_data, f"contratos_itens_{timestamp}.csv")
    return pd.DataFrame(contratos_itens_data)

# Execução direta para testes
if __name__ == "__main__":
    # Teste com número reduzido de registros
    contratos_df = extract_contratos(max_records=1000)
    itens_df = extract_contratos_itens(max_records=1000)

    print("\nResumo da extração de teste:")
    print(f"Contratos: {len(contratos_df):,} registros")
    print(f"Itens de Contratos: {len(itens_df):,} registros")