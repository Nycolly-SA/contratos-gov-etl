import pandas as pd
from pathlib import Path
from datetime import datetime

processed_dir = Path('data/processed')
processed_dir.mkdir(parents=True, exist_ok=True)

def save_processed_data(df, filename):
    """
    Salva o DataFrame processado em um arquivo CSV.
    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        filename (str): Nome do arquivo CSV (sem extensão)

    Returns:
        Path do arquivo salvo.
    """

    timestamp = datetime.now().strftime('%Y-%m-%d')
    file_path = processed_dir / f"{filename}_{timestamp}.csv"

    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"Dados processados salvos em: {file_path} ({len(df)} registros)")

    return file_path


def transform_contratos(df_contratos, save=True):
    # Copia do DataFrame para evitar modificar o original
    df = df_contratos.copy()

    if save:
        save_processed_data(df, 'contratos_limpos')
    return df

def transform_uasg(df_uasg, save=True):
    # Copia do DataFrame para evitar modificar o original
    df = df_uasg.copy()

    if save:
        save_processed_data(df, 'uasg_limpos')
    return df

def transform_orgao(df_orgao, save=True):
    # Copia do DataFrame para evitar modificar o original
    df = df_orgao.copy()

    if save:
        save_processed_data(df, 'orgao_limpos')
    return df