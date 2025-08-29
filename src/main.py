from extract import extract_contratos_por_trimestre, extract_uasg, extract_orgao
from extract import url, endpoint_contratos, endpoint_uasg, endpoint_orgao
from transform import transform_contratos, transform_uasg, transform_orgao
import time
from datetime import datetime
import logging

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-6s | %(message)s",
    datefmt="%H:%M:%S"
)

def main():
    print("=" * 50)
    logging.info(f"INICIANDO PIPELINE ETL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    inicio_total = time.time()
    
    # === FASE DE EXTRAÇÃO ===
    print()
    logging.info("=== FASE DE EXTRAÇÃO ===")
    inicio_extracao = time.time()
    
    # Extraindo dados de contratos estratificados por trimestre
    logging.info(">>> Extraindo dados de contratos com amostragem por trimestre...")
    contratos_por_trimestre = 5000 # Máximo de contratos a extrair por trimestre (configurável)
    ano = datetime.now().year - 1  # Ano de referência para extração -> ano anterior (configurável)
    logging.info(f">> Iniciando extração: {contratos_por_trimestre} contratos por trimestre do ano {ano}")
    df_contratos = extract_contratos_por_trimestre(url=url, endpoint_contratos=endpoint_contratos, contratos_por_trimestre=contratos_por_trimestre, save=True, ano=ano)

    # Extraindo todos os dados de UASG e Órgãos
    print()
    logging.info(">>> Extraindo todos os dados de UASGs...")
    df_uasg = extract_uasg(url=url, endpoint_uasg=endpoint_uasg, save=True)
    print()
    logging.info(">>> Extraindo todos os dados de Órgãos...")
    df_orgao = extract_orgao(url=url, endpoint_orgao=endpoint_orgao, save=True)

    fim_extracao = time.time()
    tempo_extracao = round((fim_extracao - inicio_extracao) / 60, 2)

    print()
    logging.info("=== RESUMO DA EXTRAÇÃO ===")
    logging.info(f"Contratos: {len(df_contratos):,} registros (amostra estratificada por trimestre)")

    # Exibir distribuição por trimestre para confirmação
    try:
        distribuicao = df_contratos['trimestre'].value_counts().sort_index()
        print()
        logging.info("Distribuição de contratos por trimestre:")
        for trim, count in distribuicao.items():
            logging.info(f"Trimestre {trim}: {count:,} contratos")
    except:
        logging.warning("Não foi possível exibir a distribuição por trimestre.")

    logging.info(f"Colunas: {', '.join(sorted(df_contratos.columns.tolist())[:10])}...")

    logging.info(f"UASGs: {len(df_uasg):,} registros (todos disponíveis)")
    logging.info(f"Colunas: {', '.join(sorted(df_uasg.columns.tolist())[:10])}...")

    logging.info(f"Órgãos: {len(df_orgao):,} registros (todos disponíveis)")
    logging.info(f"Colunas: {', '.join(sorted(df_orgao.columns.tolist())[:10])}...")

    print()
    logging.info(f"Tempo de extração: {tempo_extracao} minutos")
    logging.info("=== Extração concluída com sucesso! ===")

    # === FASE DE TRANSFORMAÇÃO ===
    print()
    logging.info("=== FASE DE TRANSFORMAÇÃO ===")
    inicio_transformacao = time.time()




    df_contratos_limpo = transform_contratos(df_contratos)
    #df_uasg_limpo = transform_uasg(df_uasg)
    #df_orgao_limpo = transform_orgao(df_orgao)




    fim_transformacao = time.time()
    tempo_transformacao = round((fim_transformacao - inicio_transformacao) / 60, 2)
    logging.info(f"Tempo de transformação: {tempo_transformacao} minutos")
    logging.info("=== Transformação concluída com sucesso! ===")


    """
    # === FASE DE CARREGAMENTO ===
    print()
    logging.info("=== FASE DE CARREGAMENTO ===")
    inicio_carga = time.time()
    
    # Adicionar lógica de carregamento aqui
    # TODO: Implementar funções de carregamento dos dados

    fim_carregamento = time.time()
    tempo_carregamento = round((fim_carregamento - inicio_carregamento) / 60, 2)
    print(f"\nTempo de carregamento: {tempo_carregamento} minutos")
    print("\n=== Carregamento concluído com sucesso! ===")
    """

    # === RESUMO FINAL ===
    fim_total = time.time()
    tempo_total = round((fim_total - inicio_total) / 60, 2)
    
    print("\n" + "=" * 50)
    logging.info(f"PIPELINE ETL CONCLUÍDO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 50)
    logging.info(f"Tempo total de processamento: {tempo_total} minutos")
    logging.info(f"- Extração: {tempo_extracao} minutos")
    #logging.info(f"- Transformação: {tempo_transformacao} minutos")
    #logging.info(f"- Carregamento: {tempo_carregamento} minutos")
    logging.info("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        logging.info("Processo interrompido pelo usuário.")
    except Exception as e:
        print()
        logging.error(f"Erro durante a execução do pipeline: {e}")
        raise