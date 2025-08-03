from extract import extract_contratos_por_trimestre, extract_uasg, extract_orgao
import time
from datetime import datetime

def main():
    print("=" * 50)
    print(f"INICIANDO PIPELINE ETL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    inicio_total = time.time()
    
    # === FASE DE EXTRAÇÃO ===
    print("\n=== FASE DE EXTRAÇÃO ===")
    inicio_extracao = time.time()
    
    # Extraindo dados de contratos estratificados por trimestre
    print("\n>> Extraindo dados de contratos com amostragem por trimestre...")
    contratos_por_trimestre = 3000  # 3.000 contratos por trimestre (total: 12.000)
    contratos_df = extract_contratos_por_trimestre(contratos_por_trimestre=contratos_por_trimestre, save=True)
    
    # Extraindo todos os dados de UASG e Órgãos
    print("\n>> Extraindo dados de UASGs...")
    uasg_df = extract_uasg(max_records=None, save=True)
    print("\n>> Extraindo dados de Órgãos...")
    orgao_df = extract_orgao(max_records=None, save=True)
    
    fim_extracao = time.time()
    tempo_extracao = round((fim_extracao - inicio_extracao) / 60, 2)
    
    print("\n=== RESUMO DA EXTRAÇÃO ===")
    print(f"Contratos: {len(contratos_df):,} registros (amostra estratificada por trimestre)")
    
    # Exibir distribuição por trimestre para confirmação
    try:
        distribuicao = contratos_df['trimestre'].value_counts().sort_index()
        print("\nDistribuição de contratos por trimestre:")
        for trim, count in distribuicao.items():
            print(f"Trimestre {trim}: {count:,} contratos")
    except:
        print("Não foi possível exibir a distribuição por trimestre.")
    
    print(f"\nColunas: {', '.join(sorted(contratos_df.columns.tolist())[:10])}...")
    
    print(f"\nUASGs: {len(uasg_df):,} registros (todos disponíveis)")
    print(f"Colunas: {', '.join(sorted(uasg_df.columns.tolist())[:10])}...")
    
    print(f"\nÓrgãos: {len(orgao_df):,} registros (todos disponíveis)")
    print(f"Colunas: {', '.join(sorted(orgao_df.columns.tolist())[:10])}...")
    
    print(f"\nTempo de extração: {tempo_extracao} minutos")
    print("\n=== Extração concluída com sucesso! ===")

    """
    # === FASE DE TRANSFORMAÇÃO ===
    print("\n=== FASE DE TRANSFORMAÇÃO ===")
    inicio_transformacao = time.time()
    
    # Adicionar lógica de transformação aqui
    # TODO: Implementar funções de transformação dos dados
    
    fim_transformacao = time.time()
    tempo_transformacao = round((fim_transformacao - inicio_transformacao) / 60, 2)
    print(f"\nTempo de transformação: {tempo_transformacao} minutos")
    print("\n=== Transformação concluída com sucesso! ===")

    # === FASE DE CARREGAMENTO ===
    print("\n=== FASE DE CARREGAMENTO ===")
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
    print(f"PIPELINE ETL CONCLUÍDO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    print(f"Tempo total de processamento: {tempo_total} minutos")
    print(f"- Extração: {tempo_extracao} minutos")
    #print(f"- Transformação: {tempo_transformacao} minutos")
    #print(f"- Carregamento: {tempo_carregamento} minutos")
    print("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nProcesso interrompido pelo usuário.")
    except Exception as e:
        print(f"\n\nErro durante a execução do pipeline: {e}")
        raise