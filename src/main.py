from extract import extract_contratos, extract_contratos_itens

def main():
    print("=== FASE DE EXTRAÇÃO ===")
    contratos_df = extract_contratos(max_records=10000)
    contratos_itens_df = extract_contratos_itens(max_records=10000)

    # Verificando os dados extraídos
    print(f"\nContratos: {len(contratos_df)} registros")
    print(f"Colunas: {contratos_df.columns.tolist()}")
    print(f"Itens de Contratos: {len(contratos_itens_df)} registros")
    print(f"Colunas: {contratos_itens_df.columns.tolist()}")
    print("\n=== Extração concluída com sucesso! ===")

    print("\n=== FASE DE TRANSFORMAÇÃO ===")
    # Adicionar lógica de transformação aqui
    print("\n=== Transformação concluída com sucesso! ===")

    print("\n=== FASE DE CARGA ===")
    # Adicionar lógica de carga aqui
    print("\n=== Carga concluída com sucesso! ===")

    print("\n=== Processo ETL concluído com sucesso! ===")


if __name__ == "__main__":
    main()