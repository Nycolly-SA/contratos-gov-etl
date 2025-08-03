# Pipeline ETL para Contratos Governamentais

Este projeto implementa um pipeline ETL (Extract, Transform, Load) para dados de contratos públicos disponibilizados pelo Portal de Compras Governamentais do Brasil.

## 📊 Sobre o Projeto

O objetivo deste projeto é extrair, transformar e analisar dados de contratos públicos, proporcionando insights sobre gastos governamentais, distribuição de contratos entre órgãos e análises temporais.

## 🔍 Fase de Extração

### Fontes de Dados
Os dados são extraídos da API de Dados Abertos do Portal de Compras Governamentais através de três endpoints principais:
- **Contratos**: Informações detalhadas sobre contratos públicos
- **UASGs (Unidades Administrativas de Serviços Gerais)**: Unidades operacionais
- **Órgãos**: Entidades governamentais responsáveis pelos contratos

### Estratégia de Amostragem
Para lidar com o grande volume de contratos disponíveis (mais de 170 mil registros apenas em 2024), implementei uma estratégia de **amostragem estratificada por trimestre**:

1. **Representatividade Temporal**: Extraí 3.000 contratos de cada trimestre (total de 12.000), garantindo representatividade ao longo do ano
2. **Cobertura Completa**: Para UASGs e órgãos, extraí conjuntos completos de dados

### Estrutura da Extração

```
src/
  ├── extract.py      # Módulo de extração
  ├── main.py         # Pipeline principal
  └── ...
data/
  └── raw/            # Dados brutos extraídos da API
```

### Detalhes de Implementação

- **Paginação**: Implementei um sistema robusto para lidar com a paginação da API (500 registros por página)
- **Tratamento de Erros**: Desenvolvi um mecanismo de retry para lidar com falhas temporárias da API
- **Verificação de Distribuição**: Adicionei validação automática da distribuição de contratos por trimestre

## 🛠️ Como Executar

```bash
# Clonar o repositório
git clone https://github.com/Nycolly-SA/contratos-gov-etl.git
cd contratos-gov-etl

# Instalar dependências
pip install -r requirements.txt

# Executar o pipeline
python src/main.py
```

## 📁 Estrutura dos Dados

A extração gera três arquivos CSV principais:
- `contratos_amostra_YYYY-MM-DD.csv`: Contratos estratificados por trimestre
- `uasg_YYYY-MM-DD.csv`: Dados completos de UASGs
- `orgao_YYYY-MM-DD.csv`: Dados completos de órgãos

## 🔜 Próximas Etapas

- Implementação das fases de transformação e carregamento
- Análises e visualizações dos dados
- Dashboard interativo para exploração dos dados