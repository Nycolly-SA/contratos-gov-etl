# Pipeline ETL - ComprasNet (ComprasGov)

Este projeto implementa um **pipeline ETL (Extract, Transform, Load)** para coleta, tratamento e análise de dados públicos do portal [ComprasGov](https://dadosabertos.compras.gov.br/).  

O objetivo é simular um fluxo de dados real, extraindo contratos e seus itens, transformando-os e carregando-os em um banco de dados, para posterior análise em dashboards interativos (via **Streamlit**).

---

## 📌 Estrutura do Projeto


---

## 🚀 Pipeline ETL

### 1. **Extract**  
A etapa de extração utiliza a **API do ComprasGov**.  
- Extração dos **contratos por trimestre**, com filtros de data configuráveis.  
- Limite de **10.000 contratos por extração** (restrição da API).  
- Para cada contrato, são coletados também os **itens** relacionados.  
- Contratos com **mais de 500 itens** são descartados para evitar truncamento de dados.  
- Contratos **sem itens** também são descartados.  

Endpoints utilizados:  
- [`consultarContratos`](https://dadosabertos.compras.gov.br/swagger-ui/index.html#/Contratos/consultarContratos)  
- [`consultarContratosItens`](https://dadosabertos.compras.gov.br/swagger-ui/index.html#/Contratos/consultarContratosItens)  
- [`uasg`](https://dadosabertos.compras.gov.br/swagger-ui/index.html#/UASG/uasg)  
- [`orgao`](https://dadosabertos.compras.gov.br/swagger-ui/index.html#/Orgao/orgao)  

> 🔧 Os parâmetros de ano e quantidade por trimestre podem ser ajustados conforme a necessidade.

---

### 2. **Transform** *(em desenvolvimento)*  
- Limpeza e padronização de colunas (datas, valores monetários, textos).  
- Criação de tabelas dimensionais para facilitar análises.  

---

### 3. **Load** *(em desenvolvimento)*  
- Carregamento em banco de dados relacional (PostgreSQL).  
- Organização em modelo relacional para consultas analíticas.  

---

## 📊 Futuras Análises

Após a conclusão das etapas de transformação e carga, os dados serão utilizados em dashboards interativos no **Streamlit**, com KPIs voltados para:  
- Custos por órgão/entidade.  
- Volume de contratos por período.  
- Distribuição de itens contratados.  
- Comparação entre UASGs.  

---

## ⚠️ Observações Importantes

- A API do ComprasGov sofre atualizações constantes:  
  - Alguns registros são **excluídos ou adicionados** ao longo do tempo.  
  - Isso pode fazer com que o total de registros extraídos varie.  
- Esses pontos estão documentados para garantir **transparência** na reprodução do pipeline.  

---

## 🔧 Tecnologias Utilizadas

- **Python 3.12+**  
- **Pandas**  
- **Requests**  
- **PostgreSQL** *(planejado)*  
- **Streamlit** *(planejado)*  

---

## 📌 Próximos Passos

- Finalizar a etapa de transformação.  
- Implementar carga no banco de dados.  
- Criar dashboard em Streamlit para análises.  

---
