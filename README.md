# ETL Pipeline para Dados da B3

Este repositório contém um pipeline de ETL desenvolvido para processar e transformar dados financeiros diários e anuais da B3, utilizando o Apache Airflow e PostgreSQL. O sistema gerencia dados de cotações de ações, integrando informações para criação de um modelo estrela (star schema), permitindo consultas e análises estruturadas.

## Funcionalidades

- **Processamento de Arquivos**: Automatiza a ingestão e o processamento de arquivos de cotações, identificando e extraindo registros relevantes.
- **Transformação e Carga de Dados**: Converte os dados e carrega informações em tabelas de dimensões e fatos, organizadas em um modelo estrela para facilitar análises e consultas.
- **Criação de Tabelas e Estrutura de Dados**: Implementa as tabelas `dim_data`, `dim_empresa`, `fato_cotacao`, e `stage_cotacao` no PostgreSQL, com mapeamento de chaves primárias e estrangeiras.
- **DAGs do Apache Airflow**: Define e gerencia fluxos de trabalho (DAGs) para garantir a execução ordenada de processos, incluindo criação de tabelas, transformação, carga e limpeza dos dados de stage.

## Estrutura do Projeto

- **DAGs**:
  - `base_anual.py`: Gerencia o processamento e carregamento dos dados anuais.
  - `base_diaria.py`: Gerencia o processamento diário dos dados, com atualizações periódicas programadas.
- **Scripts de Processamento**:
  - `processar_cotahist.py`: Processa arquivos de cotações e insere dados na tabela de stage.
  - `transformar_e_carregar.py`: Realiza as transformações e carga dos dados finais nas tabelas de dimensão e fato.
- **Tabelas**:
  - Scripts para criação das tabelas `dim_data`, `dim_empresa`, `fato_cotacao` e `stage_cotacao`, garantindo a integridade referencial e suporte para análise histórica.

## Pré-requisitos

- **Apache Airflow**
- **PostgreSQL**
- **Python 3.x**

## Como Executar

1. Configure o Airflow com as DAGs e scripts fornecidos.
2. Execute as DAGs para iniciar o pipeline de processamento.
3. Conecte-se ao PostgreSQL para acessar os dados processados e realizar consultas.

Este pipeline foi projetado para automatizar a integração de dados financeiros, possibilitando análises detalhadas sobre o histórico de cotações.