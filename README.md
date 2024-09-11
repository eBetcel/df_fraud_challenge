# Project Name: df_fraud_challenge

## Descrição
Esse projeto faz uma ingestão orquestrada por Airflow cujo objetivo é gerar as seguintes tabelas resultado:
- Listar em ordem decrescente as “location_region” por média de “risk score”
- Considerando somente a transação mais recente ("timestamp") com
"transaction_type" igual a "sale" de cada "receiving address", liste os 3
"receiving address" com maior "amount" dentre estas transações (apresente
o "receiving address", o "amount" e o "timestamp").

Durante o processo, os dados são ingeridos de um bucket para outro e é criada uma tabela de Data Quality 

## Sumário
- [Instalação](#Instalação)
- [Uso](#Uso)
- [Contributing](#contributing)
- [Licença](#Licença)

## Instalação
Para executar o projeto é necessário ter instalado o docker e o docker compose no seu ambiente. Siga os seguintes passos:
1. Clone os repositórios: `git clone git@github.com:eBetcel/df_fraud_challenge.git`
2. Crie a imagem docker estendendo a imagem do Airflow: `docker build . --tag extending_airflow:latest`
3. docker compose up -d    

## Uso
1. Crie os buckets correspondentes na AWS e modifique o código onde tem os caminhos "hardcodeds"
2. Crie o conector do Airflow dando permissão para o Athena e o S3

## Licença
Este projeto está sob a [MIT License](LICENSE).
