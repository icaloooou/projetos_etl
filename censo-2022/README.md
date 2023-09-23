## Projeto ETL - python
Esse projeto teve objetivo fazer um processo de ETL (extract, transform e load) de algum dado na internet.

#### Dado escolhido
Para trabalhar nesse etl, escolhi os dados do Censo Demográfico 2022.
    - Primeiro pegamos os dados de pessoas e depois de domicílios e no final juntamos as duas informações em um único arquivo .csv

#### Como rodar:
1. Esse projeto contém um **requirements.txt** que baixa as bibliotecas necessárias para execução do script.
2. Também contém nesse projeto uma configuração para desabilitar "unsafe legacy renegotiation", na pasta resources.
3. Por fim, basta chamar o script etl_censo2022.py