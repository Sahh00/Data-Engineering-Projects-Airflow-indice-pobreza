✅ LakeHouse ✅

[Untitled.pdf](https://github.com/user-attachments/files/22391162/Untitled.pdf)


🧠 Introdução
Nos últimos dias, mergulhei em um desafio prático de Data Engineering: construir um processo completo de ETL local utilizando Airflow, Spark e MinIO (rodando em Docker) como meu Data Lake.
O projeto trabalha com dados do Índice de Pobreza no Brasil (2012-2022), extraídos diretamente de uma API do Kaggle. Para isso, desenvolvi um operator personalizado no Airflow responsável por realizar a extração e enviar os dados para a camada Bronze do Data Lake.
Um dos maiores aprendizados foi configurar a conexão do Spark com o MinIO via S3 e trabalhar com Delta Lake, garantindo que os dados fossem armazenados em formato Delta de maneira eficiente. Essa etapa foi desafiadora, mas extremamente enriquecedora.

Na fase de transformação, explorei o PySpark de duas maneiras:

 - SQL com Spark
 - APIs do PySpark

Após testar os pipelines em notebooks, converti tudo em scripts Python e integrei no Airflow via SparkOperator. O resultado final está organizado em três camadas:

  - Bronze: dados brutos extraídos da API

  - Silver: dados tratados e padronizados

  - Gold: geração de insights finais a partir dos dados

Como próximos passos, planejo integrar o projeto com Trino + DBeaver, permitindo consultas SQL diretamente no Data Lake. Embora vá além do escopo tradicional de Data Engineering, isso traria uma forma prática e poderosa de explorar os dados.
  


📌 Destaques da arquitetura

<table>
  <tr>
    <th>Ferramenta</th>
    <th>Porta</th>
  </tr>
  <tr>
    <td>MinIO</td>
    <td>Apache Airflow</td>
  </tr>
  <tr>
    <td>Camada de Armazenamento: MinIO como lakehouse</td>
    <td>Processamento: Spark para manipulação de dados</td>
  </tr>
</table> 

🛠 Componentes da Stack
  
<table>
  <tr>
    <th>Ferramenta</th>
    <th>Porta</th>
  </tr>
  <tr>
    <td>MinIO</td>
    <td>Apache Airflow</td>
  </tr>
  <tr>
    <td>9000</td>
    <td>8080</td>
  </tr>
</table> 

Provedores Airflow
  <img width="1235" height="340" alt="image" src="https://github.com/user-attachments/assets/ec43a10c-8180-4028-a46c-457d45e42784" />


💨 Fluxo Airflow
<img width="1868" height="887" alt="image" src="https://github.com/user-attachments/assets/297bbd73-53ca-4a2e-8fda-a16c36daab45" />


  
