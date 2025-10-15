# Inteligência Energética

## Visão Geral do Projeto

Este projeto visa aplicar princípios de inteligência de dados e aprendizado de máquina para análises e otimização no setor energético. Ele integra diversas ferramentas e plataformas para orquestração de dados, transformação, armazenamento e desenvolvimento de modelos de ML.

## Tecnologias Utilizadas

O projeto utiliza uma variedade de ferramentas e frameworks:

*   **Airbyte**: Para ingestão de dados.
*   **Airflow**: Para orquestração e agendamento de fluxos de trabalho de dados.
*   **dbt (data build tool)**: Para transformação e modelagem de dados no data warehouse.
*   **Docker**: Para conteinerização e gerenciamento de ambientes de desenvolvimento e produção.
*   **Snowflake**: Como data warehouse escalável e baseado em nuvem.
*   **Machine Learning (ML)**: Para desenvolvimento e implantação de modelos preditivos e analíticos.

## Estrutura do Projeto

A estrutura do projeto é organizada da seguinte forma:

*   `airbyte/`: Configurações e definições para conectores e sincronizações do Airbyte.
*   `airflow/`: DAGs (Directed Acyclic Graphs) e configurações relacionadas ao Apache Airflow para orquestração de tarefas.
*   `dbt/`: Modelos, testes e configurações do dbt para transformações de dados.
*   `docker/`: Arquivos Dockerfile e configurações de Docker Compose para conteinerização dos serviços.
*   `ml/`: Código para desenvolvimento, treinamento e avaliação de modelos de Machine Learning.
*   `snowflake/`: Scripts SQL e configurações específicas para o Snowflake data warehouse.

## Configuração e Instalação

Para configurar e executar este projeto localmente, siga os passos abaixo:

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/seu-usuario/Inteligencia-Energetica.git
    cd Inteligencia-Energetica
    ```

2.  **Configurar Variáveis de Ambiente:**
    Crie um arquivo `.env` no diretório de cada componente docker (dbt, metabase), com as credenciais e configurações necessárias para Airbyte, Airflow, Snowflake, etc.

3.  **Docker:**
    Certifique-se de ter o Docker e o Docker Compose instalados. Os serviços do projeto podem ser iniciados usando:
    ```bash
    docker-compose up -d
    ```
    Isso iniciará os contêineres para Airbyte, Airflow e quaisquer outros serviços definidos nos arquivos `docker-compose`.

4.  **Configuração do Airbyte:**
    Acesse a UI do Airbyte (serviço SaaS) e configure as fontes e destinos de dados conforme necessário, utilizando as definições em `airbyte/`.

5.  **Configuração do Airflow:**
    Acesse a UI do Airflow (em `http://localhost:8080`) e ative as DAGs localizadas em `airflow/dags/`.

6.  **Configuração do dbt:**
    Navegue até o diretório `dbt/` e configure o perfil do dbt para se conectar ao Snowflake.
    ```bash
    cd dbt
    dbt debug --profile seu_perfil_snowflake
    ```
    Execute os modelos dbt:
    ```bash
    dbt build
    ```

7.  **Ambiente de Machine Learning:**
    Para o desenvolvimento de ML, é recomendável criar um ambiente virtual Python:
    ```bash
    cd ml
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```
    Siga as instruções específicas dentro do diretório `ml/` para executar os notebooks ou scripts de ML.

## Uso

*   **Orquestração de Dados**: Gerencie e monitore seus pipelines de dados através da interface do Apache Airflow.
*   **Transformação de Dados**: Utilize o dbt para definir e executar transformações de dados, garantindo qualidade e consistência.
*   **Análise e Machine Learning**: Desenvolva e implante modelos de ML para obter insights e fazer previsões no domínio de energia.
