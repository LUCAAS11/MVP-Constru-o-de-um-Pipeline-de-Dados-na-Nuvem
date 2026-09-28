# MVP: Pipeline de Dados na Nuvem (Olist E-Commerce)

Este repositório contém a documentação e os scripts de um Produto Mínimo Viável (MVP) focado na construção de um pipeline de dados ponta a ponta na nuvem. A arquitetura implementada segue o padrão Medalhão (Bronze, Silver e Gold) em um ambiente Data Lakehouse, utilizando **Databricks**, **PySpark** (Delta Lake) e **SQL**.

---

## Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

**Contexto do Problema:**
O comércio eletrônico depende de uma cadeia logística eficiente e de um portfólio de produtos bem direcionado. O objetivo deste projeto é analisar o ecossistema de vendas da Olist (um marketplace brasileiro) para mapear o desempenho financeiro por categoria e entender os gargalos operacionais que impactam a satisfação do consumidor final.

**Perguntas de Negócio:**
1. Qual é o impacto do tempo de entrega (atraso vs. no prazo) nas notas de avaliação dadas pelos clientes?
2. Quais são as 10 categorias de produtos que geram o maior volume total de vendas (faturamento bruto)?
3. Qual é a distribuição do volume de pedidos e o valor médio de frete cobrado por estado (UF) do cliente?
4. Qual o percentual de pedidos entregues com atraso e como isso se reflete nas piores avaliações (nota 1)?

**Sobre os Dados:**
Foi utilizado o *Brazilian E-Commerce Public Dataset by Olist*, disponibilizado publicamente no Kaggle. O dataset abrange 100 mil pedidos de 2016 a 2018 no Brasil. Os dados originais consistem em arquivos `.csv` estruturados da seguinte forma:
* `olist_orders_dataset`: Dados mestre dos pedidos (status e datas logísticas).
* `olist_order_items_dataset`: Itens comprados dentro de cada pedido, preços e valores de frete.
* `olist_order_reviews_dataset`: Avaliações e comentários feitos pelos clientes.
* `olist_products_dataset`: Dimensão com características e categorias dos produtos.
* `olist_customers_dataset`: Dimensão com a localização (CEP, Cidade, Estado) dos clientes.
* **Licença de Uso:** Os dados são fornecidos sob a licença de uso aberto (CC BY-NC-SA 4.0), permitindo o uso acadêmico e de portfólio.

---

## Carga dos Dados (Etapa 4.2)

A ingestão primária foi realizada em ambiente de nuvem utilizando o **Databricks Free Edition**.
1. Os arquivos `.csv` brutos foram baixados do Kaggle e carregados manualmente no *Unity Catalog*, especificamente no diretório de *Volumes* (`/Volumes/mvp/default/mvp/`).
2. A partir deste armazenamento temporário, um script PySpark leu os arquivos aplicando inferência de schema e salvou as tabelas nativamente no formato **Delta** dentro do *schema* (banco de dados) `bronze`.

**Evidências da Ingestão:**

![Prova do Upload dos Arquivos](Etapa%204-2.png)

![Criação das Tabelas na Camada Bronze](Etapa%204-2-1.png)

---

## Modelagem e Catálogo de Dados (Etapa 4.3)

O projeto utilizou a Modelagem Dimensional através de um **Esquema Estrela (Star Schema)**, construído na camada Gold do Lakehouse. Centralizamos os eventos logísticos e financeiros em uma tabela Fato, cercada por dimensões de contexto (clientes e produtos).

**Catálogo de Dados:**

**Tabela: `dim_clientes`** (Dimensão)
*   **Contexto:** Dados geográficos e identificação dos clientes.
*   `customer_id` (string): Chave primária do cliente no pedido.
*   `customer_unique_id` (string): Identificador único do cliente.
*   `customer_city` (string): Cidade de residência.
*   `customer_state` (string): Estado (UF) de residência.

**Tabela: `dim_produtos`** (Dimensão)
*   **Contexto:** Categorias e características dos produtos ofertados.
*   `product_id` (string): Chave primária do produto.
*   `product_category_name` (string): Categoria do produto (Domínio: nomes de categorias ou "nao_informado").

**Tabela: `fato_vendas`** (Fato)
*   **Contexto:** Registro transacional cruzando itens vendidos, clientes, valores monetários e métricas logísticas (apenas pedidos consolidados com status `delivered`).
*   `order_id` (string): Identificador único do pedido.
*   `order_item_id` (integer): Sequencial do item dentro do pedido.
*   `product_id` (string): FK ligando à `dim_produtos`.
*   `customer_id` (string): FK ligando à `dim_clientes`.
*   `price` (double): Valor cobrado pelo produto.
*   `freight_value` (double): Valor cobrado de frete.
*   `order_purchase_timestamp` (timestamp): Data/hora da compra.
*   `order_delivered_customer_date` (timestamp): Data exata da entrega.
*   `order_estimated_delivery_date` (timestamp): Prazo logístico prometido.
*   `dias_atraso` (integer): Métrica calculada (Diferença entre data de entrega e estimativa).
*   `review_score` (integer): Nota agregada (1 a 5) dada pelo cliente (mínimo, em caso de múltiplas avaliações).

**Esquema Físico no Catálogo (Databricks):**

![Esquema Físico da Camada Gold](Etapa%204-3.png)

---

## Pipeline de Dados (Etapa 4.4)

O ETL foi construído de forma centralizada utilizando Notebooks no Databricks com a API estruturada do **PySpark**, garantindo performance e reprodutibilidade, refletindo a progressão Lógica:
1. **Camada Bronze (Ingestão):** Arquivos lidos estritamente como vieram da origem (`.csv`) e convertidos para Delta Tables (`mvp.bronze.*`), preservando o histórico imutável.
2. **Camada Silver (Limpeza):** Tratamento de nulos vitais, deduplicação (`dropDuplicates()`) e tipagem rigorosa, em especial a conversão de *strings* para *timestamps*. 
3. **Camada Gold (Negócio):** Joins lógicos para compor as dimensões e a tabela fato, com o pré-cálculo da regra de negócios de logística (`dias_atraso`) utilizando a função `datediff()`.

**Execução do Pipeline de Transformação (ETL):**

![Prova do Tratamento e Processamento Silver](Etapa%204-4.png)

---

## Qualidade de Dados (Etapa 4.5)

Durante a transformação da camada Bronze para a Silver, a exploração inicial revelou inconsistências críticas:
1. **Inconsistência de Tipagem (Datas Corrompidas):** Na tabela de avaliações (`reviews`), a coluna `review_creation_date` possuía comentários em texto livre (ex: *" mas chegou dia 30/01."*) misturados com as datas. A tentativa padrão de cast para `timestamp` quebrava a execução.
    *   *Solução aplicada:* Uso da instrução `try_cast()` via PySpark. Dados válidos foram convertidos em `timestamp`, e os textos sujos foram silenciados para `null` de forma segura.
2. **Nulos em Dimensões:** Alguns produtos não tinham classificação de categoria definida. 
    *   *Solução aplicada:* Preenchimento padrão (`coalesce`) injetando a string `"nao_informado"` para garantir que nenhuma análise financeira fosse descartada em futuras agregações.
3. **Verificação Gold:** A etapa inicial de exploração (via SQL) na tabela fato validou 110.197 registros, resultando em zero clientes nulos e zero preços nulos, garantindo a completude das chaves para as análises subsequentes.

**Validação Final de Qualidade:**

![Verificação de Qualidade Chaves Nulas](Etapa%204-5-1.png)

---

## Análise de Dados (Etapa 4.5)

As consultas analíticas (SQL) sobre a camada Gold forneceram respostas claras ao problema de negócio formulado:

**Perguntas 1 e 4: Logística, Atrasos e Satisfação**
* A análise evidenciou que atrasos logísticos corroem severamente a satisfação do cliente.
* Pedidos **"No Prazo / Adiantados"** (89.451 pedidos) mantêm uma nota média excelente de **4.21**, com apenas 9,7% das avaliações recebendo a pior nota (1 estrela).
* Em contrapartida, para os **pedidos "Atrasados"** (6.381 pedidos), a nota média despenca pela metade, caindo para **2.26**. Destes, impressionantes **60,46%** resultam na pior avaliação possível (nota 1). 
* *Insight:* O atraso é o principal driver de detração da marca na Olist.

**Pergunta 2: Faturamento Bruto por Categoria**
* A categoria "Beleza e Saúde" liderou de forma isolada, gerando **R$ 1.233.131,72** no período, seguida por "Relógios e Presentes" (R$ 1.166.176,98) e "Cama, Mesa e Banho" (R$ 1.023.434,76). 

**Pergunta 3: Distribuição Geográfica e Custos de Frete**
* **Volume:** São Paulo (SP) concentra massivamente as vendas (40.501 pedidos), seguido de longe por RJ (12.350) e MG (11.354).
* **Custos:** O frete médio evidencia as barreiras geográficas brasileiras. Enquanto o cliente de SP paga em média **R$ 15,12**, os estados das regiões Norte e Nordeste pagam até três vezes mais, como Roraima (R$ 43,09) e Paraíba (R$ 43,09), o que explica o volume reduzido nestas regiões.

**Resultados das Consultas de Negócio:**

![Insights de Negócio - Faturamento](Etapa%204-5-2.png)
![Insights de Negócio - Logística](Etapa%204-5-3.png)
![Insights de Negócio - Geografia 1](Etapa%204-5-4-1.png)
![Insights de Negócio - Geografia 2](Etapa%204-5-4-2.png)

---

## Autoavaliação

Durante o desenvolvimento deste MVP, consegui aplicar na prática os conceitos teóricos de Engenharia de Dados. A principal dificuldade encontrada foi o tratamento da qualidade dos dados brutos, especificamente a tipagem de datas sujas que quebrava o script original, o que foi superado utilizando funções de tolerância a falhas (`try_cast`). 
Para trabalhos futuros, seria interessante automatizar este pipeline através do Databricks Workflows e conectar a tabela Gold em uma ferramenta de visualização, como o Power BI, construindo um dashboard em tempo real.
