# README

## Visão Geral

Este script Python (`telecomx_etl_eda.py`) realiza um processo completo de ETL (Extração, Transformação e Carga) seguido por uma Análise Exploratória de Dados (EDA) em um dataset de churn de clientes da Telecom X. O script foi projetado para ler dados em formato JSON, limpar e transformar os dados, gerar visualizações significativas e produzir um relatório detalhado em Markdown.

## Funcionalidades

 **Extração:**
 - Carrega dados de um arquivo JSON, acomodando estruturas aninhadas ou planas.
 - **Transformação:**
 - Padroniza nomes de colunas e converte tipos de dados.
 - Trata valores faltantes e padroniza categorias.
 - Remove duplicatas com base no `customerID`.
  - Cria novos recursos como `AverageMonthlyCharge`.
  - **Análise Exploratória:**
 - Calcula taxas de churn e estatísticas descritivas.
  - Gera gráficos de distribuição de churn e relações com outras variáveis.
- **Relatório:**
  - Produz um relatório em Markdown detalhando o processo ETL, insights da EDA e recomendações.
- **Visualização:**
  - Cria gráficos de barras, boxplots e distribuição de churn para análise visual.
- **Saída:**
  - Salva os dados limpos em um arquivo CSV.
  - Gera um relatório em Markdown com tabelas e links para os gráficos gerados.

## Requisitos

- Python 3
- Bibliotecas Python:  - `pandas`
- `numpy` - `matplotlib` - `argparse`
- `pathlib` - `textwrap`

Você pode instalar as dependências usando o pip:  

pip install pandas numpy matplotlib argparse pathlib
Como Usar

Salve o script: Salve o código como telecomx_etl_eda.py.

Prepare o arquivo JSON: Certifique-se de ter seu arquivo de dados JSON (por exemplo, TelecomX_Data.json).

Execute o script: Execute o script a partir da linha de comando, fornecendo o caminho para o arquivo JSON de entrada e o diretório de saída:


python telecomx_etl_eda.py --input TelecomX_Data.json --outdir ./saidas
--input: Caminho para o arquivo JSON de entrada.
--output: (Opcional) Diretório para salvar os arquivos de saída. O padrão é ./saidas.

Verifique os resultados: Após a execução, o script irá gerar:

saidas/telecomx_clean.csv: Dataset limpo em formato CSV.
saidas/relatorio_telecomx_etl_eda.md: Relatório em Markdown.
saidas/plot_churn_distribuicao.png: Gráfico de distribuição do churn.
saidas/plot_churn_por_contrato.png: Gráfico de churn por tipo de contrato.
saidas/plot_churn_por_pagamento.png: Gráfico de churn por método de pagamento.
saidas/plot_churn_por_internet.png: Gráfico de churn por serviço de internet.
saidas/plot_monthlycharges_por_churn.png: Boxplot de cobranças mensais por churn.
Detalhes de Implementação

Importações:

argparse: Para gerenciar argumentos da linha de comando.
json: Para carregar dados JSON.
pathlib: Para manipulação de caminhos de arquivos.
textwrap: Para formatar texto.
numpy: Para operações numéricas.
pandas: Para manipulação e análise de dados.
matplotlib.pyplot: Para criar visualizações.

Funções:

to_dataframe(obj): Converte objetos JSON em DataFrames pandas.
load_json(path): Carrega um arquivo JSON e o converte em um DataFrame.
standardize_columns(df): Padroniza nomes de colunas, converte tipos de dados e trata valores faltantes.
create_features(df): Cria novas colunas de recursos a partir dos dados existentes.
churn_rate(subset): Calcula a taxa de churn.
plot_bar_series(series, title, xlabel, ylabel, path): Cria e salva um gráfico de barras.
plot_churn_distribution(df, outdir): Gera e salva um gráfico de distribuição de churn.
plot_box_monthly_by_churn(df, outdir): Cria um boxplot de cobranças mensais por churn.
eda_and_plots(df, outdir): Realiza EDA e gera gráficos.
write_report(output, source_name, original_shape, df_clean, stats, dropped_dupes, saved_csv): Cria o relatório em Markdown.
main(): A função principal que executa todo o processo ETL e EDA.
Estrutura do Código

Extração de Dados

A função load_json carrega os dados JSON do arquivo especificado e os converte em um DataFrame pandas.

Transformação de Dados

A função standardize_columns padroniza os nomes das colunas, remove espaços e mapeia nomes comuns para um formato consistente.
Valores numéricos são convertidos para tipos numéricos, e as categorias são padronizadas para garantir a consistência.
Duplicatas são removidas com base no customerID.
A função create_features cria novos recursos, como o AverageMonthlyCharge, a partir dos dados existentes.

Análise Exploratória de Dados (EDA)

A função eda_and_plots calcula a taxa geral de churn e taxas de churn por categorias importantes, como contrato, método de pagamento e serviço de internet.
Estatísticas
