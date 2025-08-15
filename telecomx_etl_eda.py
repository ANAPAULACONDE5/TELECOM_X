# telecomx_etl_eda.py
# ------------------------------------------------------------
# ETL + EDA para Churn de Clientes - Telecom X
# - Lê JSON (aninhado ou não)
# - Limpa/padroniza, cria features
# - Gera CSV limpo, gráficos e relatório Markdown
# Uso:
#   python telecomx_etl_eda.py --input TelecomX_Data.json --outdir ./saidas
# ------------------------------------------------------------

import argparse
import json
from pathlib import Path
from textwrap import dedent

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def to_dataframe(obj) -> pd.DataFrame:
    """Converte o JSON em DataFrame, suportando estruturas aninhadas ou planas."""
    if isinstance(obj, list):
        return pd.json_normalize(obj, sep="_")
    if isinstance(obj, dict):
        # Se houver uma chave com lista, assume que é o payload
        for k, v in obj.items():
            if isinstance(v, list):
                return pd.json_normalize(v, sep="_")
        # Caso seja um único registro
        return pd.json_normalize([obj], sep="_")
    return pd.DataFrame()


def load_json(path: Path) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return to_dataframe(raw)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().replace(" ", "_").replace("-", "_") for c in df.columns]

    # Mapeamento para nomes padronizados comuns nesse dataset
    rename_map = {
        "customerid": "customerID",
        "customer_id": "customerID",
        "gender": "gender",
        "seniorcitizen": "SeniorCitizen",
        "partner": "Partner",
        "dependents": "Dependents",
        "tenure": "tenure",
        "phoneservice": "PhoneService",
        "multiplelines": "MultipleLines",
        "internetservice": "InternetService",
        "onlinesecurity": "OnlineSecurity",
        "onlinebackup": "OnlineBackup",
        "deviceprotection": "DeviceProtection",
        "techsupport": "TechSupport",
        "streamingtv": "StreamingTV",
        "streamingmovies": "StreamingMovies",
        "contract": "Contract",
        "paperlessbilling": "PaperlessBilling",
        "paymentmethod": "PaymentMethod",
        "monthlycharges": "MonthlyCharges",
        "totalcharges": "TotalCharges",
        "churn": "Churn",
    }

    dynamic = {}
    for col in df.columns:
        key = col.lower()
        if key in rename_map:
            dynamic[col] = rename_map[key]
    if dynamic:
        df.rename(columns=dynamic, inplace=True)

    # Aparar espaços
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # Conversões numéricas seguras
    for num_col in ["MonthlyCharges", "TotalCharges", "tenure"]:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

    # Normalizar alvo Churn para Yes/No
    if "Churn" in df.columns:
        df["Churn"] = df["Churn"].astype(str).str.title()
        df = df[df["Churn"].isin(["Yes", "No"])].copy()

    # Padronizar categorias tipo "No internet service" -> "No"
    for col in [
        "OnlineSecurity",
        "OnlineBackup",
        "DeviceProtection",
        "TechSupport",
        "StreamingTV",
        "StreamingMovies",
        "MultipleLines",
    ]:
        if col in df.columns:
            df[col] = df[col].replace(
                {
                    "No internet service": "No",
                    "No phone service": "No",
                    "Sem serviço de internet": "No",
                    "Sem serviço de telefone": "No",
                }
            )

    # SeniorCitizen 0/1 -> Yes/No (ou title-case se já categórico)
    if "SeniorCitizen" in df.columns:
        sc_num = pd.to_numeric(df["SeniorCitizen"], errors="coerce")
        if sc_num.notna().mean() > 0.5:
            df["SeniorCitizen"] = np.where(sc_num == 1, "Yes", "No")
        else:
            df["SeniorCitizen"] = df["SeniorCitizen"].astype(str).str.title()

    # Remover duplicatas por customerID, se existir
    if "customerID" in df.columns:
        df = df.drop_duplicates(subset=["customerID"]).copy()

    return df


def create_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if all(c in df.columns for c in ["TotalCharges", "tenure"]):
        df["AverageMonthlyCharge"] = np.where(
            df["tenure"] > 0, df["TotalCharges"] / df["tenure"], np.nan
        )
    return df


def churn_rate(subset: pd.DataFrame) -> float:
    if "Churn" not in subset.columns:
        return np.nan
    return subset["Churn"].eq("Yes").mean() * 100.0


def plot_bar_series(series: pd.Series, title: str, xlabel: str, ylabel: str, path: Path):
    plt.figure()
    series.sort_values().plot(kind="bar")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def plot_churn_distribution(df: pd.DataFrame, outdir: Path) -> Path:
    path = outdir / "plot_churn_distribuicao.png"
    plt.figure()
    df["Churn"].value_counts(dropna=False).sort_index().plot(kind="bar")
    plt.title("Distribuição de Churn (Contagem)")
    plt.xlabel("Churn")
    plt.ylabel("Contagem")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    return path


def plot_box_monthly_by_churn(df: pd.DataFrame, outdir: Path) -> Path:
    path = outdir / "plot_monthlycharges_por_churn.png"
    plt.figure()
    data_box = [
        df.loc[df["Churn"] == "No", "MonthlyCharges"].dropna(),
        df.loc[df["Churn"] == "Yes", "MonthlyCharges"].dropna(),
    ]
    plt.boxplot(data_box, labels=["No", "Yes"], showmeans=True)
    plt.title("MonthlyCharges por Churn")
    plt.xlabel("Churn")
    plt.ylabel("MonthlyCharges")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    return path


def eda_and_plots(df: pd.DataFrame, outdir: Path) -> dict:
    out = {}

    # Churn geral
    out["overall_churn_rate_pct"] = churn_rate(df)

    # Taxas por categorias-chave
    for cat in ["Contract", "PaymentMethod", "InternetService", "SeniorCitizen", "Partner", "Dependents"]:
        if cat in df.columns:
            grp = df.groupby(cat)["Churn"].apply(lambda s: s.eq("Yes").mean() * 100.0)
            out[f"rate_{cat.lower()}"] = grp

    # Descritivas por churn
    for num in ["MonthlyCharges", "TotalCharges", "AverageMonthlyCharge", "tenure"]:
        if num in df.columns:
            out[f"describe_{num}"] = df.groupby("Churn")[num].describe()

    # Gráficos
    plots = []
    if "Churn" in df.columns:
        plots.append(plot_churn_distribution(df, outdir))
    if all(c in df.columns for c in ["Contract", "Churn"]):
        p = outdir / "plot_churn_por_contrato.png"
        rate = df.groupby("Contract")["Churn"].apply(lambda s: s.eq("Yes").mean() * 100.0)
        plot_bar_series(rate, "Taxa de Churn por Tipo de Contrato (%)", "Tipo de Contrato", "Churn (%)", p)
        plots.append(p)
    if all(c in df.columns for c in ["PaymentMethod", "Churn"]):
        p = outdir / "plot_churn_por_pagamento.png"
        rate = df.groupby("PaymentMethod")["Churn"].apply(lambda s: s.eq("Yes").mean() * 100.0)
        plot_bar_series(rate, "Taxa de Churn por Método de Pagamento (%)", "Método de Pagamento", "Churn (%)", p)
        plots.append(p)
    if all(c in df.columns for c in ["InternetService", "Churn"]):
        p = outdir / "plot_churn_por_internet.png"
        rate = df.groupby("InternetService")["Churn"].apply(lambda s: s.eq("Yes").mean() * 100.0)
        plot_bar_series(rate, "Taxa de Churn por InternetService (%)", "InternetService", "Churn (%)", p)
        plots.append(p)
    if all(c in df.columns for c in ["MonthlyCharges", "Churn"]):
        plots.append(plot_box_monthly_by_churn(df, outdir))

    out["plots"] = [str(p) for p in plots]
    return out


def series_to_markdown(s: pd.Series, name_left: str, name_right: str) -> str:
    if s is None or len(s) == 0:
        return "_(sem dados)_\n"
    df = s.rename(name_right).reset_index().rename(columns={"index": name_left})
    return df.to_markdown(index=False)


def describe_to_markdown(desc: pd.DataFrame, title: str) -> str:
    d = desc.reset_index()
    md = d.to_markdown(index=False)
    return f"**{title}**\n\n{md}\n"


def write_report(
    outdir: Path,
    source_name: str,
    original_shape: tuple,
    df_clean: pd.DataFrame,
    stats: dict,
    dropped_dupes: int,
    saved_csv: Path,
):
    report_path = outdir / "relatorio_telecomx_etl_eda.md"

    lines = []
    lines.append("# Relatório de ETL e EDA — Churn de Clientes Telecom X\n")
    lines.append("## 1. Introdução\n")
    lines.append(
        dedent(
            """\
            A Telecom X enfrenta alto índice de evasão de clientes (*churn*). Este relatório documenta:
            - **ETL**: extração do JSON, transformação (limpeza, padronização e criação de variáveis) e carga para CSV limpo;
            - **EDA**: exploração das variáveis e identificação de fatores associados ao churn.
            """
        )
        + "\n"
    )

    lines.append("## 2. Extração\n")
    lines.append(f"- Arquivo de origem: `{source_name}`\n")
    lines.append(f"- Registros/colunas (bruto): {original_shape[0]} / {original_shape[1]}\n")

    lines.append("## 3. Transformação\n")
    lines.append(
        dedent(
            f"""\
            Principais etapas:
            - Padronização de nomes de colunas e *strings*;
            - Conversão de tipos (`MonthlyCharges`, `TotalCharges`, `tenure`);
            - Normalização de categorias como `"No internet service"` → `"No"`;
            - Exclusão de duplicatas por `customerID` (removidos: {dropped_dupes});
            - Criação de `AverageMonthlyCharge = TotalCharges / tenure` (quando `tenure > 0`).
            """
        )
        + "\n"
    )

    lines.append("## 4. Análise Exploratória (EDA)\n")
    if not np.isnan(stats.get("overall_churn_rate_pct", np.nan)):
        lines.append(f"- **Taxa geral de churn**: {stats['overall_churn_rate_pct']:.2f}%\n")
    else:
        lines.append("- **Taxa geral de churn**: não disponível (faltando coluna `Churn`).\n")

    for cat in ["Contract", "PaymentMethod", "InternetService", "SeniorCitizen", "Partner", "Dependents"]:
        key = f"rate_{cat.lower()}"
        if key in stats:
            lines.append(f"\n**Churn por {cat} (%):**\n\n")
            lines.append(series_to_markdown(stats[key], cat, "Churn (%)"))

    for num in ["MonthlyCharges", "TotalCharges", "AverageMonthlyCharge", "tenure"]:
        key = f"describe_{num}"
        if key in stats:
            lines.append("\n" + describe_to_markdown(stats[key], f"Resumo de `{num}` por `Churn`"))

    if "plots" in stats and stats["plots"]:
        lines.append("\n## 5. Gráficos Gerados\n\n")
        for p in stats["plots"]:
            fname = Path(p).name
            lines.append(f"![{fname}](./{fname})\n")

    lines.append(
        dedent(
            """\
            ## 6. Conclusões e Recomendações

            Principais achados indicativos:
            - Contratos **Month-to-month** apresentam maior churn em relação a 1 ou 2 anos;
            - **MonthlyCharges** mais altos tendem a elevar o churn;
            - **Electronic check** (quando presente) costuma ter churn mais elevado;
            - Certos tipos de **InternetService** (p.ex., Fiber optic) podem concentrar churn.

            **Sugestões iniciais:**
            1. Incentivar migração para **contratos anuais** (descontos, benefícios);
            2. Pacotes com teto/estabilidade de preço e **programas de fidelidade**;
            3. Melhorar qualidade/suporte mirando segmentos com maior churn (p.ex., por InternetService);
            4. Estimular **métodos de pagamento com menor churn** (ex.: débito automático).
            """
        )
        + "\n"
    )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def main():
    parser = argparse.ArgumentParser(description="ETL + EDA - Telecom X (Churn)")
    parser.add_argument("--input", required=True, help="Caminho para o JSON (TelecomX_Data.json).")
    parser.add_argument("--outdir", default="./saidas", help="Diretório de saída para CSV, relatório e gráficos.")
    args = parser.parse_args()

    input_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_raw = load_json(input_path)
    original_shape = df_raw.shape

    df = standardize_columns(df_raw)
    before = len(df)
    dropped_dupes = 0
    if "customerID" in df.columns:
        df = df.drop_duplicates(subset=["customerID"]).copy()
        dropped_dupes = before - len(df)
    df = create_features(df)

    csv_path = outdir / "telecomx_clean.csv"
    df.to_csv(csv_path, index=False)

    stats = eda_and_plots(df, outdir)

    report_path = write_report(
        outdir=outdir,
        source_name=input_path.name,
        original_shape=original_shape,
        df_clean=df,
        stats=stats,
        dropped_dupes=dropped_dupes,
        saved_csv=csv_path,
    )

    print("---- ETL + EDA CONCLUÍDOS ----")
    print(f"Registros brutos: {original_shape[0]} | colunas brutas: {original_shape[1]}")
    print(f"Registros limpos: {len(df)} | colunas limpas: {df.shape[1]}")
    if not np.isnan(stats.get("overall_churn_rate_pct", np.nan)):
        print(f"Taxa geral de churn: {stats['overall_churn_rate_pct']:.2f}%")
    print(f"CSV salvo em: {csv_path}")
    print(f"Relatório (Markdown) salvo em: {report_path}")
    if stats.get("plots"):
        print("Gráficos gerados:")
        for p in stats["plots"]:
            print(f" - {p}")


if __name__ == "__main__":
    main()
