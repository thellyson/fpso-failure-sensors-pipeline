import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import altair as alt
import os

# --- Configuração da conexão com o banco ---
@st.cache_resource
def get_engine():
    """Cria e retorna o engine de conexão com o Postgres via Docker."""
    user = os.getenv("POSTGRES_USER", "shape")
    pwd  = os.getenv("POSTGRES_PASSWORD", "shape")
    db   = os.getenv("POSTGRES_DB", "fpso")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    url  = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)

@st.cache_data
def load_data():
    """Carrega os DataFrames necessários com caching para otimização."""
    engine = get_engine()

    total_fail_df = pd.read_sql(
        "SELECT COUNT(*) AS total_failures FROM silver.equipment_failure_sensors;",
        engine
    )

    totals_eq_df = pd.read_sql(
        """
        SELECT equipment_id, name, total_failures
        FROM gold.equipment_failures_summary
        ORDER BY total_failures DESC;
        """,
        engine
    )

    top_per_group_df = pd.read_sql(
        """
        SELECT group_name, name, total_failures
        FROM (
          SELECT group_name, name, total_failures,
                 ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY total_failures DESC) AS rn
          FROM gold.equipment_failures_summary
        ) t
        WHERE rn = 1
        ORDER BY total_failures DESC;
        """,
        engine
    )

    avg_with_eq_df = pd.read_sql(
        """
        SELECT DISTINCT equipment_id, name, group_name, avg_failures_per_asset
        FROM gold.equipment_failures_summary
        ORDER BY avg_failures_per_asset ASC;
        """,
        engine
    )

    sensor_rank_df = pd.read_sql(
        """
        SELECT equipment_id, name, group_name,
               sensor_id, sensor_failures,
               sensor_rank_by_equipment,
               sensor_rank_by_equipment_group
        FROM gold.equipment_failures_summary
        ORDER BY equipment_id, sensor_rank_by_equipment;
        """,
        engine
    )

    return total_fail_df, totals_eq_df, top_per_group_df, avg_with_eq_df, sensor_rank_df


def filter_dataframe(df, label_prefix):
    """Gera filtros de texto para cada coluna do DataFrame."""
    df = df.copy()
    with st.expander(f"Filtros - {label_prefix}", expanded=False):
        for col in df.columns:
            user_input = st.text_input(f"Filtrar {col}", key=f"{label_prefix}_{col}")
            if user_input:
                df = df[df[col].astype(str).str.contains(user_input, case=False, na=False)]
    return df


def main():
    st.set_page_config(page_title="Dashboard de Falhas de Equipamentos", layout="wide")
    total_fail_df, totals_eq_df, top_per_group_df, avg_with_eq_df, sensor_rank_df = load_data()

    st.header("1. Total de falhas em todos os equipamentos")
    total_failures = int(total_fail_df["total_failures"].iloc[0])
    st.metric(label="Total de Falhas", value=total_failures)

    st.header("2. Equipamento com mais falhas")
    top_eq = totals_eq_df.iloc[0]
    st.write(f"**{top_eq['name']}** (ID: {top_eq['equipment_id']}) teve **{top_eq['total_failures']}** falhas.")

    st.subheader("Gráfico de falhas por equipamento (ordenado desc.)")
    chart = alt.Chart(totals_eq_df).mark_bar().encode(
        x=alt.X('name:N', sort=alt.EncodingSortField('total_failures', order='descending'), title='Equipamento'),
        y=alt.Y('total_failures:Q', title='Total de Falhas'),
        tooltip=[alt.Tooltip('name:N', title='Equipamento'), alt.Tooltip('total_failures:Q', title='Falhas')]
    ).properties(width='container', height=400)
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Tabela: equipamentos por grupo com mais falhas")
    df_top_group = top_per_group_df.rename(columns={'group_name':'Grupo','name':'Equipamento','total_failures':'Total de Falhas'})
    df_top_group = filter_dataframe(df_top_group, 'TopPorGrupo')
    st.dataframe(df_top_group, use_container_width=True)

    st.header("3. Média de falhas por ativo em cada grupo")
    df_avg = avg_with_eq_df.rename(columns={'equipment_id':'ID Equipamento','name':'Nome do Equipamento','group_name':'Grupo','avg_failures_per_asset':'Média de Falhas por Ativo'})
    df_avg = filter_dataframe(df_avg, 'MediaPorGrupo')
    st.dataframe(df_avg, use_container_width=True)

    st.header("4. Ranking de sensores por equipamento")
    df_rank = sensor_rank_df.rename(columns={'equipment_id':'ID Equipamento','name':'Nome do Equipamento','group_name':'Grupo','sensor_id':'ID Sensor','sensor_failures':'Total Falhas','sensor_rank_by_equipment':'Rank no Equipamento','sensor_rank_by_equipment_group':'Rank no Grupo'})
    df_rank = filter_dataframe(df_rank, 'RankSensores')
    st.dataframe(df_rank, use_container_width=True)

if __name__ == "__main__":
    main()
