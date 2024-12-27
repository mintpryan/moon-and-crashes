import psycopg2
import pandas as pd
import plotly.express as px
from airflow.models import Variable

DB_NAME = Variable.get("DB_NAME", default_var="postgres")
DB_USER = Variable.get("DB_USER", default_var="postgres")
DB_PASSWORD = Variable.get("DB_PASSWORD", default_var="postgres")
DB_HOST = Variable.get("DB_HOST", default_var="localhost")
DB_PORT = Variable.get("DB_PORT", default_var=5432)


def visualise(file_path):
    val = 'total_killed'
    df = pd.read_csv(file_path)
    df["crash_date"] = pd.to_datetime(df["crash_date"])
    df["month"] = df["crash_date"].dt.to_period("M")
    aggregated_df = df.groupby(["moon_phase_category", "month"])[
        val].mean().reset_index()
    aggregated_df["month"] = aggregated_df["month"].astype(str)
    monthly_totals = aggregated_df.groupby(
        "month")[val].transform("sum")
    aggregated_df["percentage"] = (
        aggregated_df[val] / monthly_totals) * 100
    fig = px.bar(aggregated_df, x='percentage', y='month', color='moon_phase_category',
                 title='Значения из CSV', orientation='h', text_auto=".1f")
    output_html_path = "output_graph.html"

    fig.write_html(output_html_path)


def load_to_postgres(file_path):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO nyc_crashes (crash_date, total_accidents, total_injured, total_killed, moon_phase, moon_phase_category)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row["crash_date"], row["total_accidents"], row["total_injured"],
            row["total_killed"], row["moon_phase"], row["moon_phase_category"]
        ))
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    visualise(
        '/Users/iana/Documents/DataEngineer/Portfolio/moon_and_crashes/transformed_data.csv')
