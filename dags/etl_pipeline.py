import pandas as pd
from sqlalchemy import create_engine

def run_etl(input_path, table_name="loan_data_cleaned"):
    df = pd.read_csv(input_path)

    # Replace NULL values with mode
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            mode_val = df[col].mode().iloc[0]
            df[col] = df[col].fillna(mode_val)

    # Split date and time
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date_only"] = df["date"].dt.date
        df["time_only"] = df["date"].dt.time

    # Insights
    insights = {
        "total_records": len(df),
        "columns": list(df.columns)
    }
    if "user_id" in df.columns:
        insights["unique_users"] = df["user_id"].nunique()

    print("Insights:", insights)

    # Load to Postgres
    engine = create_engine("postgresql://airflow:airflow@postgres/airflow")
    df.to_sql(table_name, engine, if_exists="replace", index=False)

    return insights
