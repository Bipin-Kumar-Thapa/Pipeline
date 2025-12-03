import pandas as pd
import os
from sqlalchemy import create_engine

def _safe_mode(series):
    """
    Computes the mode of a column, returns None if no mode is found.
    """
    try:
        m = series.mode()
        if not m.empty:
            return m.iloc[0]
    except Exception:
        pass
    return None

def _read_csv_robust(path):
    """
    Robust CSV reader that handles potential UnicodeDecodeErrors.
    """
    try:
        return pd.read_csv(path, encoding='utf-8')
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='latin1')

def run_etl(input_path, table_name="loan_data_cleaned"):
    """
    Reads CSV, fills NULLs with mode, splits date, and loads into Postgres.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    df = _read_csv_robust(input_path)

    # Clean empty spaces, unicode spaces etc.
    df.replace([r'^\s*$', r'\s+', ' ', r'\u200b', r'\u00a0'], pd.NA, regex=True, inplace=True)

    # ---------------------------
    # Corrected Fill-Nulls Logic
    # ---------------------------
    for col in df.columns:

        # If column is numeric type
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce")
            mode_val = _safe_mode(df[col])

        # Text / categorical
        else:
            mode_val = _safe_mode(df[col])

        # Fill NaN only if mode is valid
        if mode_val is not None and pd.notna(mode_val):
            df[col] = df[col].fillna(mode_val)

    # Split date columns
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    for date_col in date_columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[f"{date_col}_only"] = df[date_col].dt.date
        df[f"{date_col}_time_only"] = df[date_col].dt.time

    insights = {
        "total_records": len(df),
        "columns": list(df.columns),
        "null_values_before_fill": df.isnull().sum().to_dict()
    }

    if "user_id" in df.columns:
        insights["unique_users"] = int(df["user_id"].nunique())

    try:
        engine = create_engine("postgresql://airflow:airflow@postgres/airflow")
        df.to_sql(table_name, engine, if_exists="replace", index=False)
    except Exception as e:
        raise

    return insights, df
