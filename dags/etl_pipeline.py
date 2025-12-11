import pandas as pd
import os
from sqlalchemy import create_engine
import logging

# Set up basic logging so print statements go to Airflow logs correctly
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _safe_mode(series):
    """
    Computes the mode of a column, returns None if no mode is found.
    """
    try:
        m = series.mode()
        if not m.empty:
            return m.iloc[0]
    except Exception as e:
        logger.error(f"Error computing mode: {e}")
        pass
    return None

def _read_csv_robust(path):
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
    logger.info(f"Successfully read input file with {len(df)} records.")

    # -------------------------------------------------------
    # FIXED: Replace only empty or whitespace-only fields
    # -------------------------------------------------------
    df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)

    # Convert text nulls
    df.replace(
        ["null", "NULL", "Null", "nan", "NaN", "NAN"],
        pd.NA,
        inplace=True
    )

    # ---------------------------
    # Corrected Fill-Nulls Logic
    # ---------------------------
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce")
            mode_val = _safe_mode(df[col])
        else:
            mode_val = _safe_mode(df[col])

        if mode_val is not None and pd.notna(mode_val):
            df[col] = df[col].fillna(mode_val)

    # Split date columns
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    logger.info(f"Identified date columns for processing: {date_columns}")
    
    for date_col in date_columns:

        # -------------------------------------------------------
        # FIXED DATETIME: supports both H:MM and HH:MM formats
        # -------------------------------------------------------
        df[date_col] = pd.to_datetime(
            df[date_col],
            errors="raise",
            infer_datetime_format=True
        )

        df["Date_only"] = df[date_col].dt.date
        df["Time_only"] = df[date_col].dt.time

    # Diagnostic logs
    logger.info("DataFrame dtypes after date processing:\n" + str(df.dtypes))
    if date_columns:
        null_counts = df[date_columns].isnull().sum().to_dict()
        logger.info("Sample of processed date column data:\n" + str(df[date_columns].head()))
        logger.info(f"Null/NaT counts in date columns after processing: {null_counts}")

        if any(count > 0 for count in null_counts.values()):
            logger.warning("Some dates were converted to NaT (blanks/nulls). Check input data consistency.")

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
        logger.info(f"Successfully loaded data into PostgreSQL table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to load data into PostgreSQL: {e}")
        raise

    return insights, df
