from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import DoubleType
import time


REQUIRED_COLUMNS = [
    "Age",
    "Gender",
    "Cholesterol Level",
    "Smoking",
    "Blood Pressure",
    "Heart Disease Status",
]


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("heart-pipeline-v2").getOrCreate()


def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read.csv(file_path, header=True, inferSchema=False)


def validate_required_columns(df: DataFrame) -> None:
    missing = [col_name for col_name in REQUIRED_COLUMNS if col_name not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def select_required_columns(df: DataFrame) -> DataFrame:
    return df.select(*REQUIRED_COLUMNS)


def normalize_string_column(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, F.lower(F.trim(F.col(col_name))))


def clean_categorical_column(df: DataFrame, col_name: str, allowed_values: list[str]) -> DataFrame:
    df = normalize_string_column(df, col_name)
    return df.withColumn(
        col_name,
        F.when(F.col(col_name).isin(allowed_values), F.col(col_name)).otherwise(F.lit(None))
    )


def clean_numeric_column(
    df: DataFrame,
    col_name: str,
    min_value: float | None = None,
    max_value: float | None = None,
) -> DataFrame:
    # Normalize obvious placeholder strings before casting
    df = df.withColumn(col_name, F.trim(F.col(col_name)))
    df = df.withColumn(
        col_name,
        F.when(
            F.lower(F.col(col_name)).isin("", "null", "none", "n/a", "na", "---", "unknown"),
            F.lit(None)
        ).otherwise(F.col(col_name))
    )

    # Cast invalid values to NULL
    df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    # Apply simple range validation if provided
    if min_value is not None:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) < F.lit(min_value), F.lit(None)).otherwise(F.col(col_name))
        )

    if max_value is not None:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) > F.lit(max_value), F.lit(None)).otherwise(F.col(col_name))
        )

    return df


def clean_data(df: DataFrame) -> DataFrame:
    # Numeric columns
    df = clean_numeric_column(df, "Age", min_value=0, max_value=120)
    df = clean_numeric_column(df, "Cholesterol Level", min_value=0, max_value=1000)
    df = clean_numeric_column(df, "Blood Pressure", min_value=0, max_value=400)

    # Categorical columns
    df = clean_categorical_column(df, "Gender", ["male", "female"])
    df = clean_categorical_column(df, "Smoking", ["yes", "no"])
    df = clean_categorical_column(df, "Heart Disease Status", ["yes", "no"])

    # Create numeric target flag for analysis
    df = df.withColumn(
        "heart_disease_flag",
        F.when(F.col("Heart Disease Status") == "yes", F.lit(1)).when(
            F.col("Heart Disease Status") == "no", F.lit(0)
        ).otherwise(F.lit(None))
    )

    return df


def count_nulls(df: DataFrame) -> DataFrame:
    return df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])


def remove_outliers_iqr(df: DataFrame, col_name: str) -> DataFrame:
    # Work only on non-null values for quartiles
    q1, q3 = df.approxQuantile(col_name, [0.25, 0.75], 0.0)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    return df.filter(
        (F.col(col_name).isNull()) |
        ((F.col(col_name) >= lower_bound) & (F.col(col_name) <= upper_bound))
    )


def descriptive_stats(df: DataFrame) -> dict:
    total_rows = df.count()

    stats = {
        "total_rows": total_rows,
        "valid_age_rows": df.filter(F.col("Age").isNotNull()).count(),
        "valid_cholesterol_rows": df.filter(F.col("Cholesterol Level").isNotNull()).count(),
        "valid_bp_rows": df.filter(F.col("Blood Pressure").isNotNull()).count(),
        "heart_disease_yes_count": df.filter(F.col("heart_disease_flag") == 1).count(),
        "heart_disease_no_count": df.filter(F.col("heart_disease_flag") == 0).count(),
    }

    numeric_summary = df.select("Age", "Cholesterol Level", "Blood Pressure").describe()
    return {"summary": stats, "numeric_summary_df": numeric_summary}


def grouped_stats(df: DataFrame) -> dict:
    by_gender = (
        df.groupBy("Gender")
        .agg(
            F.count("*").alias("row_count"),
            F.avg("Age").alias("avg_age"),
            F.avg("Cholesterol Level").alias("avg_cholesterol"),
            F.avg("heart_disease_flag").alias("heart_disease_rate"),
        )
        .orderBy("Gender")
    )

    by_smoking = (
        df.groupBy("Smoking")
        .agg(
            F.count("*").alias("row_count"),
            F.avg("Age").alias("avg_age"),
            F.avg("Cholesterol Level").alias("avg_cholesterol"),
            F.avg("heart_disease_flag").alias("heart_disease_rate"),
        )
        .orderBy("Smoking")
    )

    by_heart_status = (
        df.groupBy("Heart Disease Status")
        .agg(
            F.count("*").alias("row_count"),
            F.avg("Age").alias("avg_age"),
            F.avg("Cholesterol Level").alias("avg_cholesterol"),
        )
        .orderBy("Heart Disease Status")
    )

    return {
        "by_gender": by_gender,
        "by_smoking": by_smoking,
        "by_heart_status": by_heart_status,
    }


def build_chart_data(df: DataFrame) -> DataFrame:
    # This is the data you can later send to frontend for scatter/regression plot
    return df.select(
        "Age",
        "Cholesterol Level",
        "heart_disease_flag"
    ).filter(
        F.col("Age").isNotNull() &
        F.col("Cholesterol Level").isNotNull()
    )
def spark_df_to_dict_list(df):
    return [row.asDict() for row in df.collect()]


def spark_single_row_to_dict(df):
    rows = df.collect()
    if not rows:
        return {}
    return rows[0].asDict()

def run_pipeline(file_path: str) -> dict:
    spark = build_spark_session()

    df = load_data(spark, file_path)
    validate_required_columns(df)
    df = select_required_columns(df)
    df = clean_data(df)

    null_counts_df = count_nulls(df)
    cleaned_df = df

    analysis_df = cleaned_df
    analysis_df = remove_outliers_iqr(analysis_df, "Age")
    analysis_df = remove_outliers_iqr(analysis_df, "Cholesterol Level")

    stats = descriptive_stats(analysis_df)
    grouped = grouped_stats(analysis_df)

    return {
        "summary": stats["summary"],
        "numeric_summary": spark_df_to_dict_list(stats["numeric_summary_df"]),
        "null_counts": spark_single_row_to_dict(null_counts_df),
        "by_gender": spark_df_to_dict_list(grouped["by_gender"]),
        "by_smoking": spark_df_to_dict_list(grouped["by_smoking"]),
        "by_heart_status": spark_df_to_dict_list(grouped["by_heart_status"]),
    }


if __name__ == "__main__":
    file_path = "media/datasets/heart_disease.csv"
    results = run_pipeline(file_path)

    print("=== PIPELINE RUNTIME ===")
    print(results["runtime_seconds"], "seconds")

    print("=== NULL COUNTS ===")
    results["null_counts_df"].show(truncate=False)

    print("=== SUMMARY ===")
    print(results["stats"]["summary"])

    print("=== NUMERIC DESCRIBE ===")
    results["stats"]["numeric_summary_df"].show(truncate=False)

    print("=== BY GENDER ===")
    results["grouped"]["by_gender"].show(truncate=False)

    print("=== BY SMOKING ===")
    results["grouped"]["by_smoking"].show(truncate=False)

    print("=== BY HEART DISEASE STATUS ===")
    results["grouped"]["by_heart_status"].show(truncate=False)

    print("=== CHART DATA SAMPLE ===")
    results["chart_df"].show(10, truncate=False)