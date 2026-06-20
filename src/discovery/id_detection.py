from pyspark.sql.functions import (
    col,
    countDistinct,
    min,
    max
)

def detect_id_columns(
    df,
    candidate_columns
):

    valid_columns = []

    total_rows = df.count()

    for column in candidate_columns:

        distinct_count = (
            df
            .select(column)
            .distinct()
            .count()
        )

        null_count = (
            df
            .filter(df[column].isNull())
            .count()
        )

        if (
            distinct_count == total_rows
            and null_count == 0
        ):
            valid_columns.append(column)

    return valid_columns


def verify_candidate_ids(
    spark_df,
    candidate_columns
):

    total_rows = spark_df.count()

    verified = []

    for column in candidate_columns:

        distinct_count = (
            spark_df
            .select(
                countDistinct(col(column))
                .alias("distinct_count")
            )
            .collect()[0]["distinct_count"]
        )

        if distinct_count == total_rows:
            verified.append(column)

    return verified


def is_sequential_column(
    spark_df,
    column_name
):

    try:

        total_rows = spark_df.count()

        stats = (
            spark_df
            .select(
                min(column_name).alias("min_val"),
                max(column_name).alias("max_val"),
                countDistinct(column_name)
                    .alias("distinct_count")
            )
            .collect()[0]
        )

        return (
            stats["distinct_count"] == total_rows
            and
            stats["max_val"] - stats["min_val"] + 1
            == total_rows
        )

    except:
        return False
    

def get_database_ids(
    spark_df,
    candidate_columns
):

    verified = verify_candidate_ids(
        spark_df,
        candidate_columns
    )

    verified = [
        column
        for column in verified
        if not is_sequential_column(
            spark_df,
            column
        )
    ]

    return verified