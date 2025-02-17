"""
Transformation Module:
- Reads input CSV data from S3.
- Applies a sample transformation (using a NumPy UDF and Pandas).
- Writes output CSV data back to S3.
"""

import logging
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)

def read_data(spark, source_path: str) -> DataFrame:
    try:
        df = spark.read.option("header", "true").csv(source_path)
        logger.info("Successfully read data from %s", source_path)
        return df
    except Exception as e:
        logger.error("Error reading data from %s", source_path, exc_info=True)
        raise

def transform_data(df: DataFrame) -> DataFrame:
    try:
        def compute_sqrt(x):
            try:
                return float(np.sqrt(float(x))) if x is not None else None
            except Exception:
                return None

        sqrt_udf = udf(compute_sqrt, DoubleType())
        df_with_sqrt = df.withColumn("value_sqrt", sqrt_udf(col("value")))
        pandas_df = df_with_sqrt.toPandas()
        pandas_df['value'] = pandas_df['value'].fillna(0)
        spark_df = df.sparkSession.createDataFrame(pandas_df)
        logger.info("Data transformation completed successfully")
        return spark_df

    except Exception as e:
        logger.error("Error during data transformation", exc_info=True)
        raise

def write_data(df: DataFrame, target_path: str) -> None:
    try:
        df.write.mode("overwrite").option("header", "true").csv(target_path)
        logger.info("Data successfully written to %s", target_path)
    except Exception as e:
        logger.error("Error writing data to %s", target_path, exc_info=True)
        raise
