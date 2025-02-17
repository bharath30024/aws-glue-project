#!/usr/bin/env python
"""
Main entry point for the AWS Glue job.
Executes the following steps:
1. Reads data from a source.
2. Applies transformations.
3. Writes transformed data to a target.
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
import transformation

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Parse job arguments (expects JOB_NAME, source_path, target_path)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    logger.info("Starting AWS Glue Job")
    logger.info("Reading data from source: %s", args['source_path'])
    df = transformation.read_data(spark, args['source_path'])
    logger.info("Transforming data")
    transformed_df = transformation.transform_data(df)
    logger.info("Writing transformed data to target: %s", args['target_path'])
    transformation.write_data(transformed_df, args['target_path'])
    logger.info("Job completed successfully")
    job.commit()

except Exception as e:
    logger.error("Error during Glue job execution", exc_info=True)
    job.commit()  # Optionally commit or mark failure
    raise e
