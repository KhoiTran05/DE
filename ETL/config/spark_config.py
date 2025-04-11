import os
from pyspark.sql import SparkSession

from typing import Optional, List, Dict

def create_spark_session(
        app_name: str,
        master: str = "local[*]",
        executor_memory: Optional[str] = "4g",
        executor_cores: Optional[int] = 2,
        driver_memory: Optional[str] = "2g",
        executor_instances: Optional[int] = 3,
        jars: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "WARN",
) -> SparkSession:
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)

    if executor_memory:
        builder.config("spark.executor.memory", executor_memory)
    if executor_cores:
        builder.config("spark.executor.cores", executor_cores)
    if driver_memory:
        builder.config("spark.driver.memory", driver_memory)
    if executor_instances:
        builder.config("spark.executor.instances", executor_instances)
    if jars:
        jars_path = ",".join([os.path.abspath(jar) for jar in jars])
        builder.config("spark.jars", jars_path)
    if spark_conf:
        for key, value in spark_conf.items():
            builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark

