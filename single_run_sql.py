from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType, DoubleType

from load import load_tables, drop_tables

import argparse
import time
import numpy as np
import subprocess
import logging
import json
import os

# Create a logger
logger = logging.getLogger("query_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

spark = SparkSession.builder \
    .appName("Running SQL Queries in PySpark") \
    .config("spark.driver.memory", "50g") \
    .config("spark.executor.memory", "200g") \
    .config("spark.driver.memoryOverhead", "25g") \
    .config("spark.executor.memoryOverhead", "50g") \
    .getOrCreate()

logger.info("Start Running Queries")
time.sleep(0.001)

argParser = argparse.ArgumentParser(description="single_run_sql: baseline (no filter)")
argParser.add_argument("-i", "--index", default="1", help="Query Index")
argParser.add_argument("-c", "--cores", default="1", help="Core Number")

# New arguments for directories:
argParser.add_argument("--tmpfs-dir", default="/tmpfs_test/100gb/baseline",
                       help="Path to the tmpfs directory (where tables are copied).")
argParser.add_argument("--chunk-dir", default="/mnt/ssd/chuxuan/100gb/chunk",
                       help="Path to the chunked original data (source).")
argParser.add_argument("--json-dir", default="/home/usps/chuxuanhu/100gb-new",
                       help="Path to the .json files describing the queries/tables.")
argParser.add_argument("--queries-dir", default="/home/usps/chuxuanhu/queries_new",
                       help="Path to the SQL query files.")
argParser.add_argument("--results-dir", default="/home/usps/chuxuanhu/100gb-new",
                       help="Path to store final Spark SQL results.")
argParser.add_argument("--log-file", default="./runningtime_baseline.txt",
                       help="Path to the log file.")

args = argParser.parse_args()

idx = args.index
cores = args.cores
logfile = args.log_file
if not os.path.exists(os.path.dirname(logfile)):
    os.makedirs(os.path.dirname(logfile))

# # Drop caches
# subprocess.call('sudo sysctl vm.drop_caches=3', shell=True)
# subprocess.call(f'vmtouch -e {args.tmpfs_dir}', shell=True)

# q_list = [idx]
# for i in q_list:
#     subprocess.call('sudo sysctl vm.drop_caches=3', shell=True)
#     subprocess.call(f'vmtouch -e {args.tmpfs_dir}', shell=True)
#     logger.info("Start Running Query "+str(i))
#     time.sleep(0.001)

#     with open(logfile, 'a') as f:
#         f.write("Start Running Query "+str(i)+"\n")

#     start_time = time.time()

#     # Path to the SQL
#     sql_file_path = os.path.join(args.queries_dir, f"{i}.sql")

#     # Make a subdirectory in tmpfs for this query
#     tmpfs_query_dir = os.path.join(args.tmpfs_dir, f"q{i}")
#     subprocess.call(f"mkdir -p {tmpfs_query_dir}", shell=True)

#     # Read the JSON describing which tables to copy
#     json_path = os.path.join(args.json_dir, f"{i}.json")
#     with open(json_path, "r") as json_file:
#         tables = json.load(json_file)["tables"]

#     # ---------------- Copying Tables ----------------
#     total_copy_start = time.time()
#     for table in tables:
#         logger.info("Copying Table " + table)
#         copy_start_time = time.time()

#         table_dir = os.path.join(tmpfs_query_dir, table)
#         subprocess.call(f"mkdir -p {table_dir}", shell=True)
#         subprocess.call(f"cp {args.chunk_dir}/{table}/*.tbl {table_dir}/", shell=True)

#         copying_time = time.time() - copy_start_time
#         logger.info(f"Copying Table {table} takes {copying_time} seconds.")
#         with open(logfile, 'a') as f:
#             f.write(f"Copying Table {table} takes {copying_time} seconds.\n")
#     total_copying_time = time.time() - total_copy_start
#     logger.info(f"Total Copying Table takes {total_copying_time} seconds.")
#     with open(logfile, 'a') as f:
#         f.write(f"Total Copying Table takes {total_copying_time} seconds.\n")

#     # ---------------- Loading Tables ----------------
#     sql_start_time = time.time()
#     for table in tables:
#         logger.info("Loading Table "+ table)
#         load_tables(spark, tmpfs_query_dir + "/", table)

#     # ---------------- Reading & Running SQL ----------------
#     with open(sql_file_path) as sf:
#         sql_query = sf.read()
#     logger.info("Start running SQL "+str(i))
#     time.sleep(0.001)

#     result = spark.sql(sql_query)

#     # Write results
#     output_csv_dir = os.path.join(args.results_dir, f"{i}_{cores}_result_baseline")
#     (result.coalesce(1)
#            .write
#            .option("header", "true")
#            .option("delimiter", "|")
#            .mode("overwrite")
#            .csv(output_csv_dir))

#     sql_time = time.time() - sql_start_time
#     logger.info(f"SQL {i} finishes running in {sql_time} seconds")
#     time.sleep(0.001)
#     with open(logfile, 'a') as f:
#         f.write(f"SQL {i} finishes running in {sql_time} seconds\n")
        
#     # Clean-ups
#     for table in tables:
#         drop_tables(spark, table)
#     subprocess.call(f"rm -r {tmpfs_query_dir}", shell=True)

#     total_time = time.time() - start_time
#     logger.info(f"Total time for SQL {i} takes {total_time} seconds.")
#     with open(logfile, 'a') as f:
#         f.write(f"Total time for SQL {i} takes {total_time} seconds.\n")
