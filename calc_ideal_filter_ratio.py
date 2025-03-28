
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
import time
import logging
import os
import glob


logger = logging.getLogger("query_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

spark = SparkSession.builder \
    .appName("Running SQL Queries in PySpark") \
    .config("spark.driver.memory", "50g") \
    .config("spark.executor.memory", "200g") \
    .config("spark.driver.memoryOverhead", "25g") \
    .config("spark.executor.memoryOverhead", "50g") \
    .config("spark.driver.maxResultSize", "50g") \
    .getOrCreate()

from load import load_tables, drop_tables
chunk_dir = "/mnt/ssd/chuxuan/100gb/chunk/"
load_tables(spark, chunk_dir, "lineitem")
load_tables(spark, chunk_dir, "orders")
load_tables(spark, chunk_dir, "partsupp")

def get_total_size(file_list):
    total_size = 0
    for f in file_list:
        try:
            total_size += os.path.getsize(f)
        except OSError as e:
            logger.error(f"Error getting size for {f}: {e}")
    return total_size

ratio_log_file = "./filtered_ratio_log.txt"
with open(ratio_log_file, 'w') as f:
    f.write("Filtered Ratio Log\n")


original_sizes = {}
for table in ["lineitem", "orders", "partsupp"]:
    files = glob.glob(os.path.join(chunk_dir, table, "*.tbl"))
    original_sizes[table] = get_total_size(files)


def run_query(query_name, table, query_str, output_path, num_partitions, ratio_log_file):
    logger.info(f"Starting {query_name} on {table}")
    
    result = spark.sql(query_str)
    
    result_repartitioned = result.repartition(num_partitions)
    result_concatenated = result_repartitioned.select(
        concat_ws("|", *result_repartitioned.columns).alias("res")
    )
    result_concatenated.write.mode("overwrite").option("sep", "|").text(output_path)
    
    time.sleep(2)
    
    filtered_files = glob.glob(os.path.join(output_path, "part-*"))
    filtered_size = get_total_size(filtered_files)
    
    original_size = original_sizes.get(table, 0)
    
    ratio = filtered_size / original_size if original_size > 0 else 0
    
    with open(ratio_log_file, 'a') as f:
        f.write(f"{query_name} on {table}: filtered size = {filtered_size} bytes, "
                f"original size = {original_size} bytes, ratio = {ratio:.2%}\n")
    
    logger.info(f"Finished {query_name} on {table} with ratio = {ratio:.2%}")


# Query 1
run_query("q1_lineitem", "lineitem",
          "select * from lineitem where l_shipdate <= date '1993-09-01' - interval '90' day",
          "/home/usps/chuxuanhu/100gb-new/SW/q1_lineitem", 150, ratio_log_file)
logger.info("q1 done")

# Query 2
run_query("q2_partsupp", "partsupp",
          "select * from partsupp where ps_availqty < 1405",
          "/home/usps/chuxuanhu/100gb-new/SW/q2_partsupp", 24, ratio_log_file)
logger.info("q2 done")

# Query 3 (two parts)
run_query("q3_lineitem", "lineitem",
          "select * from lineitem where l_shipmode = 'MAIL'",
          "/home/usps/chuxuanhu/100gb-new/SW/q3_lineitem", 150, ratio_log_file)
run_query("q3_orders", "orders",
          "select * from orders where o_orderdate < date '1993-04-15'",
          "/home/usps/chuxuanhu/100gb-new/SW/q3_orders", 35, ratio_log_file)
logger.info("q3 done")

# Query 4 (two parts)
run_query("q4_lineitem", "lineitem",
          "select * from lineitem where l_returnflag = 'R'",
          "/home/usps/chuxuanhu/100gb-new/SW/q4_lineitem", 150, ratio_log_file)
run_query("q4_orders", "orders",
          "select * from orders where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month",
          "/home/usps/chuxuanhu/100gb-new/SW/q4_orders", 35, ratio_log_file)
logger.info("q4 done")

# Query 5 (two parts)
run_query("q5_lineitem", "lineitem",
          "select * from lineitem where l_returnflag = 'R'",
          "/home/usps/chuxuanhu/100gb-new/SW/q5_lineitem", 150, ratio_log_file)
run_query("q5_orders", "orders",
          "select * from orders where o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year",
          "/home/usps/chuxuanhu/100gb-new/SW/q5_orders", 35, ratio_log_file)
logger.info("q5 done")

# Query 6
run_query("q6_lineitem", "lineitem",
          "select * from lineitem where (l_discount between 0.05 and 0.07) and (l_quantity < 24) and "
          "(l_shipdate >= date '1994-01-01') and (l_shipdate < date '1994-01-01' + interval '1' year)",
          "/home/usps/chuxuanhu/100gb-new/SW/q6_lineitem", 150, ratio_log_file)
logger.info("q6 done")

# Query 7 (two parts)
run_query("q7_lineitem", "lineitem",
          "select * from lineitem where l_shipdate between date '1996-01-01' and date '1996-12-31'",
          "/home/usps/chuxuanhu/100gb-new/SW/q7_lineitem", 150, ratio_log_file)
run_query("q7_orders", "orders",
          "select * from orders where o_orderpriority = '1-URGENT'",
          "/home/usps/chuxuanhu/100gb-new/SW/q7_orders", 35, ratio_log_file)
logger.info("q7 done")

# Query 8 (two parts)
run_query("q8_lineitem", "lineitem",
          "select * from lineitem where l_shipmode = 'MAIL'",
          "/home/usps/chuxuanhu/100gb-new/SW/q8_lineitem", 150, ratio_log_file)
run_query("q8_orders", "orders",
          "select * from orders where o_orderdate between date '1996-01-01' and date '1996-12-31'",
          "/home/usps/chuxuanhu/100gb-new/SW/q8_orders", 35, ratio_log_file)
logger.info("q8 done")

# Query 9 (three parts)
run_query("q9_lineitem", "lineitem",
          "select * from lineitem where l_shipdate > date '1997-06-15'",
          "/home/usps/chuxuanhu/100gb-new/SW/q9_lineitem", 150, ratio_log_file)
run_query("q9_orders", "orders",
          "select * from orders where o_orderpriority = '1-URGENT'",
          "/home/usps/chuxuanhu/100gb-new/SW/q9_orders", 35, ratio_log_file)
run_query("q9_partsupp", "partsupp",
          "select * from partsupp where ps_availqty < 1800",
          "/home/usps/chuxuanhu/100gb-new/SW/q9_partsupp", 24, ratio_log_file)
logger.info("q9 done")

# Query 10 (two parts)
run_query("q10_lineitem", "lineitem",
          "select * from lineitem where l_returnflag = 'R'",
          "/home/usps/chuxuanhu/100gb-new/SW/q10_lineitem", 150, ratio_log_file)
run_query("q10_orders", "orders",
          "select * from orders where o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month",
          "/home/usps/chuxuanhu/100gb-new/SW/q10_orders", 35, ratio_log_file)
logger.info("q10 done")

# Query 11
run_query("q11_partsupp", "partsupp",
          "select * from partsupp where ps_availqty < 1800",
          "/home/usps/chuxuanhu/100gb-new/SW/q11_partsupp", 24, ratio_log_file)
logger.info("q11 done")

# Query 12 (two parts)
run_query("q12_lineitem", "lineitem",
          "select * from lineitem where (l_shipmode in ('MAIL', 'SHIP')) and "
          "l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year",
          "/home/usps/chuxuanhu/100gb-new/SW/q12_lineitem", 150, ratio_log_file)
run_query("q12_orders", "orders",
          "select * from orders where o_orderpriority = '1-URGENT'",
          "/home/usps/chuxuanhu/100gb-new/SW/q12_orders", 35, ratio_log_file)
logger.info("q12 done")

# Query 13
run_query("q13_orders", "orders",
          "select * from orders where o_orderdate < date '1993-04-15'",
          "/home/usps/chuxuanhu/100gb-new/SW/q13_orders", 35, ratio_log_file)
logger.info("q13 done")

# Query 14
run_query("q14_lineitem", "lineitem",
          "select * from lineitem where l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month",
          "/home/usps/chuxuanhu/100gb-new/SW/q14_lineitem", 150, ratio_log_file)
logger.info("q14 done")

# Query 15
run_query("q15_lineitem", "lineitem",
          "select * from lineitem where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month",
          "/home/usps/chuxuanhu/100gb-new/SW/q15_lineitem", 150, ratio_log_file)
logger.info("q15 done")

# Query 16
run_query("q16_partsupp", "partsupp",
          "select * from partsupp where ps_availqty < 1800",
          "/home/usps/chuxuanhu/100gb-new/SW/q16_partsupp", 24, ratio_log_file)
logger.info("q16 done")

# Query 17
run_query("q17_lineitem", "lineitem",
          "select * from lineitem where l_shipdate <= date '1993-09-01' - interval '90' day",
          "/home/usps/chuxuanhu/100gb-new/SW/q17_lineitem", 150, ratio_log_file)
logger.info("q17 done")

# Query 18 (two parts)
run_query("q18_lineitem", "lineitem",
          "select * from lineitem where l_shipdate <= date '1993-09-01' - interval '90' day",
          "/home/usps/chuxuanhu/100gb-new/SW/q18_lineitem", 150, ratio_log_file)
run_query("q18_orders", "orders",
          "select * from orders where o_orderpriority = '1-URGENT'",
          "/home/usps/chuxuanhu/100gb-new/SW/q18_orders", 35, ratio_log_file)
logger.info("q18 done")

# Query 19
run_query("q19_lineitem", "lineitem",
          "select * from lineitem where (l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' "
          "and l_quantity >= 20 and l_quantity <= 20 + 10) or (l_shipmode in ('AIR', 'AIR REG') and "
          "l_shipinstruct = 'DELIVER IN PERSON' and l_quantity >= 10 and l_quantity <= 10 + 5) or (l_shipmode in "
          "('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' and l_quantity >= 5 and l_quantity <= 1 + 10)",
          "/home/usps/chuxuanhu/100gb-new/SW/q19_lineitem", 150, ratio_log_file)
logger.info("q19 done")

# Query 20 (two parts)
run_query("q20_lineitem", "lineitem",
          "select * from lineitem where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year",
          "/home/usps/chuxuanhu/100gb-new/SW/q20_lineitem", 150, ratio_log_file)
run_query("q20_partsupp", "partsupp",
          "select * from partsupp where ps_availqty < 1800",
          "/home/usps/chuxuanhu/100gb-new/SW/q20_partsupp", 24, ratio_log_file)
logger.info("q20 done")

# Query 21 (two parts)
run_query("q21_lineitem", "lineitem",
          "select * from lineitem where l_returnflag = 'R'",
          "/home/usps/chuxuanhu/100gb-new/SW/q21_lineitem", 150, ratio_log_file)
run_query("q21_orders", "orders",
          "select * from orders where o_orderpriority = '1-URGENT'",
          "/home/usps/chuxuanhu/100gb-new/SW/q21_orders", 35, ratio_log_file)
logger.info("q21 done")

# Query 22
run_query("q22_orders", "orders",
          "select * from orders where o_orderdate < date '1993-04-15'",
          "/home/usps/chuxuanhu/100gb-new/SW/q22_orders", 35, ratio_log_file)
logger.info("q22 done")
