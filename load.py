import glob
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

def load_tables(spark, prefix, table_name):
    table_directory = prefix + table_name + "/"
    table_paths = glob.glob(table_directory + '*.tbl')
    switch_dict = {
        'nation': ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
        'part': ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"],
        'customer': ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"],
        'orders': ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"],
        'supplier': ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
        'lineitem': ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", 
                     "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", 
                     "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
                     "l_shipmode", "l_comment"],
        'partsupp': ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
        'region': ["r_regionkey", "r_name", "r_comment"]
    }
    dfs = []
    for table_path in table_paths:
        df = spark.read.option("delimiter", "|").csv(table_path)
        new_column_names = switch_dict[table_name]
        old_column_names = df.columns
        for old_col, new_col in zip(old_column_names, new_column_names):
            df = df.withColumnRenamed(old_col, new_col)
        dfs.append(df)
    
    df = reduce(DataFrame.union, dfs)
    df.createOrReplaceTempView(table_name)
    return

def drop_tables(spark, table_name):
    spark.sql("DROP TABLE IF EXISTS " + table_name)
    return
