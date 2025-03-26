import argparse
import bisect
import os
import tempfile
from tqdm import tqdm
import numpy as np
import pandas as pd
import struct
from datetime import datetime, timedelta
import hashlib

# Nation table (4 columns)
NATION_TABLE_COLUMN_COUNT = 4
N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT = range(NATION_TABLE_COLUMN_COUNT)

# Region table (3 columns)
REGION_TABLE_COLUMN_COUNT = 3
R_REGIONKEY, R_NAME, R_COMMENT = range(REGION_TABLE_COLUMN_COUNT)

# Part table (9 columns)
PART_TABLE_COLUMN_COUNT = 9
P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT = range(PART_TABLE_COLUMN_COUNT)

# Supplier table (7 columns)
SUPPLIER_TABLE_COLUMN_COUNT = 7
S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT = range(SUPPLIER_TABLE_COLUMN_COUNT)

# Partsupp table (5 columns)
PARTSUPP_TABLE_COLUMN_COUNT = 5
PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT = range(PARTSUPP_TABLE_COLUMN_COUNT)

# Customer table (8 columns)
CUSTOMER_TABLE_COLUMN_COUNT = 8
C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT = range(CUSTOMER_TABLE_COLUMN_COUNT)

# Orders table (9 columns)
ORDER_TABLE_COLUMN_COUNT = 9
O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT = range(ORDER_TABLE_COLUMN_COUNT)

# Lineitem table (16 columns)
LINEITEM_TABLE_COLUMN_COUNT = 16
L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, \
L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, \
L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT = range(LINEITEM_TABLE_COLUMN_COUNT)

###############################################################################
# 1. HELPER FUNCTIONS
###############################################################################

def is_date(val, date_format="%Y-%m-%d"):
    try:
        dt = datetime.strptime(val, date_format)
        return dt.timestamp()
    except ValueError:
        return None

def is_float(string):
    try:
        return float(string)
    except ValueError:
        return None

def hash_numerical(val, precentile):
    idx = bisect.bisect_left(precentile, val)
    return [idx]

def hash_string(val, size):
    tokens = val.split()
    if len(tokens) > 255:
        raise ValueError("Line is too long for 256-bit tokenization requirement.")
    res = []
    for token in tokens:
        h = hashlib.sha256()
        h.update(token.encode('utf-8'))
        digest_hex = h.hexdigest()
        last_byte = int(digest_hex[-2:], 16)
        pos = last_byte % size
        res.append(pos)
    return list(set(res))

def bits_to_bytes(bitstring):
    """
    Convert a bitstring (a string of '0's and '1's) to bytes.
    
    This function automatically pads the bitstring if it is shorter than the expected full length.
    
    For a data hash:
      - Expected full length is 256 bits.
      - If the generated bitstring is shorter than 256 bits, it is padded with zeros to 256.
      
    For a query hash:
      - The bitstring is expected to be composed of 12 chunks (one per query snippet).
      - Each chunk is originally of length `size` (user-specified).
      - If size < 256, then each chunk is padded with zeros to 256 bits.
      - The final bitstring will have length 256 * 12.
      
    This function distinguishes between the two cases by checking the total length.
    """
    if len(bitstring) <= 256:
        bitstring = bitstring.ljust(256, '0')
    elif len(bitstring) < 256 * 12:
        num_chunks = 12
        chunk_len = len(bitstring) // num_chunks
        new_chunks = []
        for i in range(num_chunks):
            chunk = bitstring[i * chunk_len : (i + 1) * chunk_len]
            padded_chunk = chunk.ljust(256, '0')
            new_chunks.append(padded_chunk)
        bitstring = "".join(new_chunks)
    nbytes = len(bitstring) // 8
    out = bytearray(nbytes)
    for i in range(nbytes):
        chunk = bitstring[i*8:(i+1)*8]
        val = 0
        for b in range(8):
            if chunk[b] == '1':
                val |= (1 << b)
        out[i] = val
    return bytes(out)

###############################################################################
# 2. DATA-HASH LOGIC
###############################################################################

def hash_row(row_string, percentiles, size=256, delimiter='|'):
    fields = row_string.strip().split(delimiter)[:-1]
    bitarray = ['0']*size
    size_per_col = size // len(fields)
    for colidx, val in enumerate(fields):
        precentile = percentiles[colidx]

        fval = is_float(val)
        if fval is not None:
            positions = hash_numerical(fval, precentile)
        else:
            dval = is_date(val)
            if dval is not None:
                positions = hash_numerical(dval, precentile)
            else:
                positions = hash_string(val, size_per_col)

        for p in positions:
            bitarray[p + (colidx) * size_per_col] = '1'
            
    bitstring = "".join(bitarray)
    if len(bitstring) < 256:
        bitstring += '0' * (256 - len(bitstring))
    return bitstring


def hash_row_q14(row_string, size=256, delimiter='|'):
    fields = row_string.strip().split(delimiter)[:-1]
    bitarray = ['0'] * size
    size_per_col = size // 16
    
    try:
        date_val = fields[L_SHIPDATE]
    except IndexError:
        raise ValueError("Row does not have enough columns for lineitem.")
    
    dt_timestamp = is_date(date_val)
    lower_ts = datetime(1995, 9, 1).timestamp()
    upper_ts = datetime(1995, 10, 1).timestamp()
    
    if dt_timestamp is not None and lower_ts <= dt_timestamp < upper_ts:
        start = L_SHIPDATE * size_per_col
        end = start + size_per_col
        for i in range(start, end):
            bitarray[i] = '1'
    
    return "".join(bitarray)


def create_data_hash(input_csv, output_hash_bin, output_len_bin=None, percentiles_path=None,
                     delimiter='|', size=256):
    try:
        percentiles = np.load(percentiles_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Percentile file not found at {percentiles_path}, generate it first.")

    with open(input_csv, 'r', encoding='utf-8') as fin, \
         open(output_hash_bin, 'wb') as fhash:
        flen = None
        if output_len_bin:
            flen = open(output_len_bin, 'wb')

        for line in tqdm(fin, desc="Hashing rows"):
            trimmed = line.strip()
            if not trimmed:
                continue
            bitstring = hash_row_q14(line, size=size, delimiter=delimiter)
            row_bytes = bits_to_bytes(bitstring)
            fhash.write(row_bytes)

            if flen:
                rowlen = len(line.encode('utf-8'))
                if rowlen > 255:
                    raise ValueError("Line length > 255, cannot store in 1-byte length.")
                flen.write(struct.pack('B', rowlen))

        if flen:
            flen.close()

###############################################################################
# 3. QUERY-HASH LOGIC
###############################################################################

def create_query_hash(output_dir, base_dir, size=256):
    lineitem_percentiles = np.load(os.path.join(base_dir, "lineitem_percentiles.npy"))
    partsupp_percentiles = np.load(os.path.join(base_dir, "partsupp_percentiles.npy"))
    orders_percentiles = np.load(os.path.join(base_dir, "orders_percentiles.npy"))

    #########################################
    # Q1 snippet
    #########################################

    q1opsMask = "010000000000"
    assert len(q1opsMask) == 12
    q19_lineitem_opsMask = size // LINEITEM_TABLE_COLUMN_COUNT
    hashbits = ['0'] * size

    given_date = datetime(1993, 9, 1)
    result_date = given_date - timedelta(days=90)
    val = result_date.timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]
    res = hash_numerical(val, l_shipdate_percentiles)[0]
    for i in range(res + 1):
        idx = i + L_SHIPDATE * q19_lineitem_opsMask
        hashbits[idx] = '1'
    result_shipdate = "".join(hashbits)

    hashbits2 = ['0']*size
    # val = "accounts"
    # res_list = hash_string(val, q19_lineitem_opsMask)
    # for res in res_list:
    #     hashbits2[res + L_COMMENT * q19_lineitem_opsMask] = '1'
    result_lineitem = "".join(hashbits2)

    result_q1 = result_lineitem + result_shipdate + ('0' * size * 10)
    assert len(result_q1) == size * 12, f"Expected final bitstring of length {size*12}, got {len(result_q1)}"

    out_path_q1 = os.path.join(output_dir, "query_lineitem_hash_q1.bin")
    with open(out_path_q1, 'wb') as f_out:
        f_out.write(bits_to_bytes(result_q1))
    print(f"[INFO] Wrote Q1 => {out_path_q1}")

    #########################################
    # Q2 snippet
    #########################################

    q2opsMask = "010000000000"
    assert len(q2opsMask) == 12
    q19_lineitem_opsMask = size // PARTSUPP_TABLE_COLUMN_COUNT
    hashbits = ['0'] * size
    val = 1800

    partsupp_availqty_percentiles = partsupp_percentiles[PS_AVAILQTY]

    res = hash_numerical(val, partsupp_availqty_percentiles)[0]
    hashbits = ['0'] * size
    for i in range(res + 1):
        idx = i + PS_AVAILQTY * q19_lineitem_opsMask
        hashbits[idx] = '1'
    result_ps_availqty = "".join(hashbits)

    hashbits2 = ['0'] * size
    # val = "even"
    # res_list = hash_string(val, q19_lineitem_opsMask)
    # for res in res_list:
    #     hashbits2[res + PS_COMMENT * q19_lineitem_opsMask] = '1'
    result_ps_comment = "".join(hashbits2)
    
    result_q2 = result_ps_comment + result_ps_availqty + '0' * size * 10
    assert len(result_q2) == size * 12

    out_path_q2 = os.path.join(output_dir, "query_partsupp_hash_q2.bin")
    with open(out_path_q2, 'wb') as f_out:
        f_out.write(bits_to_bytes(result_q2))
    print(f"[INFO] Wrote Q2 => {out_path_q2}")

    #########################################
    # Q3 snippet (orders)
    #########################################

    q3_orders_opsMask = "010000000000"
    assert len(q3_orders_opsMask) == 12
    hashbits = ['0']*size
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    order_date = datetime(1993, 4, 15)
    val = order_date.timestamp()

    orderdate_percentiles = orders_percentiles[O_ORDERDATE]

    res = hash_numerical(val, orderdate_percentiles)[0]

    for i in range(res + 1):
        idx = i + O_ORDERDATE * k_order
        hashbits[idx] = '1'

    result_order = "".join(hashbits)
    result = '0'* size + result_order + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q3_order = os.path.join(output_dir, "query_orders_hash_q3.bin")
    with open(out_path_q3_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q3 (Orders) => {out_path_q3_order}")

    #########################################
    # Q3 snippet (lineitem)
    #########################################

    q3_lineitem_opsMask = "100000000000"
    assert len(q3_lineitem_opsMask) == 12
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
        
    text_val = 'MAIL'
    res1 = hash_string(text_val, k_lineitem)[0]

    # Initialize hashbits for l_shipmode = 'MAIL'
    hashbits_shipmode = ['0'] * size
    hashbits_shipmode[res1 + L_SHIPMODE * k_lineitem] = '1'
    result_comment = "".join(hashbits_shipmode)

    l_shipdate_cutoff = datetime(1993, 4, 15).timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]

    res = hash_numerical(l_shipdate_cutoff, l_shipdate_percentiles)[0]

    hashbits_shipdate = ['0'] * size
    # for i in range(res + 1, k_lineitem):
    #     idx = i + L_SHIPDATE * k_lineitem
    #     hashbits_shipdate[idx] = '1'
    result_shipdate = "".join(hashbits_shipdate)

    result = result_comment + result_shipdate + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q3_lineitem = os.path.join(output_dir, "query_lineitem_hash_q3.bin")
    with open(out_path_q3_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q3 (Lineitem) => {out_path_q3_lineitem}")

    #########################################
    # Q4 snippet (order)
    #########################################

    q4_order_opsMask = "010000000000"
    assert len(q4_order_opsMask) == 12
    
    order_date = datetime(1993, 7, 1)
    val_lower = order_date.timestamp()
    result_date = datetime(1993, 10, 1)
    val_higher = result_date.timestamp()

    order_date_percentiles = orders_percentiles[O_ORDERDATE]

    res_lower = hash_numerical(val_lower, order_date_percentiles)[0]
    res_higher = hash_numerical(val_higher, order_date_percentiles)[0]
    hashbits = ['0'] * size
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    for i in range(res_lower, res_higher+1): # exclusive
        idx = i + O_ORDERDATE * k_order
        hashbits[idx] = '1'

    result_order = "".join(hashbits)

    result = '0'* size + result_order + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q4_order = os.path.join(output_dir, "query_orders_hash_q4.bin")
    with open(out_path_q4_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q4 (Orders) => {out_path_q4_order}")

    #########################################
    # Q4 snippet (lineitem)
    #########################################

    q4_lineitem_opsMask = "100000000000"
    assert len(q4_lineitem_opsMask) == 12

    val = "R"
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    res = hash_string(val, k_lineitem)[0]
    hashbits = ['0']*size
    hashbits[res + L_RETURNFLAG * k_lineitem] = '1'
    result_lineitem = "".join(hashbits)

    val_lower = 0.05
    val_higher = 1.0

    l_discount_percentiles = lineitem_percentiles[L_DISCOUNT]

    res_lower_discount = hash_numerical(val_lower, l_discount_percentiles)[0]
    res_higher_discount = hash_numerical(val_higher, l_discount_percentiles)[0]

    hashbits_discount = ['0'] * size
    # for i in range(res_lower_discount, res_higher_discount + 1):
    #     idx = i + L_DISCOUNT * k_lineitem
    #     hashbits_discount[idx] = '1'
    result_discount = "".join(hashbits_discount)

    result = result_lineitem + result_discount + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q4_lineitem = os.path.join(output_dir, "query_lineitem_hash_q4.bin")
    with open(out_path_q4_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q4 (Lineitem) => {out_path_q4_lineitem}")

    #########################################
    # Q5 snippet (orders)
    #########################################

    q5_order_opsMask = "010000000000"
    assert len(q5_order_opsMask) == 12

    val_lower = datetime(1994, 1, 1).timestamp()
    val_higher = datetime(1995, 1, 1).timestamp()

    order_date_percentiles = orders_percentiles[O_ORDERDATE]

    res_lower = hash_numerical(val_lower, order_date_percentiles)[0]
    res_higher = hash_numerical(val_higher, order_date_percentiles)[0]
    hashbits = ['0']*size
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    for i in range(res_lower, res_higher+1): # exclusive
        idx = i + O_ORDERDATE * k_order
        hashbits[idx] = '1'
    
    result_order = "".join(hashbits)

    result = '0'* size + result_order + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q5_order = os.path.join(output_dir, "query_orders_hash_q5.bin")
    with open(out_path_q5_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q5 (Orders) => {out_path_q5_order}")

    #########################################
    # Q5 snippet (lineitem)
    #########################################

    q5_lineitem_opsMask = "100000000000"
    assert len(q5_lineitem_opsMask) == 12

    val = "R"
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT

    res = hash_string(val, k_lineitem)[0]
    hashbits = ['0']*size
    hashbits[res + k_lineitem * L_RETURNFLAG] = '1'
    result_lineitem = "".join(hashbits)

    val_lower = 0.05
    val_higher = 1.0

    l_discount_percentiles = lineitem_percentiles[L_DISCOUNT]
    res_lower_discount = hash_numerical(val_lower, l_discount_percentiles)[0]
    res_higher_discount = hash_numerical(val_higher, l_discount_percentiles)[0]

    hashbits_discount = ['0'] * size
    # for i in range(res_lower_discount, res_higher_discount + 1):
    #     idx = i + L_DISCOUNT * k_lineitem
    #     hashbits_discount[idx] = '1'
    result_discount = "".join(hashbits_discount)


    result = result_lineitem + result_discount + '0' * size * 10
    assert len(result) == size * 12

    out_path_q5_lineitem = os.path.join(output_dir, "query_lineitem_hash_q5.bin")
    with open(out_path_q5_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q5 (Lineitem) => {out_path_q5_lineitem}")

    #########################################
    # Q6 snippet (lineitem)
    #########################################
    q6_opsMask = "010000000000"
    assert len(q6_opsMask) == 12

    val_lower = datetime(1994, 1, 1).timestamp()
    val_higher = datetime(1995, 1, 1).timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]

    res_lower = hash_numerical(val_lower, l_shipdate_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_shipdate_percentiles)[0]
    hashbits = ['0']*size
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    for i in range(res_lower, res_higher+1):
        idx = i + L_SHIPDATE * k_lineitem
        hashbits[idx] = '1'
    result_shipdate = "".join(hashbits)

    comment = "account"
    res = hash_string(comment, k_lineitem)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + L_COMMENT * k_lineitem
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_shipdate + '0' * size * 10
    assert len(result) == size * 12

    out_path_q6_lineitem = os.path.join(output_dir, "query_lineitem_hash_q6.bin")
    with open(out_path_q6_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q6 (Lineitem) => {out_path_q6_lineitem}")
    
    #########################################
    # Q7 snippet (lineitem)
    #########################################
    q7_lineitem_opsMask = "010000000000"
    assert len(q7_lineitem_opsMask) == 12

    val_lower = datetime(1996, 1, 1).timestamp()
    val_higher = datetime(1996, 12, 31).timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]

    res_lower = hash_numerical(val_lower, l_shipdate_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_shipdate_percentiles)[0]
    hashbits = ['0']*size
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    for i in range(res_lower, res_higher+1):
        idx = i + L_SHIPDATE * k_lineitem
        hashbits[idx] = '1'
    result_lineitem = "".join(hashbits)

    comment = "express"
    res = hash_string(comment, k_lineitem)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + L_COMMENT * k_lineitem
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_lineitem + '0' * size * 10
    assert len(result) == size * 12

    out_path_q7_lineitem = os.path.join(output_dir, "query_lineitem_hash_q7.bin")
    with open(out_path_q7_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q7 (Lineitem) => {out_path_q7_lineitem}")

    #########################################
    # Q7 snippet (orders)
    #########################################
    q7_order_opsMask = "100000000000"
    assert len(q7_order_opsMask) == 12
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    o_priority = '1-URGENT'
    res = hash_string(o_priority, k_order)[0]

    hashbits = ['0']*size
    hashbits[res + O_ORDERPRIORITY * k_order] = '1'
    res_order_priority = "".join(hashbits)

    result = res_order_priority + '0' * size * 11
    assert len(result) == size * 12
    
    out_path_q7_order = os.path.join(output_dir, "query_orders_hash_q7.bin")
    with open(out_path_q7_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q7 (Orders) => {out_path_q7_order}")
    #########################################
    # Q8 snippet (orders)
    #########################################
    q8_orders_opsMask = "010000000000"
    assert len(q8_orders_opsMask) == 12

    val_lower = datetime(1996, 1, 1).timestamp()
    val_higher = datetime(1996, 12, 31).timestamp()

    order_date_percentiles = orders_percentiles[O_ORDERDATE]
    res_lower = hash_numerical(val_lower, order_date_percentiles)[0]
    res_higher = hash_numerical(val_higher, order_date_percentiles)[0]
    hashbits = ['0']*size
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    for i in range(res_lower, res_higher+1): # exclusive
        idx = i + O_ORDERDATE * k_order
        hashbits[idx] = '1'

    result_order = "".join(hashbits)

    hashbits2 = ['0']*size
    # res_list = hash_string("1-URGENT", k_order)
    # for res in res_list:
    #     hashbits2[res + O_ORDERPRIORITY * k_order] = '1'

    result_order_comment = "".join(hashbits2)

    result = result_order_comment + result_order + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q8_order = os.path.join(output_dir, "query_orders_hash_q8.bin")
    with open(out_path_q8_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q8 (Orders) => {out_path_q8_order}")

    #########################################
    # Q8 snippet (lineitem)
    #########################################
    q8_lineitem_opsMask = "100000000000"
    assert len(q8_lineitem_opsMask) == 12
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT

    val1 = 'MAIL'
    res1 = hash_string(val1, k_lineitem)[0]

    hashbits = ['0']*size
    hashbits[res1 + L_SHIPMODE * k_lineitem] = '1'

    result_shipmode = "".join(hashbits)

    val_lower = 0.05
    val_higher = 1.0

    l_discount_percentiles = lineitem_percentiles[L_DISCOUNT]
    res_lower_discount = hash_numerical(val_lower, l_discount_percentiles)[0]
    res_higher_discount = hash_numerical(val_higher, l_discount_percentiles)[0]

    hashbits_discount = ['0'] * size
    # for i in range(res_lower_discount, res_higher_discount + 1):
    #     idx = i + L_DISCOUNT * k_lineitem
    #     hashbits_discount[idx] = '1'
    result_discount = "".join(hashbits_discount)

    result = result_shipmode + result_discount + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q8_lineitem = os.path.join(output_dir, "query_lineitem_hash_q8.bin")
    with open(out_path_q8_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q8 (Lineitem) => {out_path_q8_lineitem}")

    #########################################
    # Q9 snippet (lineitem)
    #########################################
    q9_lineitem_opsMask = "010000000000"
    assert len(q9_lineitem_opsMask) == 12
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT

    lineitem_date = datetime(1997, 6, 15)
    val = lineitem_date.timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]

    res = hash_numerical(val, l_shipdate_percentiles)[0]

    hashbits = ['0']*size
    
    for i in range(res, k_lineitem):
        idx = i + L_SHIPDATE * k_lineitem
        hashbits[idx] = '1'
    result_lineitem = "".join(hashbits)

    comment = "regular"
    res = hash_string(comment, k_lineitem)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + L_COMMENT * k_lineitem
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_lineitem + '0' * size * 10
    assert len(result) == size * 12

    out_path_q9_lineitem = os.path.join(output_dir, "query_lineitem_hash_q9.bin")
    with open(out_path_q9_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q9 (Lineitem) => {out_path_q9_lineitem}")

    #########################################
    # Q9 snippet (orders)
    #########################################
    q9_order_opsMask = "100000000000"
    assert len(q9_order_opsMask) == 12
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    val1 = '1-URGENT'
    res1 = hash_string(val1, k_order)[0]

    hashbits = ['0']*size
    hashbits[res1 + O_ORDERPRIORITY * k_order] = '1'
    result_o_orderpriority = "".join(hashbits)
    result = result_o_orderpriority + '0' * size * 11
    assert len(result) == size * 12

    out_path_q9_order = os.path.join(output_dir, "query_orders_hash_q9.bin")
    with open(out_path_q9_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q9 (Orders) => {out_path_q9_order}")

    #########################################
    # Q9 snippet (partsupp)
    #########################################
    q9_partsupp_opsMask = "010000000000"
    assert len(q9_partsupp_opsMask) == 12
    k_partsupp = size // PARTSUPP_TABLE_COLUMN_COUNT

    val = 1800
    partsupp_availqty_percentiles = partsupp_percentiles[PS_AVAILQTY]

    res = hash_numerical(val, partsupp_availqty_percentiles)[0]
    hashbits = ['0']*size
    
    for i in range(res+1):
        idx = i + PS_AVAILQTY * k_partsupp
        hashbits[idx] = '1'
    result = "".join(hashbits)
    result = '0'* size + result + '0' * size * 10
    assert len(result) == size * 12

    out_path_q9_partsupp = os.path.join(output_dir, "query_partsupp_hash_q9.bin")
    with open(out_path_q9_partsupp, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q9 (Partsupp) => {out_path_q9_partsupp}")

    #########################################
    # Q10 snippet (orders)
    #########################################
    q10_orders_opsMask = "010000000000"
    assert len(q10_orders_opsMask) == 12

    val_lower = datetime(1993, 10, 1).timestamp()
    val_higher = datetime(1994, 1, 1).timestamp()

    order_date_percentiles = orders_percentiles[O_ORDERDATE]
    res_lower = hash_numerical(val_lower, order_date_percentiles)[0]
    res_higher = hash_numerical(val_higher, order_date_percentiles)[0]
    hashbits = ['0']*size
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    for i in range(res_lower, res_higher+1):
        idx = i + O_ORDERDATE * k_order
        hashbits[idx] = '1'

    result_order = "".join(hashbits)

    hashbits2 = ['0']*size
    # res_list = hash_string("slyly", k_order)
    # for res in res_list:
    #     hashbits2[res + O_COMMENT * k_order] = '1'

    result_order_comment = "".join(hashbits2)

    result = result_order_comment + result_order + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q10_order = os.path.join(output_dir, "query_orders_hash_q10.bin")
    with open(out_path_q10_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q10 (Orders) => {out_path_q10_order}")

    #########################################
    # Q10 snippet (lineitem)
    #########################################
    q10_lineitem_opsMask = "100000000000"
    assert len(q10_lineitem_opsMask) == 12

    val = "R"
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    res = hash_string(val, k_lineitem)[0]
    hashbits = ['0']*size
    hashbits[res + L_RETURNFLAG * k_lineitem] = '1' # l_returnflag is the 9th column

    result_lineitem = "".join(hashbits)

    result = result_lineitem + '0' * size * 11
    assert len(result) == size * 12
    
    out_path_q10_lineitem = os.path.join(output_dir, "query_lineitem_hash_q10.bin")
    with open(out_path_q10_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q10 (Lineitem) => {out_path_q10_lineitem}")

    #########################################
    # Q11 snippet (partsupp)
    #########################################
    q11_partsupp_opsMask = "010000000000"
    assert len(q11_partsupp_opsMask) == 12
    val = 1800

    partsupp_availqty_percentiles = partsupp_percentiles[PS_AVAILQTY]

    res = hash_numerical(val, partsupp_availqty_percentiles)[0]
    hashbits = ['0']*size
    k_partsupp = size // PARTSUPP_TABLE_COLUMN_COUNT
    # all values smaller than the hash should be masked
    for i in range(res+1):
        idx = i + PS_AVAILQTY * k_partsupp
        hashbits[idx] = '1'
    result_ps_availqty = "".join(hashbits)

    comment = "bold"
    res = hash_string(comment, k_partsupp)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + PS_COMMENT * k_partsupp
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_ps_availqty + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q11_partsupp = os.path.join(output_dir, "query_partsupp_hash_q11.bin")
    with open(out_path_q11_partsupp, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q11 (Partsupp) => {out_path_q11_partsupp}")

    #########################################
    # Q12 snippet (orders)
    #########################################
    q12_orders_opsMask = "100000000000"
    assert len(q12_orders_opsMask) == 12

    k_order = size // ORDER_TABLE_COLUMN_COUNT

    val1 = '1-URGENT'
    res1 = hash_string(val1, k_order)[0]

    hashbits = ['0']*size
    hashbits[res1 + O_ORDERPRIORITY * k_order] = '1'
    result_order = "".join(hashbits) 
    result = result_order + '0' * size * 11
    assert len(result) == size * 12

    out_path_q12_order = os.path.join(output_dir, "query_orders_hash_q12.bin")
    with open(out_path_q12_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q12 (Orders) => {out_path_q12_order}")

    #########################################
    # Q12 snippet (lineitem)
    #########################################
    
    q12_lineitem_opsMask = "011000000000"
    assert len(q12_lineitem_opsMask) == 12
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT

    val1 = 'MAIL'
    val2 = 'SHIP'
    res1 = hash_string(val1, k_lineitem)[0]
    res2 = hash_string(val2, k_lineitem)[0]

    hashbits = ['0']*size
    hashbits[res1 + L_RECEIPTDATE * k_lineitem] = '1'
    hashbits[res2 + L_RECEIPTDATE * k_lineitem] = '1'
    result_shipmode = "".join(hashbits)

    val_lower = datetime(1994, 1, 1).timestamp()
    val_higher = datetime(1995, 1, 1).timestamp()

    l_receiptdate_percentiles = lineitem_percentiles[L_RECEIPTDATE]
    res_lower = hash_numerical(val_lower, l_receiptdate_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_receiptdate_percentiles)[0]
    hashbits = ['0']*size
    
    for i in range(res_lower, res_higher+1):
        idx = i + L_RECEIPTDATE * k_lineitem
        hashbits[idx] = '1'
    result_receiptdate = "".join(hashbits)

    val = "account"
    res = hash_string(val, k_lineitem)[0]
    hashbits = ['0']*size
    # hashbits[res + L_COMMENT * k_lineitem] = '1'
    # result_comment = "".join(hashbits)
    
    result = result_comment + result_receiptdate + result_shipmode + '0' * size * 9
    assert len(result) == size * 12

    out_path_q12_lineitem = os.path.join(output_dir, "query_lineitem_hash_q12.bin")
    with open(out_path_q12_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q12 (Lineitem) => {out_path_q12_lineitem}")

    #########################################
    # Q13 snippet (orders) No change in the query
    #########################################
    q13_orders_opsMask = "010000000000"
    assert len(q13_orders_opsMask) == 12

    val = datetime(1993, 4, 15).timestamp()
    orderdate_percentiles = orders_percentiles[O_ORDERDATE]

    res = hash_numerical(val, orderdate_percentiles)[0]
    hashbits = ['0']*size
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    for i in range(res+1):
        idx = i + O_ORDERDATE * k_order
        hashbits[idx] = '1'

    result_order = "".join(hashbits)
    result = '0'* size + result_order + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q13_order = os.path.join(output_dir, "query_orders_hash_q13.bin")
    with open(out_path_q13_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q13 (Orders) => {out_path_q13_order}")

    #########################################
    # Q14 snippet (lineitem)
    #########################################
    q14_opsMask = "010000000000"
    assert len(q14_opsMask) == 12

    val_lower = datetime(1995, 9, 1).timestamp()
    val_higher = datetime(1995, 10, 1).timestamp()
    
    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]

    res_lower = hash_numerical(val_lower, l_shipdate_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_shipdate_percentiles)[0]
    hashbits = ['0']*size
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    
    for i in range(res_lower, res_higher+1):
        idx = i + L_SHIPDATE * k_lineitem
        hashbits[idx] = '1'
    result_lineitem = "".join(hashbits)

    comment = 'account'
    res = hash_string(comment, k_lineitem)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + L_COMMENT * k_lineitem
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_lineitem + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q14_lineitem = os.path.join(output_dir, "query_lineitem_hash_q14.bin")
    with open(out_path_q14_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q14 (Lineitem) => {out_path_q14_lineitem}")

    #########################################
    # Q15 snippet (lineitem)
    #########################################
    q19_lineitem_opsMask = "010000000000"
    assert len(q19_lineitem_opsMask) == 12

    val_lower = datetime(1996, 1, 1).timestamp()
    val_higher = datetime(1996, 4, 1).timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]
    res_lower = hash_numerical(val_lower, l_shipdate_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_shipdate_percentiles)[0]
    hashbits = ['0']*size
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    
    for i in range(res_lower, res_higher+1):
        idx = i + L_SHIPDATE * k_lineitem
        hashbits[idx] = '1'

    result_lineitem = "".join(hashbits)

    comment = 'even'
    res = hash_string(comment, k_lineitem)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + L_COMMENT * k_lineitem
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_lineitem + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q15_lineitem = os.path.join(output_dir, "query_lineitem_hash_q15.bin")
    with open(out_path_q15_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q15 (Lineitem) => {out_path_q15_lineitem}")

    #########################################
    # Q16 snippet (partsupp)
    #########################################
    q16_partsupp_opsMask = "010000000000"
    assert len(q16_partsupp_opsMask) == 12
    val = 1800

    partsupp_availqty_percentiles = partsupp_percentiles[PS_AVAILQTY]
    res = hash_numerical(val, partsupp_availqty_percentiles)[0]
    hashbits = ['0']*size
    k_partsupp = size // PARTSUPP_TABLE_COLUMN_COUNT
    for i in range(res+1):
        idx = i + PS_AVAILQTY * k_partsupp
        hashbits[idx] = '1'
    result_availqty = "".join(hashbits)

    comment = 'even'
    res = hash_string(comment, k_partsupp)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + PS_COMMENT * k_partsupp
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_availqty + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q16_partsupp = os.path.join(output_dir, "query_partsupp_hash_q16.bin")
    with open(out_path_q16_partsupp, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q16 (Partsupp) => {out_path_q16_partsupp}")

    #########################################
    # Q17 snippet (lineitem)
    #########################################
    q17_lineitem_opsMask = "010000000000"
    assert len(q17_lineitem_opsMask) == 12

    given_date = datetime(1993, 9, 1)
    result_date = given_date - timedelta(days=90)
    val = result_date.timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]

    res = hash_numerical(val, l_shipdate_percentiles)[0]
    hashbits = ['0']*size
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    # all values smaller than the hash should be masked
    for i in range(res+1):
        idx = i + L_SHIPDATE * k_lineitem
        hashbits[idx] = '1'
    result_shipdate = "".join(hashbits)

    comment = 'account'
    res = hash_string(comment, k_lineitem)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + L_COMMENT * k_lineitem
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_shipdate + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q17_lineitem = os.path.join(output_dir, "query_lineitem_hash_q17.bin")
    with open(out_path_q17_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q17 (Lineitem) => {out_path_q17_lineitem}")

    #########################################
    # Q18 snippet (lineitem)
    #########################################
    q18_lineitem_opsMask = "010000000000"
    assert len(q18_lineitem_opsMask) == 12
    val = (datetime(1993, 9, 1) - timedelta(days=90)).timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]

    res = hash_numerical(val, l_shipdate_percentiles)[0]
    hashbits = ['0']*size
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    for i in range(res+1):
        idx = i + L_SHIPDATE * k_lineitem
        hashbits[idx] = '1'
    result_shipdate = "".join(hashbits)

    comment = 'slyly'
    res = hash_string(comment, k_lineitem)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + L_COMMENT * k_lineitem
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_shipdate + '0' * size * 10
    assert len(result) == size * 12

    out_path_q18_lineitem = os.path.join(output_dir, "query_lineitem_hash_q18.bin")
    with open(out_path_q18_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q18 (Lineitem) => {out_path_q18_lineitem}")
    
    #########################################
    # Q18 snippet (orders)
    #########################################
    q18_order_opsMask = "100000000000"
    assert len(q18_order_opsMask) == 12
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    val1 = '1-URGENT'
    res1 = hash_string(val1, k_order)

    hashbits = ['0']*size
    for r in res1:
        hashbits[r + O_ORDERPRIORITY*k_order] = '1'
    result_o_priority = "".join(hashbits)

    result = result_o_priority + '0' * size * 11
    assert len(result) == size * 12

    out_path_q18_order = os.path.join(output_dir, "query_orders_hash_q18.bin")
    with open(out_path_q18_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q18 (Orders) => {out_path_q18_order}")

    #########################################
    # Q19 snippet (lineitem) No change in the query
    #########################################
    q19_lineitem_opsMask = "111011101110"
    assert len(q19_lineitem_opsMask) == 12
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT

    shipmode_list = ['AIR', 'AIR REG']
    shipinstruct = 'DELIVER IN PERSON'

    hashbits = ['0']*size
    for shipmode in shipmode_list:
        res_list = hash_string(shipmode, k_lineitem)
        for res in res_list:
            hashbits[res + k_lineitem*L_SHIPMODE] = '1'
    result_shipmode = "".join(hashbits)

    hashbits = ['0']*size
    res_list = hash_string(shipinstruct, k_lineitem)
    for res in res_list:
        hashbits[res + k_lineitem*L_SHIPINSTRUCT] = '1'
    result_shipinstruct = "".join(hashbits)

    l_quantity_percentiles = lineitem_percentiles[L_QUANTITY]
    # set 1
    val_lower = 5
    val_higher = 11

    res_lower = hash_numerical(val_lower, l_quantity_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_quantity_percentiles)[0]
    hashbits = ['0']*size
    for i in range(res_lower, res_higher+1):
        idx = i + L_QUANTITY * k_lineitem
        hashbits[idx] = '1'
    result_quantity1 = "".join(hashbits)

    # set 2
    val_lower = 10
    val_higher = 15

    res_lower = hash_numerical(val_lower, l_quantity_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_quantity_percentiles)[0]
    hashbits = ['0']*size
    for i in range(res_lower, res_higher+1):
        idx = i + L_QUANTITY * k_lineitem
        hashbits[idx] = '1'
    result_quantity2 = "".join(hashbits)

    # set 3
    val_lower = 20
    val_higher = 30

    res_lower = hash_numerical(val_lower, l_quantity_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_quantity_percentiles)[0]
    hashbits = ['0']*size
    # all values smaller than the hash should be masked
    for i in range(res_lower, res_higher+1):
        idx = i + L_QUANTITY * k_lineitem
        hashbits[idx] = '1'
    result_quantity3 = "".join(hashbits)

    result = result_shipinstruct + result_shipmode + result_quantity1 + '0' * size
    result += (result_shipinstruct + result_shipmode + result_quantity2 + '0' * size)
    result += (result_shipinstruct + result_shipmode + result_quantity3 + '0' * size)
    assert len(result) == size * 12

    out_path_q19_lineitem = os.path.join(output_dir, "query_lineitem_hash_q19.bin")
    with open(out_path_q19_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q19 (Lineitem) => {out_path_q19_lineitem}")

    #########################################
    # Q20 snippet (lineitem)
    #########################################
    q19_lineitem_opsMask = "010000000000"
    assert len(q19_lineitem_opsMask) == 12
    val_lower = datetime(1994, 1, 1).timestamp()
    val_higher = datetime(1995, 1, 1).timestamp()

    l_shipdate_percentiles = lineitem_percentiles[L_SHIPDATE]
    res_lower = hash_numerical(val_lower, l_shipdate_percentiles)[0]
    res_higher = hash_numerical(val_higher, l_shipdate_percentiles)[0]
    hashbits = ['0']*size
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    for i in range(res_lower, res_higher+1):
        idx = i + L_SHIPDATE * k_lineitem
        hashbits[idx] = '1'
    result_lineitem = "".join(hashbits)

    comment = 'regular'
    res = hash_string(comment, k_lineitem)
    hashbits = ['0']*size
    # for r in res:
    #     idx = r + L_COMMENT * k_lineitem
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_lineitem + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q20_lineitem = os.path.join(output_dir, "query_lineitem_hash_q20.bin")
    with open(out_path_q20_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q20 (Lineitem) => {out_path_q20_lineitem}")

    #########################################
    # Q20 snippet (partsupp)
    #########################################
    val = 1800

    partsupp_availqty_percentiles = partsupp_percentiles[PS_AVAILQTY]
    res = hash_numerical(val, partsupp_availqty_percentiles)[0]
    hashbits = ['0']*size
    k_partsupp = size // PARTSUPP_TABLE_COLUMN_COUNT
    for i in range(res+1):
        idx = i + PS_AVAILQTY * k_partsupp
        hashbits[idx] = '1'
    result = "".join(hashbits)
    result = '0'* size + result + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q20_partsupp = os.path.join(output_dir, "query_partsupp_hash_q20.bin")
    with open(out_path_q20_partsupp, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q20 (Partsupp) => {out_path_q20_partsupp}")
    
    #########################################
    # Q21 snippet (lineitem) No change in the query
    #########################################
    q21_lineitem_opsMask = "100000000000"
    assert len(q21_lineitem_opsMask) == 12

    val = "R"
    k_lineitem = size // LINEITEM_TABLE_COLUMN_COUNT
    res = hash_string(val, k_lineitem)[0]
    hashbits = ['0']*size
    hashbits[res + k_lineitem * L_RETURNFLAG] = '1'
    result_lineitem = "".join(hashbits)

    result = result_lineitem + '0' * size * 11
    assert len(result) == size * 12
    
    out_path_q21_lineitem = os.path.join(output_dir, "query_lineitem_hash_q21.bin")
    with open(out_path_q21_lineitem, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q21 (Lineitem) => {out_path_q21_lineitem}")

    #########################################
    # Q21 snippet (orders) No change in the query
    #########################################
    q21_order_opsMask = "100000000000"
    assert len(q21_order_opsMask) == 12
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    val1 = '1-URGENT'
    res1 = hash_string(val1, k_order)[0]

    hashbits = ['0']*size
    hashbits[res1 + O_ORDERPRIORITY * k_order] = '1'
    result_o_priority = "".join(hashbits) 
    result = result_o_priority + '0' * size * 11
    assert len(result) == size * 12

    out_path_q21_order = os.path.join(output_dir, "query_orders_hash_q21.bin")
    with open(out_path_q21_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q21 (Orders) => {out_path_q21_order}")

    #########################################
    # Q22 snippet (order)
    #########################################
    q22_order_opsMask = "010000000000"
    assert len(q22_order_opsMask) == 12

    order_date = datetime(1993, 4, 15)
    val = order_date.timestamp()

    order_date_percentiles = orders_percentiles[O_ORDERDATE]
    res = hash_numerical(val, order_date_percentiles)[0]
    
    hashbits = ['0']*size
    k_order = size // ORDER_TABLE_COLUMN_COUNT

    for i in range(res+1):
        idx = i + O_ORDERDATE * k_order
        hashbits[idx] = '1'

    result_order = "".join(hashbits)

    comment = 'special'
    res = hash_string(comment, k_order)
    # hashbits = ['0']*size
    # for r in res:
    #     idx = r + O_COMMENT * k_order
    #     hashbits[idx] = '1'
    result_comment = "".join(hashbits)

    result = result_comment + result_order + '0' * size * 10
    assert len(result) == size * 12
    
    out_path_q22_order = os.path.join(output_dir, "query_orders_hash_q22.bin")
    with open(out_path_q22_order, 'wb') as f_out:
        f_out.write(bits_to_bytes(result))
    print(f"[INFO] Wrote Q22 (order) => {out_path_q22_order}")


def create_query_hash_q14(output_dir, size=256):
    size_per_col = size // 16
    bitarray = ['0'] * size
    start = L_SHIPDATE * size_per_col
    end = start + size_per_col
    for i in range(start, end):
        bitarray[i] = '1'
    
    query_bits = "".join(bitarray) + '0' * (size * 11)
    
    out_path = os.path.join(output_dir, "query_lineitem_hash_q14.bin")
    with open(out_path, 'wb') as f_out:
        f_out.write(bits_to_bytes(query_bits))
    print(f"[INFO] Wrote Q14 (Lineitem) query hash => {out_path}")

###############################################################################
# 4. GENERATE PERCENTILES
###############################################################################
def count_rows(input_csv, delimiter='|', chunksize=10**6):
    """Count total rows in the CSV."""
    count = 0
    for chunk in pd.read_csv(input_csv, delimiter=delimiter, header=None, chunksize=chunksize):
        count += len(chunk)
    return count

def generate_percentiles(input_csv, output_npy, delimiter='|', size=256, chunksize=10**6):
    nrows = count_rows(input_csv, delimiter, chunksize)
    print(f"Total rows: {nrows}")
    
    sample = pd.read_csv(input_csv, delimiter=delimiter, header=None, nrows=100)
    sample = sample[sample.columns[:-1]]
    ncols = len(sample.columns)
    size_per_col = size // ncols
    print(f"Columns: {ncols}, Size per column: {size_per_col}")
    
    col_types = {}
    for col in sample.columns:
        check = sample[col].iloc[0]
        if is_float(check) is None and is_date(check) is None:
            col_types[col] = 'string'
        elif is_float(check) is not None:
            col_types[col] = 'num'
        else:
            col_types[col] = 'date'
    
    # --- Prepare temporary storage for numeric/date columns ---
    temp_files = {}  # Will map column index -> temporary file name
    arrays = {}      # Will map column index -> memmap array
    for col in range(ncols):
        if col_types[col] in ['num', 'date']:
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            temp_files[col] = temp_file.name
            arrays[col] = np.memmap(temp_file.name, dtype='float64', mode='w+', shape=(nrows,))
        else:
            arrays[col] = None

    row_idx = 0
    for chunk in tqdm(pd.read_csv(input_csv, delimiter=delimiter, header=None, chunksize=chunksize), 
                      desc="Storing data"):
        chunk = chunk[chunk.columns[:-1]]
        chunk_len = len(chunk)
        for col in chunk.columns:
            if col_types[col] == 'num':
                values = chunk[col].apply(is_float).values.astype('float64')
                arrays[col][row_idx:row_idx+chunk_len] = values
            elif col_types[col] == 'date':
                values = chunk[col].apply(is_date).values.astype('float64')
                arrays[col][row_idx:row_idx+chunk_len] = values
        row_idx += chunk_len
    
    percentiles_list = []
    for col in range(ncols):
        if col_types[col] in ['num', 'date']:
            arrays[col].flush()
            data = np.array(arrays[col])
            sorted_data = np.sort(data)
            
            col_percentiles = []
            for p in np.linspace(0, 100, num=size_per_col+2)[1:-1]:
                rank = int(np.ceil((p / 100) * nrows)) - 1
                rank = max(0, min(nrows - 1, rank))
                col_percentiles.append(sorted_data[rank])
            percentiles_list.append(col_percentiles)
            
            os.remove(temp_files[col])
        else:
            # For string columns, just store zeros
            percentiles_list.append([0] * size_per_col)
    
    np.save(output_npy, np.array(percentiles_list))

###############################################################################
# 5. MAIN CLI
###############################################################################

def main():
    parser = argparse.ArgumentParser(description="Data & Query Hash Tool")

    subparsers = parser.add_subparsers(dest="subcmd", required=True)

    # DATA HASH
    parser_data = subparsers.add_parser("data", help="Generate data-hash (and optional length file) from a .tbl file.")
    parser_data.add_argument("--input", required=True, help="Path to input .tbl (CSV) file.")
    parser_data.add_argument("--output-hash", required=True, help="Path to output .bin file for the row-hashes.")
    parser_data.add_argument("--percentile", default="/mnt/ssd/chuxuan/rebuttal/percentiles/lineitem_percentiles.npy", help="Percentile file for numerical columns.")
    parser_data.add_argument("--output-len", default=None, help="Optional .bin file for row length, one byte per row.")
    parser_data.add_argument("--delimiter", default="|", help="Field delimiter in the input table (default='|').")
    parser_data.add_argument("--size", type=int, default=256, help="Number of bits for the row-hash (default=256).")

    # QUERY HASH
    parser_query = subparsers.add_parser("query", help="Generate query-hash files")
    parser_query.add_argument("--output-dir", required=True, help="Directory to store query-hash .bin files (Q1..Q22, etc.).")
    parser_query.add_argument("--base-dir", default="/mnt/ssd/chuxuan/rebuttal/percentiles", help="Base directory for percentile files (default=/mnt/ssd/chuxuan/rebuttal/percentiles).")
    parser_query.add_argument("--size", type=int, default=256, help="Number of bits per chunk (default=256).")

    parser_percentiles = subparsers.add_parser("percentiles", help="Generate percentiles for numerical columns.")
    parser_percentiles.add_argument("--input", required=True, help="Path to input .tbl (CSV) file.")
    parser_percentiles.add_argument("--output", required=True, help="Path to output .npy file for the percentiles.")
    parser_percentiles.add_argument("--delimiter", default="|", help="Field delimiter in the input table (default='|').")
    parser_percentiles.add_argument("--size", type=int, default=256, help="Number of bits for the row-hash (default=256).")
    parser_percentiles.add_argument("--chunksize", type=int, default=10**6, help="Chunk size for reading the input CSV (default=10^6).")

    args = parser.parse_args()

    if args.subcmd == "data":
        create_data_hash(
            input_csv=args.input,
            output_hash_bin=args.output_hash,
            output_len_bin=args.output_len,
            percentiles_path=args.percentile,
            delimiter=args.delimiter,
            size=args.size
        )
        print(f"Data-hash created at {args.output_hash}")
        if args.output_len:
            print(f"Row-lengths stored in {args.output_len}")

    elif args.subcmd == "query":
        create_query_hash_q14(
            output_dir=args.output_dir,
            size=args.size
        )
        print(f"Query-hash created at {args.output_dir}")
    
    elif args.subcmd == "percentiles":
        generate_percentiles(
            input_csv=args.input,
            output_npy=args.output,
            delimiter=args.delimiter,
            size=args.size,
            chunksize=args.chunksize
        )
        print(f"Percentiles created at {args.output}")

if __name__ == "__main__":
    main()
