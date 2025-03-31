import os
import re
import csv
import pandas as pd

TABLE_SIZES = {
    'lineitem': 79579694556,
    'customer': 2463490271,
    'part': 2453234158,
    'partsupp': 11870888279,
    'orders': 17793116301,
    'supplier': 142869803,
    'nation': 2224,
    'region': 389
}

def parse_latencies(filepath):
    total_latency = 0
    with open(filepath, 'r') as f:
        lines = f.readlines()

    for line in lines:
        copy_match = re.search(r'Total time for SQL \d+ takes ([\d.]+) seconds', line)
        if copy_match:
            total_latency = float(copy_match.group(1))
            break
    return total_latency

def parse_upp_filtered_ratios(filepath):
    filtered_ratios = []
    current_query = {}

    with open(filepath, 'r') as f:
        for line in f:
            start_match = re.match(r'Start Running Query (\d+)', line)
            if start_match:
                if current_query:
                    filtered_ratios.append(current_query)
                    current_query = {}
                continue

            ratio_match = re.search(r'Table (\w+) filtered data size ratio: ([\d.]+)%', line)
            if ratio_match:
                table = ratio_match.group(1)
                ratio = float(ratio_match.group(2)) / 100
                current_query[table] = ratio

        if current_query:
            filtered_ratios.append(current_query)

    return filtered_ratios

def calculate_filtered_ratio(filtered_info):
    ratios = []
    for query_ratio in filtered_info:
        total_filtered = sum(TABLE_SIZES[table] * ratio for table, ratio in query_ratio.items())
        total_original = sum(TABLE_SIZES[table] for table in query_ratio.keys())
        avg_ratio = total_filtered / total_original if total_original > 0 else 0.0
        ratios.append(avg_ratio)
    return ratios

def parse_baseline_filtered_ratios(filepath, all_upp_tables):
    filtered_ratios = []
    with open(filepath, 'r') as f:
        content = f.read()

    for i in range(14, 15):
        pattern = re.compile(rf'q{i}_(\w+) on (\w+): filtered size = (\d+) bytes, original size = (\d+) bytes, ratio = ([\d.]+)%')
        matches = pattern.findall(content)
        query_tables = {table: float(ratio) / 100 for _, table, _, _, ratio in matches}
        for table in all_upp_tables[0].keys():
            if table not in query_tables:
                query_tables[table] = 1.0

        filtered_ratios.append(query_tables)
    return calculate_filtered_ratio(filtered_ratios)

def save_combined_latency_table(filename, base_t, upp_t, baseline_ratios, upp_ratios):
    with open(filename, 'w') as f:
        f.write('| Hash Length | Total latency (CPU) | Total latency (UPP) | Filtered Ratio (CPU)     | Filtered Ratio (UPP) |\n')
        f.write('|-------------|---------------------|---------------------|--------------------------|----------------------|\n')
        hash_lengths = [64, 128, 256, "ideal"]
        for i, hash_length in enumerate(hash_lengths):
            f.write(f'| {str(hash_length):<11} | {base_t[i]:<19.6f} | {upp_t[i]:<19.6f} | {baseline_ratios[i]:<24.4f} | {upp_ratios[i]:<20.4f} |\n')

def parse_generate_fig8a():
    core_counts = [8, 4, 2, 1]
    output_lines = []

    for core in core_counts:
        base_filename = f"fig8/runningtime_baseline_{core}cores_256.txt"
        upp_filename = f"fig8/runningtime_usps_{core}cores_256.txt"

        base_time = None
        upp_time = None

        if os.path.exists(base_filename):
            with open(base_filename, 'r') as f:
                for line in f:
                    match = re.search(r'Total time for SQL \d+ takes ([\d.]+) seconds', line)
                    if match:
                        base_time = float(match.group(1))
                        break

        if os.path.exists(upp_filename):
            with open(upp_filename, 'r') as f:
                for line in f:
                    match = re.search(r'Total time for SQL \d+ takes ([\d.]+) seconds', line)
                    if match:
                        upp_time = float(match.group(1))
                        break

        label = f"{core}C:1S"
        cpu_str = f"{base_time:<19.6f}" if base_time is not None else "N/A"
        upp_str = f"{upp_time:<19.6f}" if upp_time is not None else "N/A"

        line = f"| {label:<15}  | {cpu_str:<19} | {upp_str:<19} |"
        output_lines.append(line)

    # Save to file
    with open("figure8a_data.txt", "w") as f:
        f.write('| #Cores:#SmartSSD | Total latency (CPU) | Total latency (UPP) |\n')
        f.write('|------------------|---------------------|---------------------|\n')
        for line in output_lines:
            f.write(line + "\n")

def parse_generate_fig8b():
    filter_log_path = "filtered_ratio_log.txt"
    hash_lengths = [64, 128, 256, "ideal"]
    base_total_latency_list = []
    upp_total_latency_list = []
    baseline_ratios_list =[]
    upp_ratios_list = []
    for hash_length in hash_lengths:
        base_path = f"fig8/runningtime_baseline_4cores_{hash_length}.txt"
        upp_path = f"fig8/runningtime_usps_4cores_{hash_length}.txt"

        # parse CPU latency
        base_total_latency = parse_latencies(base_path)
        base_total_latency_list.append(base_total_latency)

        # parse UPP latency
        upp_total_latency = parse_latencies(upp_path)
        upp_total_latency_list.append(upp_total_latency)

        # calculate UPP filter ratio
        upp_table_ratios = parse_upp_filtered_ratios(upp_path)
        upp_ratios = calculate_filtered_ratio(upp_table_ratios)
        upp_ratios_list.append(upp_ratios[0])

        # calculate CPU filter ratio
        baseline_ratios = parse_baseline_filtered_ratios(filter_log_path, upp_table_ratios)
        baseline_ratios_list.append(baseline_ratios[0])

    save_combined_latency_table('figure8b_data.txt', base_total_latency_list, upp_total_latency_list, baseline_ratios_list, upp_ratios_list)

def main():
    parse_generate_fig8a()
    parse_generate_fig8b()

if __name__ == '__main__':
    main()
