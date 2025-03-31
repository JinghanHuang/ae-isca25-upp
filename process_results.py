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

def process_file_with_error_handling(file_path):
    try:
        data = pd.read_csv(file_path, delim_whitespace=True, skiprows=2)
        avg_idle = data['%idle'].mean()
        return 100 - avg_idle
    except KeyError:
        return None

def extract_io_stat_data_average(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
        nvme0c0n1_values = []
        for line in lines:
            if 'nvme0c0n1' in line:
                parts = line.split()
                if len(parts) > 2:
                    nvme0c0n1_values.append(float(parts[2]))
        avg_value = sum(nvme0c0n1_values) / len(nvme0c0n1_values) if nvme0c0n1_values else None
        return avg_value / 1024
    except (ValueError, IndexError):
        return None

def extract_pcie_stat_data_average(file_path):
    try:
        data = pd.read_csv(file_path)
        filtered_data = data[(data['Skt'] == '0') & (data['PCIe Wr (B)'].str.endswith('(Total)'))]
        pcie_wr_values = filtered_data['PCIe Wr (B)'].str.extract(r'(\d+)').astype(float)
        avg_value = pcie_wr_values.mean()[0] if not pcie_wr_values.empty else None
        return avg_value / 1024 / 1024
    except (KeyError, IndexError, ValueError, pd.errors.EmptyDataError):
        return None

def extract_power_stat_data_average(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
        system_powers = []
        smartssd_powers = []
        for line in lines:
            if 'System Power:' in line:
                system_power = float(re.search(r'System Power: (\d+\.?\d*)', line).group(1))
                system_powers.append(system_power)
            elif 'SmartSSD Power:' in line:
                smartssd_power = float(re.search(r'SmartSSD Power: (\d+\.?\d*)', line).group(1))
                smartssd_powers.append(smartssd_power)
        avg_system_power = sum(system_powers) / len(system_powers) if system_powers else None
        avg_smartssd_power = sum(smartssd_powers) / len(smartssd_powers) if smartssd_powers else None
        return avg_system_power, avg_smartssd_power
    except (AttributeError, IndexError, ValueError):
        return None, None

def extract_cpu_power_difference(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
        if len(lines) < 2:
            return None
        total_diff = 0.0
        for i in range(1, len(lines)):
            current_val = float(lines[i].strip())
            previous_val = float(lines[i-1].strip())
            diff = current_val - previous_val
            if diff > 0:
                total_diff += diff
        return total_diff / 1_000_000
    except (ValueError, IndexError):
        return None

def count_lines_in_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return sum(1 for _ in file)
    except:
        return None

def process_all_files(directory_path):
    file_paths = sorted([
        os.path.join(directory_path, file)
        for file in os.listdir(directory_path)
        if file.endswith((
            '_cpu_utilization_stat.txt', 
            '_io_stat.txt', 
            '_pcie_stat.csv', 
            '_power_stat.txt',
            '_cpu_0_power.txt',
            '_cpu_1_power.txt'
        ))
    ])

    results = {}
    for file_path in file_paths:
        file_name = os.path.splitext(os.path.basename(file_path))[0].split('_')[0]

        # CPU utilization
        if '_cpu_utilization_stat.txt' in file_path:
            avg_idle = process_file_with_error_handling(file_path)
            if file_name not in results:
                results[file_name] = {'Average CPU utilization (%)': avg_idle}
            else:
                results[file_name]['Average CPU utilization (%)'] = avg_idle

        # IO stats
        elif '_io_stat.txt' in file_path:
            io_stat_avg = extract_io_stat_data_average(file_path)
            if file_name not in results:
                results[file_name] = {'IO Stat Avg (MB/s)': io_stat_avg}
            else:
                results[file_name]['IO Stat Avg (MB/s)'] = io_stat_avg

        # PCIe stats
        elif '_pcie_stat.csv' in file_path:
            pcie_stat_avg = extract_pcie_stat_data_average(file_path)
            if file_name not in results:
                results[file_name] = {'PCIe Stat Avg (MB/s)': pcie_stat_avg}
            else:
                results[file_name]['PCIe Stat Avg (MB/s)'] = pcie_stat_avg

        # Power stats
        elif '_power_stat.txt' in file_path and not ('_cpu_0_power.txt' in file_path or '_cpu_1_power.txt' in file_path):
            system_power, smartssd_power = extract_power_stat_data_average(file_path)
            if file_name not in results:
                results[file_name] = {
                    'System Power Avg (W)': system_power, 
                    'SmartSSD Power Avg (W)': smartssd_power
                }
            else:
                results[file_name].update({
                    'System Power Avg (W)': system_power, 
                    'SmartSSD Power Avg (W)': smartssd_power
                })

        # CPU 0 power difference
        elif '_cpu_0_power.txt' in file_path:
            cpu0_power_diff = extract_cpu_power_difference(file_path)
            cpu0_power_lines = count_lines_in_file(file_path)
            if file_name not in results:
                results[file_name] = {
                    'CPU 0 Power Diff (J)': cpu0_power_diff,
                    'CPU 0 Power Line Count': cpu0_power_lines
                }
            else:
                results[file_name].update({
                    'CPU 0 Power Diff (J)': cpu0_power_diff,
                    'CPU 0 Power Line Count': cpu0_power_lines
                })

        # CPU 1 power difference
        elif '_cpu_1_power.txt' in file_path:
            cpu1_power_diff = extract_cpu_power_difference(file_path)
            if file_name not in results:
                results[file_name] = {'CPU 1 Power Diff (J)': cpu1_power_diff}
            else:
                results[file_name]['CPU 1 Power Diff (J)'] = cpu1_power_diff

    return results

def parse_latencies(filepath):
    storage_latencies = []
    compute_latencies = []

    with open(filepath, 'r') as f:
        lines = f.readlines()

    for line in lines:
        copy_match = re.search(r'Total Copying Table takes ([\d.]+) seconds', line)
        compute_match = re.search(r'SQL (\d+) finishes running in ([\d.]+) seconds', line)

        if copy_match:
            storage_latencies.append(float(copy_match.group(1)))
        if compute_match:
            compute_latencies.append(float(compute_match.group(2)))

    return storage_latencies, compute_latencies

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

    for i in range(1, 23):
        pattern = re.compile(rf'q{i}_(\w+) on (\w+): filtered size = (\d+) bytes, original size = (\d+) bytes, ratio = ([\d.]+)%')
        matches = pattern.findall(content)
        query_tables = {table: float(ratio) / 100 for _, table, _, _, ratio in matches}

        if i - 1 < len(all_upp_tables):
            for table in all_upp_tables[i - 1].keys():
                if table not in query_tables:
                    query_tables[table] = 1.0

        filtered_ratios.append(query_tables)
    return calculate_filtered_ratio(filtered_ratios)

def read_compiled_stats(filepath):
    stats = {}
    with open(filepath, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            file_key = row['File'].strip()
            stats[file_key] = {
                'cpu_power_diff': float(row['CPU 0 Power Diff (J)']),
                'cpu_power_lines': float(row['CPU 0 Power Line Count']),
                'smartssd_power': float(row['SmartSSD Power Avg (W)']),
                'system_power': float(row['System Power Avg (W)']),
                'cpu_util': float(row['Average CPU utilization (%)'])
            }
    return stats

def save_combined_latency_table(filename, base_s, base_c, upp_s, upp_c, baseline_ratios, upp_ratios):
    with open(filename, 'w') as f:
        f.write('| Query  | Storage-access latency (CPU) | Storage-access latency (UPP)  | Compute-only latency (CPU)  | Compute-only latency (UPP)  | Total latency (CPU) | Total latency (UPP) | Filtered Ratio (CPU)     | Filtered Ratio (UPP) |\n')
        f.write('|--------|------------------------------|-------------------------------|-----------------------------|-----------------------------|---------------------|---------------------|--------------------------|----------------------|\n')
        for i in range(len(base_s)):
            total_base = base_s[i] + base_c[i]
            total_upp = upp_s[i] + upp_c[i]
            f.write(f'| {i+1:<6} | {base_s[i]:<28.6f} | {upp_s[i]:<29.6f} | {base_c[i]:<27.6f} | {upp_c[i]:<27.6f} | {total_base:<19.6f} | {total_upp:<19.6f} | {baseline_ratios[i]:<24.4f} | {upp_ratios[i]:<20.4f} |\n')

def save_energy_stats(filename, base_s, base_c, upp_s, upp_c, stats):
    with open(filename, 'w') as f:
        f.write('| Query  | CPU energy (CPU)  | CPU energy (UPP)  | SmartSSD energy (UPP)   | Other system energy (CPU)   | Other system energy (UPP)   | Total energy (CPU)          | Total energy (UPP)          | CPU util ratio (UPP/CPU) |\n')
        f.write('|--------|-------------------|-------------------|-------------------------|-----------------------------|-----------------------------|-----------------------------|-----------------------------|---------------------------|\n')
        for i in range(len(base_s)):
            query = f"Q{i+1}"
            base_key = f"{query}-baseline"
            upp_key = f"{query}-usps"
            base_latency = base_s[i] + base_c[i]
            upp_latency = upp_s[i] + upp_c[i]

            b = stats.get(base_key)
            u = stats.get(upp_key)
            if not b or not u:
                continue

            cpu_energy_b = (b['cpu_power_diff'] / b['cpu_power_lines']) * base_latency / 1000 if b['cpu_power_lines'] else 0.0
            cpu_energy_u = (u['cpu_power_diff'] / u['cpu_power_lines']) * upp_latency / 1000 if u['cpu_power_lines'] else 0.0

            smart_energy_b = 0
            smart_energy_u = u['smartssd_power'] * upp_latency / 1000

            other_energy_b = ((b['system_power'] - 272) * base_latency - cpu_energy_b - smart_energy_b) / 1000
            other_energy_u = ((u['system_power'] - 272) * upp_latency - cpu_energy_u - smart_energy_u) / 1000
            
            total_energy_b = cpu_energy_b + smart_energy_b + other_energy_b
            total_energy_u = cpu_energy_u + smart_energy_u + other_energy_u

            cpu_util_b = b['cpu_util'] * base_latency
            cpu_util_u = u['cpu_util'] * upp_latency
            cpu_util_ratio = cpu_util_u / cpu_util_b if cpu_util_b else 0.0

            f.write(f"| {i+1:<6} | {cpu_energy_b:<17.4f} | {cpu_energy_u:<17.4f} | {smart_energy_u:<23.4f} | {other_energy_b:<27.4f} | {other_energy_u:<27.4f} | {total_energy_b:<27.4f} | {total_energy_u:<27.4f} | {cpu_util_ratio:<25.4f} | \n")

def main():
    results = process_all_files('./output_results')
    df = pd.DataFrame.from_dict(results, orient='index')
    df = df.reset_index().rename(columns={'index': 'File'})
    df['SortKey'] = df['File'].apply(lambda x: int(re.search(r'\d+', x).group()))
    df_sorted = df.sort_values(by=['SortKey', 'File']).reset_index(drop=True)

    csv_output_path = 'compiled_stats.csv'
    df_sorted.to_csv(csv_output_path, index=False)

    # Files from the current path
    base_path = 'runningtime_baseline.txt'
    upp_path = 'runningtime_usps.txt'
    filter_log_path = 'filtered_ratio_log.txt'
    stats_path = 'compiled_stats.csv'

    # Parse latency results about figure 6
    base_storage, base_compute = parse_latencies(base_path)
    upp_storage, upp_compute = parse_latencies(upp_path)

    # Parse filtered ratio results about figure 6
    upp_table_ratios = parse_upp_filtered_ratios(upp_path)
    upp_ratios = calculate_filtered_ratio(upp_table_ratios)
    baseline_ratios = parse_baseline_filtered_ratios(filter_log_path, upp_table_ratios)

    # Parse results about figure 7
    stats = read_compiled_stats(stats_path)

    save_combined_latency_table('figure6_data.txt', base_storage, base_compute, upp_storage, upp_compute, baseline_ratios, upp_ratios)
    save_energy_stats('figure7_data.txt', base_storage, base_compute, upp_storage, upp_compute, stats)

if __name__ == '__main__':
    main()
