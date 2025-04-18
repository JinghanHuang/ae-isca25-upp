#!/usr/bin/env bash

###############################################################################
#             CONFIGURABLE PARAMETERS (EDIT THESE AS NEEDED)
###############################################################################
NUM_QUERIES=22              # queries from 1..22
SPARK_CORES_CPU_FILTER=4    # cores for CPU-filter experiment
SPARK_CORES_BASELINE=4      # cores for baseline
SPARK_CORES_USPS=4          # cores for USPS
SPARK_DRIVER_MEM="200g"     # memory for Spark driver, e.g. 200g

# Python scripts (point these to your updated code)
SCRIPT_BASELINE="./single_run_sql.py"
SCRIPT_USPS="./single_run_usps.py"

# Directory arguments for the Python scripts:
TMPFS_DIR_BASELINE="/tmpfs_test/100gb/baseline"
TMPFS_DIR_USPS="/tmpfs_test/100gb/usps"

CHUNK_DIR="/mnt/ssd/chuxuan/100gb-dp/chunk"
JSON_DIR="/mnt/ssd/chuxuan/rebuttal/jsons"
QUERIES_DIR="/mnt/ssd/chuxuan/rebuttal/queries"
RESULTS_DIR="./results"

# Output path for logs (CPU, IO, power, pcie):
OUTPUT_PATH="./output_results/"
mkdir -p "${OUTPUT_PATH}"

# measure_power script
POWER_SCRIPT="/home/usps/smartssd/USPS-SmartSSD-20231117/isp_kernel/tool/measure_power.sh"
# PCM-PCIe binary
PCM_PCIE="/home/usps/pcm/build/bin/pcm-pcie"

###############################################################################
#             XILINX / VITIS ENVIRONMENT SETUP
###############################################################################
source /tools/Xilinx/Vivado/2021.2/settings64.sh
source /opt/xilinx/xrt/setup.sh
source /tools/Xilinx/Vitis/2021.2/settings64.sh
source /tools/Xilinx/Vitis_HLS/2021.2/settings64.sh
export PLATFORM_REPO_PATHS=/opt/xilinx/platforms

###############################################################################
#             FUNCTION TO RUN A SINGLE QUERY IN ONE MODE
###############################################################################
run_experiment() {
  local query_id="$1"        # e.g. 1..22
  local mode="$2"            # e.g. "cpu-filter", "baseline", "usps"
  local py_script="$3"       # path to single_run_sql_xxx.py
  local spark_cores="$4"     # e.g. 4, 1, ...
  local spark_mem="$5"       # e.g. "200g"

  local tmpfs_dir_arg=""     # We'll set this differently for baseline vs. usps
  if [ "$mode" = "baseline" ]; then
    tmpfs_dir_arg="$TMPFS_DIR_BASELINE"
  else
    # usps
    tmpfs_dir_arg="$TMPFS_DIR_USPS"
  fi

  # Output name => Q<query_id>-<mode>
  local output_name="Q${query_id}-${mode}"

  echo "====================================================================="
  echo "[INFO] Starting query Q${query_id} in mode '${mode}' with ${spark_cores} core(s)."
  echo "       Output => ${OUTPUT_PATH}${output_name}_*"
  echo "====================================================================="

  # 1) Start the Spark job in the background
  spark-submit \
    --name "q${query_id} ${mode}" \
    --master local["${spark_cores}"] \
    --conf "spark.driver.memory=${spark_mem}" \
    "${py_script}" \
      -i "${query_id}" \
      -c "${spark_cores}" \
      --tmpfs-dir "${tmpfs_dir_arg}" \
      --chunk-dir "${CHUNK_DIR}" \
      --json-dir "${JSON_DIR}" \
      --queries-dir "${QUERIES_DIR}" \
      --results-dir "${RESULTS_DIR}" &

  BENCHMARK_PID=$!

  # 2) Start stats collection in the background
  sar -u 1 > "${OUTPUT_PATH}${output_name}_cpu_utilization_stat.txt" &
  SAR_PID=$!

  iostat -x 1 > "${OUTPUT_PATH}${output_name}_io_stat.txt" &
  IOSTAT_PID=$!

  bash "${POWER_SCRIPT}" 6000 > "${OUTPUT_PATH}${output_name}_power_stat.txt" &
  POWER_PID=$!

  sudo "${PCM_PCIE}" 1 -silent -e -B -csv="${OUTPUT_PATH}${output_name}_pcie_stat.csv" &
  PCM_PCIE_PID=$!

  # 3) Wait until Spark job finishes
  while kill -0 $BENCHMARK_PID 2> /dev/null; do
    sudo cat /sys/class/powercap/intel-rapl:0/energy_uj >> "${OUTPUT_PATH}${output_name}_cpu_0_power.txt"
    sudo cat /sys/class/powercap/intel-rapl:1/energy_uj >> "${OUTPUT_PATH}${output_name}_cpu_1_power.txt"
    sleep 1
  done

  # 4) Stop all background stats
  echo "[INFO] Spark job done for Q${query_id} (${mode}). Stopping stats..."
  sudo kill $SAR_PID
  sudo kill $IOSTAT_PID
  sudo kill $POWER_PID
  sudo killall pcm-pcie

  echo "[INFO] Finished query Q${query_id} in mode '${mode}'."
}

###############################################################################
#             CTRL-C HANDLER
###############################################################################
cleanup_on_interrupt() {
  echo ""
  echo "[WARN] Caught Ctrl-C (SIGINT). Stopping all background processes..."
  sudo killall sar iostat
  sudo killall pcm-pcie
  sudo killall measure_power.sh
  # Optionally kill spark jobs if needed
  # pkill -f spark-submit
  echo "[WARN] Cleanup complete. Exiting."
  exit 1
}

trap cleanup_on_interrupt INT

###############################################################################
#             MAIN: RUN QUERIES 1..NUM_QUERIES FOR 3 MODES
###############################################################################
for (( i=1; i<=NUM_QUERIES; i++ )); do

  # 1) baseline
  run_experiment "${i}" "baseline"   "${SCRIPT_BASELINE}"   "${SPARK_CORES_BASELINE}"   "${SPARK_DRIVER_MEM}"

  # 2) usps
  run_experiment "${i}" "usps"       "${SCRIPT_USPS}"       "${SPARK_CORES_USPS}"       "${SPARK_DRIVER_MEM}"

done

spark-submit --name "ideal_filter"  --master local[4] --conf "spark.driver.memory=200g" "calc_ideal_filter_ratio.py"

echo "[INFO] All experiments completed."
