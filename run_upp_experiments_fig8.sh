#!/usr/bin/env bash

QUERY_ID=14

# List of hash configurations to test (64, 128, 256, and ideal for an infinite hash)
HASH_CONFIG_LIST=(64 128 256 ideal)

CORES_LIST=(8 4 2 1)

SPARK_DRIVER_MEM="200g"

SCRIPT_BASELINE="./single_run_sql.py"
SCRIPT_USPS="./single_run_usps.py"

QUERIES_DIR="/mnt/ssd/chuxuan/rebuttal/queries"
RESULTS_DIR="./results"
OUTPUT_PATH="./power_results/core4/"

POWER_SCRIPT="/home/usps/smartssd/USPS-SmartSSD-20231117/isp_kernel/tool/measure_power.sh"
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
#             SINGLE QUERY EXPERIMENT RUN
###############################################################################
run_experiment() {
  local query_id="$1"
  local mode="$2"             # "baseline" or "usps"
  local py_script="$3"
  local spark_cores="$4"
  local spark_mem="$5"
  local chunk_dir="$6"
  local json_dir="$7"
  local hash_config="$8"      # e.g., "64", "128", "256", or "ideal"

  local tmpfs_dir_arg=""
  local logfile=""
  if [ "$mode" = "baseline" ]; then
    tmpfs_dir_arg="/tmpfs_test/100gb/baseline"
    logfile="./fig8/runningtime_baseline_${spark_cores}cores_${hash_config}.txt"
  elif [ "$mode" = "usps" ]; then
    tmpfs_dir_arg="/tmpfs_test/100gb/usps"
    logfile="./fig8/runningtime_usps_${spark_cores}cores_${hash_config}.txt"
  else
    echo "[ERROR] Unknown mode: ${mode}. Must be 'baseline' or 'usps'."
    exit 1
  fi

  local output_name="Q${query_id}-${mode}-${hash_config}-${spark_cores}cores"

  echo "====================================================================="
  echo "[INFO] Starting query Q${query_id} in mode '${mode}' with hash configuration '${hash_config}' using ${spark_cores} cores"
  echo "       Chunk Dir: ${chunk_dir}"
  echo "       JSON Dir:  ${json_dir}"
  echo "       Log file:  ${logfile}"
  echo "====================================================================="

  spark-submit \
    --name "q${query_id} ${mode} ${hash_config} ${spark_cores}cores" \
    --master local["${spark_cores}"] \
    --conf "spark.driver.memory=${spark_mem}" \
    "${py_script}" \
      -i "${query_id}" \
      -c "${spark_cores}" \
      --tmpfs-dir "${tmpfs_dir_arg}" \
      --chunk-dir "${chunk_dir}" \
      --json-dir "${json_dir}" \
      --queries-dir "${QUERIES_DIR}" \
      --log-file "${logfile}" \
      --results-dir "${RESULTS_DIR}" &

  BENCHMARK_PID=$!

  sar -u 1 > "${OUTPUT_PATH}${output_name}_cpu_utilization_stat.txt" &
  SAR_PID=$!

  iostat -x 1 > "${OUTPUT_PATH}${output_name}_io_stat.txt" &
  IOSTAT_PID=$!

  bash "${POWER_SCRIPT}" 6000 > "${OUTPUT_PATH}${output_name}_power_stat.txt" &
  POWER_PID=$!

  sudo "${PCM_PCIE}" 1 -silent -e -B -csv="${OUTPUT_PATH}${output_name}_pcie_stat.csv" &
  PCM_PCIE_PID=$!

  while kill -0 $BENCHMARK_PID 2> /dev/null; do
    sleep 1
  done

  echo "[INFO] Spark job done for Q${query_id} (${mode}) with hash configuration '${hash_config}' using ${spark_cores} cores. Stopping stats..."
  sudo kill $SAR_PID
  sudo kill $IOSTAT_PID
  sudo kill $POWER_PID
  sudo killall pcm-pcie

  echo "[INFO] Finished query Q${query_id} in mode '${mode}' with hash configuration '${hash_config}' using ${spark_cores} cores."
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
  echo "[WARN] Cleanup complete. Exiting."
  exit 1
}

trap cleanup_on_interrupt INT

###############################################################################
#             MAIN: RUN QUERY 14 FOR EACH HASH CONFIGURATION AND CORE COUNT
###############################################################################
for hash_config in "${HASH_CONFIG_LIST[@]}"; do
  if [ "$hash_config" = "64" ]; then
    JSON_DIR_THIS="/mnt/ssd/chuxuan/rebuttal/jsons64"
    CHUNK_DIR_THIS="/mnt/ssd/chuxuan/100gb-dp-64/chunk"
  elif [ "$hash_config" = "128" ]; then
    JSON_DIR_THIS="/mnt/ssd/chuxuan/rebuttal/jsons128"
    CHUNK_DIR_THIS="/mnt/ssd/chuxuan/100gb-dp-128/chunk"
  elif [ "$hash_config" = "256" ]; then
    JSON_DIR_THIS="/mnt/ssd/chuxuan/rebuttal/jsons"
    CHUNK_DIR_THIS="/mnt/ssd/chuxuan/100gb-dp/chunk"
  elif [ "$hash_config" = "ideal" ]; then
    JSON_DIR_THIS="/mnt/ssd/chuxuan/rebuttal/jsons_ideal"
    CHUNK_DIR_THIS="/mnt/ssd/chuxuan/100gb-dp-ideal/chunk"
  else
    echo "[ERROR] Unknown hash configuration: ${hash_config}"
    exit 1
  fi

  for spark_cores in "${CORES_LIST[@]}"; do
    run_experiment "$QUERY_ID" "baseline" "$SCRIPT_BASELINE" "$spark_cores" "$SPARK_DRIVER_MEM" "$CHUNK_DIR_THIS" "$JSON_DIR_THIS" "$hash_config"
    run_experiment "$QUERY_ID" "usps" "$SCRIPT_USPS" "$spark_cores" "$SPARK_DRIVER_MEM" "$CHUNK_DIR_THIS" "$JSON_DIR_THIS" "$hash_config"
  done
done

echo "[INFO] All experiments for query ${QUERY_ID} completed."
