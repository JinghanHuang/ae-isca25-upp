#!/bin/bash
if [ "$1" != "usps" ] && [ "$1" != "baseline" ]; then
    echo "Usage: $0 [usps|baseline] [additional spark args...]"
    exit 1
fi

MODE="$1"

source /tools/Xilinx/Vivado/2021.2/settings64.sh
source /opt/xilinx/xrt/setup.sh
source /tools/Xilinx/Vitis/2021.2/settings64.sh
source /tools/Xilinx/Vitis_HLS/2021.2/settings64.sh
export PLATFORM_REPO_PATHS=/opt/xilinx/platforms

OUTPUT_PATH="./output_results/"
mkdir -p "$OUTPUT_PATH"

BENCHMARK_PID=""
SAR_PID=""
IOSTAT_PID=""
POWER_PID=""
PCM_PCIE_PID=""


cleanup() {
    echo "Caught interrupt. Cleaning up background processes..."
    [[ -n "$BENCHMARK_PID" ]] && kill "$BENCHMARK_PID" 2>/dev/null
    [[ -n "$SAR_PID" ]] && kill "$SAR_PID" 2>/dev/null
    [[ -n "$IOSTAT_PID" ]] && kill "$IOSTAT_PID" 2>/dev/null
    [[ -n "$POWER_PID" ]] && kill "$POWER_PID" 2>/dev/null
    sudo killall pcm-pcie 2>/dev/null
    exit 1
}

trap cleanup SIGINT SIGTERM

for i in {1..22}; do
    if [ "$MODE" = "usps" ]; then
        OUTPUT_NAME="Q${i}-usps"
        SCRIPT_NAME="single_run_usps.py"
        SPARK_JOB_NAME="q${i} usps"
    else
        OUTPUT_NAME="Q${i}-baseline"
        SCRIPT_NAME="single_run_sql.py"
        SPARK_JOB_NAME="q${i} baseline"
    fi

    command_line="${@:2}"

    spark-submit --name "$SPARK_JOB_NAME" --master local[4] --conf "spark.driver.memory=200g" "$SCRIPT_NAME" -i "$i" -c 4 $command_line &
    BENCHMARK_PID=$!

    sar -u 1 > "${OUTPUT_PATH}${OUTPUT_NAME}_cpu_utilization_stat.txt" &
    SAR_PID=$!
    iostat -x 1 > "${OUTPUT_PATH}${OUTPUT_NAME}_io_stat.txt" &
    IOSTAT_PID=$!
    bash /home/usps/smartssd/USPS-SmartSSD-20231117/isp_kernel/tool/measure_power.sh 6000 > "${OUTPUT_PATH}${OUTPUT_NAME}_power_stat.txt" &
    POWER_PID=$!
    sudo /home/usps/pcm/build/bin/pcm-pcie 1 -silent -e -B -csv="${OUTPUT_PATH}${OUTPUT_NAME}_pcie_stat.csv" &
    PCM_PCIE_PID=$!

    while kill -0 $BENCHMARK_PID 2> /dev/null; do
        sudo cat /sys/class/powercap/intel-rapl:0/energy_uj >> "${OUTPUT_PATH}${OUTPUT_NAME}_cpu_0_power.txt"
        sudo cat /sys/class/powercap/intel-rapl:1/energy_uj >> "${OUTPUT_PATH}${OUTPUT_NAME}_cpu_1_power.txt"
        sleep 1
    done

    sudo kill $SAR_PID
    sudo kill $IOSTAT_PID
    sudo kill $POWER_PID
    sudo killall pcm-pcie

    BENCHMARK_PID=""
    SAR_PID=""
    IOSTAT_PID=""
    POWER_PID=""
    PCM_PCIE_PID=""
done
