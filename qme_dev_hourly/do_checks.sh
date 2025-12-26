# Bash script for sending jobs

# empat="ssp370"
empat="ssp126"
bc="MRNBC"
# bc="QME"

submit_jobs () {
    job=$(qsub -v gcm=${gcm},empat=${empat},mdl_run=$1,org=$2,rcm=$3,bc=${bc} -j oe -o "$dir_path/${gcm}-$3-$1.OU" job-check.pbs)
    echo $job
}

timestamp=$(date +"%m_%d_%H-%M-%S")
dir_path="logs/check/$bc/$empat/$timestamp"
mkdir -p $dir_path

gcm="ACCESS-CM2"
submit_jobs "r4i1p1f1" "BOM" "BARPA-R"
submit_jobs "r4i1p1f1" "CSIRO" "CCAM-v2203-SN"
# submit_jobs "r2i1p1f1" "UQ-DEC" "CCAMoc-v2112"
# exit

gcm="ACCESS-ESM1-5"
submit_jobs "r6i1p1f1" "BOM" "BARPA-R"
submit_jobs "r6i1p1f1" "CSIRO" "CCAM-v2203-SN"
# submit_jobs "r20i1p1f1" "UQ-DEC" "CCAMoc-v2112"
# submit_jobs "r40i1p1f1" "UQ-DEC" "CCAMoc-v2112"
# submit_jobs "r6i1p1f1" "UQ-DEC" "CCAM-v2105"
# submit_jobs "r6i1p1f1" "NSW-Government" "NARCliM2-0-WRF412R3"
# submit_jobs "r6i1p1f1" "NSW-Government" "NARCliM2-0-WRF412R5"
# exit

gcm="CESM2"
submit_jobs "r11i1p1f1" "BOM" "BARPA-R"
submit_jobs "r11i1p1f1" "CSIRO" "CCAM-v2203-SN"
# exit

gcm="CMCC-ESM2"
submit_jobs "r1i1p1f1" "BOM" "BARPA-R"
submit_jobs "r1i1p1f1" "CSIRO" "CCAM-v2203-SN" # big flag
# submit_jobs "r1i1p1f1" "UQ-DEC" "CCAM-v2105"

# gcm="CNRM-CM6-1-HR"
# submit_jobs "r1i1p1f2" "UQ-DEC" "CCAMoc-v2112"
# submit_jobs "r1i1p1f2" "UQ-DEC" "CCAM-v2112"
# exit

gcm="CNRM-ESM2-1"
submit_jobs "r1i1p1f2" "CSIRO" "CCAM-v2203-SN"

gcm="EC-Earth3"
submit_jobs "r1i1p1f1" "BOM" "BARPA-R"
submit_jobs "r1i1p1f1" "CSIRO" "CCAM-v2203-SN"
# submit_jobs "r1i1p1f1" "UQ-DEC" "CCAM-v2105"

# gcm="EC-Earth3-Veg"
# submit_jobs "r1i1p1f1" "NSW-Government" "NARCliM2-0-WRF412R3"
# submit_jobs "r1i1p1f1" "NSW-Government" "NARCliM2-0-WRF412R5"

# gcm="FGOALS-g3"
# submit_jobs "r4i1p1f1" "UQ-DEC" "CCAM-v2105"

# gcm="GFDL-ESM4"
# submit_jobs "r1i1p1f1" "UQ-DEC" "CCAM-v2105"

# gcm="GISS-E2-1-G"
# submit_jobs "r2i1p1f2" "UQ-DEC" "CCAM-v2105"

gcm="MPI-ESM1-2-HR"
submit_jobs "r1i1p1f1" "BOM" "BARPA-R"
# submit_jobs "r1i1p1f1" "NSW-Government" "NARCliM2-0-WRF412R3"
# submit_jobs "r1i1p1f1" "NSW-Government" "NARCliM2-0-WRF412R5"

# gcm="MPI-ESM1-2-LR"
# submit_jobs "r9i1p1f1" "UQ-DEC" "CCAM-v2105"

# gcm="MRI-ESM2-0"
# submit_jobs "r1i1p1f1" "UQ-DEC" "CCAM-v2105"

gcm="NorESM2-MM"
submit_jobs "r1i1p1f1" "BOM" "BARPA-R"
submit_jobs "r1i1p1f1" "CSIRO" "CCAM-v2203-SN"
# submit_jobs "r1i1p1f1" "UQ-DEC" "CCAMoc-v2112"
# submit_jobs "r1i1p1f1" "UQ-DEC" "CCAM-v2112"
# submit_jobs "r1i1p1f1" "NSW-Government" "NARCliM2-0-WRF412R3"
# submit_jobs "r1i1p1f1" "NSW-Government" "NARCliM2-0-WRF412R5"

# gcm="UKESM1-0-LL"
# submit_jobs "r1i1p1f2" "NSW-Government" "NARCliM2-0-WRF412R3"
# submit_jobs "r1i1p1f2" "NSW-Government" "NARCliM2-0-WRF412R5"
