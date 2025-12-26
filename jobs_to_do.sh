submit_jobs () {
    dir_path="logs/calc/$6/$1/$5/$3/$2"
    timestamp=$(date +"%m_%d_%H-%M-%S")
    mkdir -p $dir_path

    job=$(qsub -v gcm=$1,empat=$2,mdl_run=$3,org=$4,rcm=$5,ref=$6,var=$7 -j oe -o "$dir_path/$7-$timestamp.OU" job-general.pbs)
    echo $job
}

#submit_jobs "ACCESS-CM2" "ssp126" "r4i1p1f1" "BOM" "BARPA-R" "BARRAR2" "prsn"
#submit_jobs "ACCESS-CM2" "ssp370" "r4i1p1f1" "BOM" "BARPA-R" "BARRAR2" "prsn"
submit_jobs "ACCESS-CM2" "ssp126" "r4i1p1f1" "CSIRO" "CCAM-v2203-SN" "BARRAR2" "prsn"
#submit_jobs "ACCESS-CM2" "ssp370" "r4i1p1f1" "CSIRO" "CCAM-v2203-SN" "BARRAR2" "prsn"
