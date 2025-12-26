#Quantile Matching for Extremes (QME)

Nonparametric calibration method with attention to extremes including in a changing climate.

Allows spatial gridded data (or point locations) for time steps over a time period to be calibrated to other similar data.

Originally developed in 2019 as research code, with refinements in 2020 and 2021 including for application to new data types and models.
Version 2 in development 2022.

QME Contact: Andrew Dowdy (andrew.dowdy@bom.gov.au)

Python coding: Mitchell Black (mitchell.black@bom.gov.au)

Python translation WIP by Andrew Gammon (andrew.gammon@bom.gov.au)

Python code runnable (by import in Jupyter notebook or from PBS submission, see example qme_run.py and qme_job.pbs scripts).

The Python code can create the required histograms from given data, generate QME bias correction factors and apply them to model data (including future data). It is based on Andrew Dowdy's IDL code including his recent revisions.


# Further comments on the qme_dev_daily branch (Alicia Takbash)
To run the code:

1) Open 
/qme_dev_daily/dataset_finder/paths.yml 
and create or modify a dictionary key to access your input files (reference and model data).

2) Run 
/qme_dev_daily/find_runs.ipynb
with your specific dictionary key to create the `jobs_to_do.sh` file (e.g., /qme_dev_daily/jobs_to_do.sh).
In the `jobs_to_do.sh` file, uncomment the line for processing historical data, and run only the lines for processing SSP370 and SSP126 one after another. When you run 
SSP370 first, it will create bias-corrected historical and future data. Once the historical output files are created, 
you can start running SSP126 for future data.

3) Configure 
/qme_dev_daily/job-general.pbs 
if needed, and then run
```bash
bash jobs_to_do.sh
```
This will execute the main code 
/qme_dev_daily/acs_run_qme.py 
Make sure you have specified the output directory here. Also, ensure that your desired variable is defined in 
/qme_dev_daily/qme_vars.py 
before the code execution.

4) Monitor logs and Dask client commands
Logs are saved in: 
/qme_dev_daily/logs/
Dask scheduler tracking is stored in:
/qme_dev_daily/client_cmd 

