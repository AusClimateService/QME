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


# Hourly implementation (qme_dev_hourly branch)
Hourly QME Implementation: Alicia Takbash (Alicia.Takbash@csiro.au)

`qme_dev_hourly` was implemented for hourly bias-correction, adopted from `qme_dev_daily`.

The hourly implementation of the QME code groups data by hour, calculates the adjustment factors on an hourly basis, 
and then applies these hourly-based adjustment factors to the hourly data for each year separately (1960 – 2100). 
Note that the input data must be uncompressed and unchunked. The generated outputs (yearly files) are not compressed and not chunked (i.e., they are contiguous files) 
to enable timely and quick evaluation of the results, as compressed files take longer to read. After the evaluation process, the output files will/can be 
chunked and compressed in a post-processing step for sharing and publishing purposes.

To run the code:

1) Open 

`/qme_dev_hourly/dataset_finder/paths.yml` 

and create or modify a dictionary key to access your input files (reference and model data).

2) Run 

`/qme_dev_hourly/find_runs.ipynb` 

with your specific dictionary key to create the `jobs_to_do.sh` file (e.g., `/qme_dev_hourly/jobs_to_do.sh`).
In the `jobs_to_do.sh` file, uncomment the line for processing historical data, and run only the lines for processing SSP370 and SSP126 one after another. When you run 
SSP370 first, it will create bias-corrected historical and future data (this takes ~24 hours). Once the historical output files are created, 
you can start running SSP126 for future data (this takes ~19 hours).

3) Configure

`/qme_dev_hourly/job-general.pbs` 

if needed, and then run
```bash
bash jobs_to_do.sh
```
This will execute the main code 

`/qme_dev_hourly/acs_run_qme.py` 

Make sure you have specified the output directory here. Also, ensure that your desired variable is defined in 

`/qme_dev_hourly/qme_vars.py` 

before the code execution.

4) Monitor logs and Dask client commands
Logs are saved in: 

`/qme_dev_hourly/logs/`

Dask scheduler tracking is stored in:

`/qme_dev_hourly/client_cmd` 


The performance of the code could be further improved by using futures or delayed objects, among others. This is the goal for the next code update.