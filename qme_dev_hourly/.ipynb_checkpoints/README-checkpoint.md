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
