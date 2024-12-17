# QME
Code for QME bias correction method

This repository contains the code for running the QME method, developed by Andrew Dowdy in IDL and translated to Python by Andrew Gammon for use by the ACS.

A simple example notebook has been provided, as well as an example Python script (with slightly more advanced application as it uses the account trend option) and an accompanying example PBS job submission if being run in the NCI environment (noting that it requires access to projects hh5 and dk92, and that it should be adjusted according to where data will be read and stored, and which project the job should be submitted under).

## More about Python QME

The Python implementation of QME aims to take advantage of the flexibility of Python and the Dask framework while remaining accurate to the original IDL implementation of QME. The implementation relies primarily on xarray and Numpy to speed up the processing of large arrays. xarray's apply_ufunc easily allows work to be distributed by Dask to the cores available to the job. 

The code is divided into different files depending on the role of each function, with some created to assist in the execution of the code. These include a rounding function written as a workaround for Numpy's rounding, which rounds towards evens instead of upwards for halfway values like IDL. Most of these can be ignored by the end user, and the simple template in the example files should be usable for most if not all datasets (which includes some default parameters).

The algorithm consists of 3 main steps:

1) Calculating the distribution histograms for the model data and observational data. This is done by first applying lower and upper limits to the data, then mapping the data to values between 0 and a given maximum (by default 500, but this can be adjusted up or down depending on the required resolution) using a scaling function. The scaling function can be a simple linear equation or anything more complicated (such as logarithmic like has been used for pr).

The difference in this step between IDL and Python is greatly increased flexibility for setting up different variables, including an option for the code to compute linear scaling factors itself instead of requiring the user to do so, which is convenient for testing minor adjustments to limits or resolution.

2) Applying the QME function to calculate adjustment factors given the two sets of histograms. This code has been written to closely match the original IDL code, including with many of its parameters.

3) Applying the adjustment factors to the given data (future or historical). This step by default does not include the "accounting for trend" option that the IDL code applies for temperature runs, though this can still be accomplished with a couple more steps as shown in the example script.

Extra notes:
- The data must be rechunked with continuous time rather than continuous lat and lon as it is usually stored in. Unlike the multivariate methods, this can be done at runtime and should not need any special pre-processing.
- lat and lon values must match exactly between the model and reference data - there is no attempt at regridding. Time values do not have to match (and if leap days are excluded from the model data they will be excluded from the output).
- Currently the Python code only supports calculating factors on a monthly basis. Previously the IDL version has been applied at a seasonal scale. This could be achieved with a few modifications to the Python code - specifically, in steps 1 and 3 - but is not currently implemented.
- A couple of the extra features of the IDL code (in particular, applying the location mask and creating diagnostic figures) were deemed unnecessary for this implementation and were excluded.
