import xarray as xr
import numpy as np

import os
import sys
import yaml
from datetime import datetime

import dataset_finder.dataset_finder as dataset_finder

import dask
from dask.distributed import Client, LocalCluster

# outdir = "/scratch/eg3/ag0738/qme/NHP/"
# outdir = "/scratch/eg3/ag0738/mrnbc/acs/converted/"
# outdir = "/scratch/eg3/ag0738/mrnbc/acs/new_ver_nc/"
# outdir = "/g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/bias-adjustment-output/AGCD-05i/"
# outdir = "/g/data/ia39/national-hydrological-projections/test-data/CORDEX-CMIP6/bias-adjustment-output/AGCD-05i/"

# def preprocess_ds(ds):
#     # set errant time values to datetime (no-leap models will have conflicting time values otherwise)
#     if ds.time.dtype == "O":
#         ds = ds.assign_coords(time = ds.indexes['time'].to_datetimeindex(True)) 
#     return ds
        
if __name__ == "__main__":

    # Set up Dask cluster from the scheduler
    client = Client(scheduler_file = os.environ["DASK_PBS_SCHEDULER"])
    
    gcm = sys.argv[1]
    scenario = sys.argv[2]
    mdl_run = sys.argv[3]
    org = sys.argv[4]
    rcm = sys.argv[5]
    bc = sys.argv[6]    

    # check_vars = [
    #     # "pr",
    #     # "tasmax",
    #     # "tasmin",
    #     # "rsds",
    #     # "sfcWindmax",
    #     # "hursmax",
    #     # "hursmin",
    #     "psl",
    #     "sfcWind"
    # ]    

    check_vars = ['hurs', 'huss', 'pr', 'prsn', 'ps', 'psl', 'rlds', 'rsds', 'sfcWind', 'tas', 'tasmax', 'tasmin']

    print()
    
    # for ref in "AGCDv1", "BARRAR2":
    # for ref in "AGCDv1",:
    for ref in "BARRAR2",:
        # for var in "tasmax", "tasmin", "pr", "rsds":
        # for var in "tasmax",:
        for var in check_vars:
    

            if ref == "AGCDv1":
                if var not in ("pr", "tasmax", "tasmin"):
                    continue

            else:
                if var == "sfcWindmax":
                    if org == "UQ-DEC":
                        if not (gcm == "ACCESS-ESM1-5" and mdl_run == "r40i1p1f1" and scenario == "ssp370"):
                            continue

            long_var = var + "Adjust"
            
            if ref == "AGCDv1":
                train_yr_start = 1960
                train_yr_end = 2022
        
            elif ref == "BARRAR2":
                train_yr_start = 1980
                train_yr_end = 2022

            apply_yr_start = 1960 if scenario == "ssp370" else 2015
            apply_yr_end = 2100 if org != "CSIRO" else 2099

            # apply_yr_end = 2022
            
            # apply_yr_start = 1960
            # apply_yr_end = 1974
        
            files = []
            
            # files = dataset_finder.get_datasets("ACS_BC", org = org, gcm = gcm, scenario = ("historical", scenario) if scenario == "ssp370" else scenario, mdl_run = mdl_run, rcm = rcm, bc = bc, ref = ref).select(var = long_var).get_files()
            files = dataset_finder.get_datasets("ACS_BC", org = org, gcm = gcm, scenario = ("historical", scenario) if scenario == "ssp370" else scenario, mdl_run = mdl_run, rcm = rcm, bc = bc, ref = ref).select(var = long_var).get_files()
            
            perform_check = True

            if not files:
                print(f'Missing {var}')
                perform_check = False
        
            # for year in range(apply_yr_start, apply_yr_end + 1):


            #     path = outdir + f'{org}/{gcm}/{"historical" if year <= 2014 else scenario}/{mdl_run}/{rcm}/v1-r1-ACS-{bc}-{ref}-{train_yr_start}-{train_yr_end}/day/{var}Adjust/v20250311/'
            #     file = path + f'{var}Adjust_AUST-05i_{gcm}_{"historical" if year <= 2014 else scenario}_{mdl_run}_{org}_{rcm}_v1-r1-ACS-{bc}-{ref}-{train_yr_start}-{train_yr_end}_day_{year}0101-{year}1231.nc'
                
            #     if os.path.isfile(file):                    
            #         files.append(file)

            #     else:
            #         if year == apply_yr_start:
            #             print(f'Missing {var}')
            #             perform_check = False
            #             break
                        
            #         print(f'Missing year {year}')

            if perform_check:
        
                # data = xr.open_mfdataset(files, preprocess = preprocess_ds, chunks = {"time": 27}, parallel = True)
                data = xr.open_mfdataset(files, chunks = {"time": 27}, parallel = True)
                if ref == "AGCDv1":
                    # 277751 values are part of the mask; 334475 are excluded of 612226 (691 * 886) total per timestep
                    total_data_count = 277751 * data.time.size
                else:
                    total_data_count = data[long_var].size
                missing_data_count = total_data_count - (data[long_var]).count().compute().item()

                print(f'Missing value count: {missing_data_count} of {total_data_count} ({missing_data_count/total_data_count:.3%}) for {var} with {ref}')

                if var == "pr" and bc == "MRNBC":
                    high_data = data[long_var].where(data[long_var] > 999)
                    high_data_counts = high_data.count("time")
                    highest_data_count = high_data_counts.max().compute().item()
                    if highest_data_count > 10:
                        print(f'High pr count: {highest_data_count}')
                
                data.close()

        print()
