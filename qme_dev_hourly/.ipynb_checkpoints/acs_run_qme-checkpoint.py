import xarray as xr
import numpy as np

import os
import sys

import yaml
from datetime import datetime
import cmdline_provenance as cmdprov

from qme_train import *
from qme_apply import *

import dataset_finder.dataset_finder as dataset_finder

import dask
from dask.distributed import Client, LocalCluster, wait


# set as appropriate
# adjust specific output filenames in the to_netcdf arguments
grid="AUST-11i"
# outdir = f"/g/data/ia39/australian-climate-service/release/CORDEX/output-Adjust/CMIP6/bias-adjusted-output/{grid}/"
outdir = f"/g/data/ia39/australian-climate-service/test-data/CORDEX/output-CMIP6/bias-adjusted-output/{grid}/"

# AGCD data uses slightly different variable names
agcd_to_standard = {"precip": "pr", "tmax": "tasmax", "tmin": "tasmin"}


def adjust_attrs(ds, var):
    
    # general attributes
    fconfig = f'acs_attrs_{ref}_{empat}.yml'
    with open(fconfig, 'r') as fstream:
        config = yaml.safe_load(fstream)
    
    for original, new in config['rename'].items():
        if original in ds.attrs:
            ds.attrs[new] = ds.attrs.pop(original)
    
    for item in config['delete']:
        if item in ds.attrs:
            ds.attrs.pop(item)
    
    ds = ds.assign_attrs(config['add'])
    ds = ds.assign_attrs({"creation_date": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')})
    
    # variable specific
    ds[var].attrs['long_name'] = f"Bias-Adjusted {ds[var].attrs['long_name']}"
    ds = ds.rename_vars({var: f'{var}Adjust'})

    ds.attrs['history'] = cmdprov.new_log(code_url = "https://github.com/AusClimateService/QME") + '\n' + ds.attrs['history']
    
    return ds


def standardise_latlon(ds, digits=2):
    """
    This function rounds the latitude / longitude coordinates to the 4th digit, because some dataset
    seem to have strange digits (e.g. 50.00000001 instead of 50.0), which prevents merging of data.
    """
    ds = ds.assign_coords({"lat": np.round(ds.lat.astype('float64'), digits)}) # after xarray version update, convert first before rounding
    ds = ds.assign_coords({"lon": np.round(ds.lon.astype('float64'), digits)})
    return(ds)


def preprocess_ds(ds):
    # set errant time values to datetime (no-leap models will have conflicting time values otherwise)
    if ds.time.dtype == "O":
        ds = ds.assign_coords(time = ds.indexes['time'].to_datetimeindex(True))
    
    ds = standardise_latlon(ds)

    for key in agcd_to_standard:
        if key in ds.variables:
            ds = ds.rename({key: agcd_to_standard[key]})

    # round all time values to midday for consistency
    # may not be necessary for univariate methods, depending on the gcm and ref data
    # ds = ds.assign_coords(time = ds.time.dt.floor("D") + np.timedelta64(12, 'h')) 
    
    return ds



if __name__ == "__main__":

    # Set up Dask cluster from the scheduler
    client = Client(scheduler_file = os.environ["DASK_PBS_SCHEDULER"])
    # Upload the four files containing the QME functions so that the workers can access them
    client.upload_file("qme_utils.py")
    client.upload_file("qme_vars.py")
    client.upload_file("qme_train.py")
    client.upload_file("qme_apply.py")

    gcm = sys.argv[1]
    empat = sys.argv[2]
    mdl_run = sys.argv[3]
    org = sys.argv[4]
    rcm = sys.argv[5]
    ref = sys.argv[6]
    var = sys.argv[7]

    chunk_lat=20 
    chunk_lon=20 
    chunk_time=-1 

    if ref == "AGCDv1":
        train_yr_start = 1960
        train_yr_end = 2022

    elif ref == "BARRAR2":
        train_yr_start = 1980
        train_yr_end = 2022
    
    apply_yr_start = 1960
    apply_yr_end = 2100 if org != "CSIRO" else 2099


    train_empat = "ssp370"
    
    if ref == "AGCDv1":
        for key, value in agcd_to_standard.items():
            if var == value:
                agcd_var = key
        ref_path_start = f'/g/data/xv83/agcd-csiro/{agcd_var}/daily/{agcd_var}{"-total" if var == "pr" else ""}_AGCD-CSIRO_r005_'
        ref_file_list = [ref_path_start + '{year}0101-{year}1231_daily.nc'.format(year=y) for y in range(train_yr_start, train_yr_end + 1)]
        
    elif ref == "BARRAR2":
        ref_file_list = dataset_finder.get_datasets("ACS_DD_1hr_wbgt", gcm = "ERA5", rcm = "BARRAR2", timescale = '1hr', exact_match = True).select(var = var, year = dataset_finder.year_range(train_yr_start, train_yr_end), exact_match = True).get_files()
        
    else:
        raise ValueError("Unknown reference data")
    

    hist_dataset = dataset_finder.get_datasets("ACS_DD_1hr_wbgt", org = org, gcm = gcm, scenario = ("historical", train_empat), mdl_run = mdl_run, rcm = rcm, timescale = '1hr', exact_match = True).select(var = var, exact_match = True)
    hist_file_list = hist_dataset.select(year = dataset_finder.year_range(train_yr_start, train_yr_end),remove_empty=False).get_files()

    fut_dataset = dataset_finder.get_datasets("ACS_DD_1hr_wbgt", org = org, gcm = gcm, scenario = ("historical", empat), mdl_run = mdl_run, rcm = rcm, timescale = '1hr', exact_match = True).select(var = var, exact_match = True)
    fut_file_list = fut_dataset.select(year = dataset_finder.year_range(apply_yr_start, apply_yr_end),remove_empty=False).get_files()
    
    # Whether to account for trend with moving average
    if var in ("tasmax", "tasmin", "tas", "wbgt"):
        account_trend = True
        
        # mdl_raw_means = xr.open_mfdataset([file for file in fut_file_list if os.path.isfile(file)], preprocess = preproc_find_mean, parallel = True).persist()
        mdl_raw_means = xr.open_mfdataset(fut_file_list, preprocess=preproc_find_mean, parallel = True, engine='h5netcdf',chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}).persist()  
        print('mdl_raw_means done')

        mdl_means = get_smoothed_mean_values(mdl_raw_means.chunk({"values": -1}), 31, 15).persist()
        wait(mdl_means)
        print('mdl_means done')
        del mdl_raw_means
    else:
        account_trend = False
    
    # ref_data_full = xr.open_mfdataset(ref_file_list, preprocess = preprocess_ds, parallel = True)#.rename({"precip": "pr"})
    # hist_data_full = xr.open_mfdataset(hist_file_list, parallel = True)
    ref_data_full = xr.open_mfdataset(ref_file_list, preprocess = preprocess_ds, parallel = True, engine='h5netcdf',chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}) 
    hist_data_full = xr.open_mfdataset(hist_file_list, parallel = True, engine='h5netcdf',chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}) 
    
    bnds_list = ['lat_bnds', 'lon_bnds', 'time_bnds']

    # ref_data = ref_data_full.drop_vars(bnds_list).chunk(chunks={"lat": chunk_size, "lon": chunk_size, "time": -1}).sel(time = ref_data_full.time.dt.year.isin(range(train_yr_start, train_yr_end + 1)))
    # hist_data = hist_data_full.drop_vars(bnds_list).chunk(chunks={"lat": chunk_size, "lon": chunk_size, "time": -1}).sel(time = hist_data_full.time.dt.year.isin(range(train_yr_start, train_yr_end + 1)))       
    ref_data = ref_data_full.drop_vars(bnds_list).chunk(chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}).sel(time = ref_data_full.time.dt.year.isin(range(train_yr_start, train_yr_end + 1))) 
    hist_data = hist_data_full.drop_vars(bnds_list).chunk(chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}).sel(time = hist_data_full.time.dt.year.isin(range(train_yr_start, train_yr_end + 1))) 
   
    ref_data_full.close()
    hist_data_full.close()

    
    # Parameters for training
    params = {
        "xtr": 3,
        "cal_smth": 21,
        "mthd": 'quick',
        "mn_smth": '3mn' if var == "pr" else None,
        "ssze_lim": 50,
        "mltp": False,
        "lmt": 1.5 if var == "pr" else None,
        "lmt_thresh": 10,
        "retain_zero": var == "pr"
    }   
    
    # Create distributions histograms   
    hourly_bc = {}        
    # Group data by hour 
    ref_grouped = ref_data.groupby("time.hour")
    hist_grouped = hist_data.groupby("time.hour")
    
    for hr in range(24):
        ref_hr = ref_grouped[hr]
        hist_hr = hist_grouped[hr]    
        
         # Compute hourly bias correction distributions
        dist_obs_hr = make_dist(var, ref_hr).chunk({"values": -1, "month": -1}).persist()
        dist_mdl_hr = make_dist(var, hist_hr).chunk({"values": -1, "month": -1}).persist()

        # Apply QME method to distributions to calculate bias correction factors
        dist_bc_hr = calc_qme(var, dist_mdl_hr, dist_obs_hr, **params).chunk({"values": -1, "month": -1}).persist()
        wait(dist_bc_hr)
        hourly_bc[hr] = dist_bc_hr
    print("Hourly bias-correction factors computed.")

    # Apply hourly bias correction to model data
    for file in fut_file_list:
        fut_data = xr.open_dataset(file, engine="h5netcdf")
        fut_data_pre = fut_data.drop_vars(bnds_list).chunk({"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}).sel(time = fut_data.time.dt.year.isin(range(apply_yr_start, apply_yr_end + 1)))

        year_start = fut_data_pre.time.dt.year.values[0]
        year_end = fut_data_pre.time.dt.year.values[-1]

        if empat != train_empat and year_end <= 2014:
            continue

        if org == "CSIRO":
            version = "v20251120"
        elif org == "BOM":
            version = "v20250901"
        else:
            raise ValueError(f"{org} is unknown.")
            
        path = outdir + f'{org}/{gcm}/{"historical" if year_end <= 2014 else empat}/{mdl_run}/{rcm}/v1-r1-ACS-QME-{ref}-{train_yr_start}-{train_yr_end}/1hr/{var}Adjust/{version}/'
        if not os.path.exists(path):
            os.makedirs(path)
        
        outfile = os.path.join(path, f"{var}Adjust_{grid}_{gcm}_{'historical' if year_end <= 2014 else empat}_{mdl_run}_{org}_{rcm}_v1-r1-ACS-QME-{ref}-{train_yr_start}-{train_yr_end}_1hr_{year_start}0101-{year_end}1231.nc")
        ## If file exists, delete file
        # if os.path.exists(outfile):
        #     os.remove(outfile)
        # If file exists, skip to next iteration
        if os.path.exists(outfile):
            print(f"File already exists, skipping: {outfile}")
            continue  
        

        # Apply hourly correction 
        corrected_chunks = []
        fut_grouped = fut_data_pre.groupby("time.hour")
        for hr in range(24):
            fut_hr = fut_grouped[hr]           
            # Apply trend adjustment before bias correction
            if account_trend:
                fut_hr = apply_mean_value_yr(fut_hr, -mdl_means, apply_yr_start + 15)    
            # Apply hourly bias correction
            fut_hr_bc = apply_bc(var, fut_hr, hourly_bc[hr], chunked=True)    
            # Apply trend adjustment after bias correction
            if account_trend:
                fut_hr_bc = apply_mean_value_yr(fut_hr_bc, mdl_means, apply_yr_start + 15)
    
            corrected_chunks.append(fut_hr_bc)
        fut_data_bc = xr.concat(corrected_chunks, dim="time").sortby("time")
        print(f"Hourly bias-correction applied {year_start}.")
        fut_data_bc = adjust_attrs(fut_data_bc, var)
        for item in bnds_list:
            fut_data_bc[item] = fut_data[item]

        fut_data_bc = fut_data_bc.unify_chunks().load()
        fut_data_bc.to_netcdf(outfile, engine="netcdf4")
        print(f"Hourly bias-corrected output generated {year_start}.")
        fut_data.close()
        del fut_data_bc               
        del corrected_chunks
        del fut_grouped


    
