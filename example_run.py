import xarray as xr
import numpy as np

import os
import sys

from qme_train import *
from qme_apply import *

import dask
from dask.distributed import Client, LocalCluster


# set as appropriate
# adjust specific output filenames in the to_netcdf arguments
outdir = ""


def standardise_latlon(ds, digits=2):
    """
    This function rounds the latitude / longitude coordinates to the 4th digit, because some dataset
    seem to have strange digits (e.g. 50.00000001 instead of 50.0), which prevents merging of data.
    """
    ds = ds.assign_coords({"lat": np.round(ds.lat, digits).astype('float64')})
    ds = ds.assign_coords({"lon": np.round(ds.lon, digits).astype('float64')})
    return(ds)


def preprocess_ds(ds):
    # set errant time values to datetime (no-leap models will have conflicting time values otherwise)
    if ds.time.dtype == "O":
        ds = ds.assign_coords(time = ds.indexes['time'].to_datetimeindex()) 
    
    ds = standardise_latlon(ds)
    ds = ds.drop_vars([item for item in ('height', 'lat_bnds', 'lon_bnds', 'time_bnds') if item in ds.variables or item in ds.dims])
   
    return ds


def qme_run(var, ref_data, hist_data, fut_data = None):
        
    # Parameters for training
    params = {
        "xtr": 3,
        "cal_smth": 21,
        "mthd": 'quick',
        "mn_smth": '3mn' if var == "pr" else '',
        "ssze_lim": 50,
        "mltp": False,
        "lmt": 1.5 if var == "pr" else -1,
        "lmt_thresh": 10,
        "retain_zero": var == "pr"
    }
    
    # Whether to account for trend with moving average
    if var in ("tasmax", "tasmin"):
        account_trend = True
    else:
        account_trend = False    

    # Create distributions histograms
    dist_obs = make_dist(var, ref_data).chunk({"values": -1, "month": -1})
    dist_mdl = make_dist(var, hist_data).chunk({"values": -1, "month": -1})

    # Apply QME method to distributions to calculate bias correction factors
    dist_bc = calc_qme(var, dist_mdl, dist_obs, **params).chunk({"values": -1, "month": -1}).persist()

    mdl_mean = None
    fut_mdl_mean = None

    # Apply bias correction to model data
    if account_trend:
        # Find model means
        mdl_mean = find_means(hist_data).chunk({"values": -1}).persist()
        
        mdl_bc = apply_mean_values(hist_data, -mdl_mean, 15)
        mdl_bc = apply_bc(var, mdl_bc, dist_bc)
        mdl_bc = apply_mean_values(mdl_bc, mdl_mean, 15)
        
    else:
        # Apply the bias correction to model data without means
        mdl_bc = apply_bc(var, hist_data, dist_bc)

    if fut_data:
        if account_trend:
            # Find model means (note that this is calculated slightly differently for future data)
            fut_mdl_mean = future_means(hist_data, fut_data).chunk({"values": -1}).persist()
            
            fut_bc = apply_mean_values(fut_data, -fut_mdl_mean)
            fut_bc = apply_bc(var, fut_bc, dist_bc)
            fut_bc = apply_mean_values(fut_bc, fut_mdl_mean)
            
        else:
            # Apply the bias correction to future data without means
            fut_bc = apply_bc(var, fut_data, dist_bc)
        
    # Write bias corrected historical data
    mdl_bc = mdl_bc.chunk({'time': 'auto', 'lat': -1, 'lon': -1})
    mdl_bc.to_netcdf(outdir + f'{var}_historical.nc', # short output name for the sake of example
                   unlimited_dims=['time'],
                   encoding={'time': {'dtype': 'float32'}})

    if fut_data:
        # Write bias corrected projection data
        fut_bc = fut_bc.chunk({'time': 'auto', 'lat': -1, 'lon': -1})
        fut_bc.to_netcdf(outdir + f'{var}_projection.nc', 
                       unlimited_dims=['time'],
                       encoding={'time': {'dtype': 'float32'}})
    
    # Clean up for next round (just these in particular as they were called with "persist" to keep them in memory for the various operations they were required for)
    del dist_bc
    if mdl_mean is not None:
        del mdl_mean
    if fut_mdl_mean is not None:
        del fut_mdl_mean


if __name__ == "__main__":

    # Set up Dask cluster from the scheduler
    client = Client(scheduler_file = os.environ["DASK_PBS_SCHEDULER"])

    # Upload the four files containing the QME functions so that the workers can access them
    client.upload_file("qme_utils.py")
    client.upload_file("qme_vars.py")
    client.upload_file("qme_train.py")
    client.upload_file("qme_apply.py")

    mdl = "CSIRO-ACCESS-ESM1-5"
    mdl_params = "r6i1p1f1"
    downscaling = "BOM-BARPA-R"

    var = "pr"
    # var = "tasmax"
    # var = "tasmin"
    
    chunk_size = 25

    train_yr_start = 1980
    train_yr_end = 2019
    
    proj_yr_start = 2060
    proj_yr_end = 2099
    
    start_lat = -44
    end_lat   = -10
    start_lon = 112
    end_lon   = 154

    ref_path = '/g/data/ia39/npcp/data/{var}/observations/AGCD/raw/task-reference/{var}_NPCP-20i_AGCD_v1-0-1_day_{year}0101-{year}1231.nc'    
    gcm_path = '/g/data/ia39/npcp/data/{var}/{mdl}/{downscaling}/raw/task-reference/{var}_NPCP-20i_{mdl}_{empat}_{params}_{downscaling}_v1_day_{year}0101-{year}1231.nc'

    ref_file_list = [ref_path.format(var=var, year=y) for y in range(train_yr_start, train_yr_end + 1)]
    hist_file_list = [gcm_path.format(var=var, year=y, mdl = mdl, downscaling = downscaling, params = mdl_params, 
                                      empat = 'evaluation' if mdl == 'ECMWF-ERA5' else ('historical' if y <= 2014 else 'ssp370')) 
                      for y in range(train_yr_start, train_yr_end + 1)]
    fut_file_list = [gcm_path.format(var=var, year=y, mdl = mdl, downscaling = downscaling, params = mdl_params, 
                                     empat = 'evaluation' if mdl == 'ECMWF-ERA5' else ('historical' if y <= 2014 else 'ssp370'))
                     for y in range(proj_yr_start, proj_yr_end + 1)]
    
    ref_data_full = xr.open_mfdataset(ref_file_list, preprocess = preprocess_ds)
    hist_data_full = xr.open_mfdataset(hist_file_list, preprocess = preprocess_ds)
    fut_data_full = xr.open_mfdataset(fut_file_list, preprocess = preprocess_ds)

    ref_data = ref_data_full.sel(lon=slice(start_lon, end_lon),lat=slice(start_lat, end_lat)).chunk(chunks={"lat": chunk_size, "lon": chunk_size, "time": -1})
    hist_data = hist_data_full.sel(lon=slice(start_lon, end_lon),lat=slice(start_lat, end_lat)).chunk(chunks={"lat": chunk_size, "lon": chunk_size, "time": -1})
    fut_data = fut_data_full.sel(lon=slice(start_lon, end_lon),lat=slice(start_lat, end_lat)).chunk(chunks={"lat": chunk_size, "lon": chunk_size, "time": -1})

    qme_run(var, ref_data, hist_data, fut_data)
