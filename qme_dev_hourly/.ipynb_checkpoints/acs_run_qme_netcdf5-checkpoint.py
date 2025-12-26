import xarray as xr
import numpy as np

import os
import sys

sys.path.append('/g/data/xv83/users/at2708/bias_adjustment_acs_qme/qme_dev_copy_hourly')

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
outdir = "/g/data/xv83/users/at2708/bias_adjustment_acs_qme/qme_dev_copy_hourly/outputs_netcdf5/"
grid = "{grid}"
# outdir = "/g/data/ia39/australian-climate-service/release/CORDEX/output-Adjust/CMIP6/bias-adjusted-output/{grid}/"

# AGCD data uses slightly different variable names
agcd_to_standard = {"precip": "pr", "tmax": "tasmax", "tmin": "tasmin"}

####### Alicia from Damien
def get_output_encoding(ds, var, nlats, nlons):
    """Define the output file encoding."""

    encoding = {}
    ds_vars = list(ds.coords) + list(ds.keys())
    #data type and fill value
    for ds_var in ds_vars:
        if ds_var == var:
            encoding[ds_var] = {'_FillValue': np.float32(1e20)}
            encoding[ds_var]['dtype'] = 'float32'
        else:
            encoding[ds_var] = {'_FillValue': None}
            encoding[ds_var]['dtype'] = 'float64'
    #compression
    encoding[var]['zlib'] = True
    encoding[var]['least_significant_digit'] = 2
    encoding[var]['complevel'] = 5
    if not var == 'orog':
        #chunking
        var_shape = ds[var].shape
        assert len(var_shape) == 3
        assert var_shape[0] == ntimes
        assert var_shape[1] == nlats
        assert var_shape[2] == nlons
        # encoding[var]['chunksizes'] = (1, nlats, nlons)
        encoding[var]['chunksizes'] = (ntimes, 1, 1)
        #time units
        encoding['time']['units'] = 'days since 1950-01-01'

    return encoding
################

def adjust_attrs(ds, var):
    
    # general attributes
    fconfig = f'/g/data/xv83/users/at2708/bias_adjustment_acs_qme/qme_dev_copy_hourly/acs_attrs_{ref}_{empat}.yml'
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
    # print(client) #Alicia
    # print(client.dashboard_link) #Alicia
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

    # chunk_size = 50 
    chunk_lat=1 #Alicia
    chunk_lon=1 #Alicia
    chunk_time=-1 #Alicia

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
        ref_file_list = dataset_finder.get_datasets("ACS_DD_1h_test_5km", gcm = "ERA5", rcm = "BARRAR2", timescale = '1hr', exact_match = True).select(var = var, year = dataset_finder.year_range(train_yr_start, train_yr_end), exact_match = True).get_files()
        
    else:
        raise ValueError("Unknown reference data")
    

    hist_dataset = dataset_finder.get_datasets("ACS_DD_1h_test_5km", org = org, gcm = gcm, scenario = ("historical", train_empat), mdl_run = mdl_run, rcm = rcm, timescale = '1hr', exact_match = True).select(var = var, exact_match = True)
    hist_file_list = hist_dataset.select(year = dataset_finder.year_range(train_yr_start, train_yr_end),remove_empty=False).get_files()

    fut_dataset = dataset_finder.get_datasets("ACS_DD_1h_test_5km", org = org, gcm = gcm, scenario = ("historical", empat), mdl_run = mdl_run, rcm = rcm, timescale = '1hr', exact_match = True).select(var = var, exact_match = True)
    fut_file_list = fut_dataset.select(year = dataset_finder.year_range(apply_yr_start, apply_yr_end),remove_empty=False).get_files()
    
    
    # Whether to account for trend with moving average
    if var in ("tasmax", "tasmin", "tas"):
        account_trend = True
        
        # mdl_raw_means = xr.open_mfdataset([file for file in fut_file_list if os.path.isfile(file)], preprocess = preproc_find_mean, parallel = True).persist()
        mdl_raw_means = xr.open_mfdataset(fut_file_list, engine='netcdf4',chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}, preprocess=preproc_find_mean, parallel = True).persist() #Alicia , engine='netcdf4',chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}
     
        mdl_means = get_smoothed_mean_values(mdl_raw_means.chunk({"values": -1}), 31, 15).persist()
        # mdl_means = get_smoothed_mean_values(mdl_raw_means.chunk({"values": -1}), 31, 15).persist() #Alicia
        wait(mdl_means)
        
    else:
        account_trend = False
    
    # ref_data_full = xr.open_mfdataset(ref_file_list, preprocess = preprocess_ds, parallel = True)#.rename({"precip": "pr"})
    # hist_data_full = xr.open_mfdataset(hist_file_list, parallel = True)
    ref_data_full = xr.open_mfdataset(ref_file_list, engine='netcdf4',chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}, preprocess = preprocess_ds, parallel = True) #Alicia
    hist_data_full = xr.open_mfdataset(hist_file_list, engine='netcdf4',chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}, parallel = True) #Alicia
    
    bnds_list = ['lat_bnds', 'lon_bnds', 'time_bnds']


    # ref_data = ref_data_full.drop_vars(bnds_list).chunk(chunks={"lat": chunk_size, "lon": chunk_size, "time": -1}).sel(time = ref_data_full.time.dt.year.isin(range(train_yr_start, train_yr_end + 1)))
    # hist_data = hist_data_full.drop_vars(bnds_list).chunk(chunks={"lat": chunk_size, "lon": chunk_size, "time": -1}).sel(time = hist_data_full.time.dt.year.isin(range(train_yr_start, train_yr_end + 1)))       
    ref_data = ref_data_full.drop_vars(bnds_list).chunk(chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}).sel(time = ref_data_full.time.dt.year.isin(range(train_yr_start, train_yr_end + 1))) #Alicia
    hist_data = hist_data_full.drop_vars(bnds_list).chunk(chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}).sel(time = hist_data_full.time.dt.year.isin(range(train_yr_start, train_yr_end + 1))) #Alicia
        
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
    dist_obs = make_dist(var, ref_data).chunk({"values": -1, "month": -1}).persist()
    dist_mdl = make_dist(var, hist_data).chunk({"values": -1, "month": -1}).persist()

    # Apply QME method to distributions to calculate bias correction factors
    dist_bc = calc_qme(var, dist_mdl, dist_obs, **params).chunk({"values": -1, "month": -1}).persist()
    wait(dist_bc)

    if ref == "AGCDv1":
        mask_path = "/g/data/mn51/projects/reference_data/AGCD_land_mask.nc"
        mask = standardise_latlon(xr.open_dataset(mask_path).rename({"latitude": "lat", "longitude": "lon"}))

    # for file, year in zip(fut_file_list, range(apply_yr_start, apply_yr_end + 1)):
    for file in fut_file_list:
            
        fut_data = xr.open_dataset(file)#, chunks = {'time': 1})#, chunks = {'time': 27, 'lat': -1, 'lon': -1})
        fut_data_pre = fut_data.drop_vars(bnds_list).chunk(chunks={"time": chunk_time, "lat": chunk_lat, "lon": chunk_lon}).sel(time = fut_data.time.dt.year.isin(range(apply_yr_start, apply_yr_end + 1)))
        # fut_data_pre = fut_data.drop_vars(bnds_list).sel(time = fut_data.time.dt.year.isin(range(apply_yr_start, apply_yr_end + 1)))

        year_start = fut_data_pre.time.dt.year.values[0]
        year_end = fut_data_pre.time.dt.year.values[-1]


        # only replace historical if it was from the ssp370 run
        if empat != train_empat:
            if year_end <= 2014:
                continue
    
        path = outdir + f'{org}/{gcm}/{"historical" if year_end <= 2014 else empat}/{mdl_run}/{rcm}/v1-r1-ACS-QME-{ref}-{train_yr_start}-{train_yr_end}/1hr/{var}Adjust/v20250311/'
        if not os.path.exists(path):
            os.makedirs(path)

        # if not os.path.isfile(file):
        #     print(f'Year {year} does not exist ({file})')
        #     continue

        if ref == "AGCDv1":
            fut_data_pre[var] = fut_data_pre[var].where(mask.mask)
            fut_data_pre = fut_data_pre.assign_attrs({"mask_path": "/g/data/mn51/projects/reference_data/AGCD_land_mask.nc"})
        
        if account_trend:
            # mdl_means = xr.open_dataset(f'/scratch/eg3/ag0738/qme/acs/mdl_means/{var}/{var}_{org}_{gcm}_{mdl_run}_{rcm}_{apply_yr_start}-{apply_yr_end}_mdl_means_{year}.nc', chunks = 'auto')
            # mdl_means = mdl_means.expand_dims({"values": [year]}).persist()
            fut_data_pre = apply_mean_value_yr(fut_data_pre, -mdl_means, apply_yr_start + 15)
            
        fut_data_bc = apply_bc(var, fut_data_pre, dist_bc, chunked = True)
        # fut_data_bc = apply_bc(var, fut_data_pre, dist_bc, chunked = True).persist() #Alicia
        # wait(fut_data_bc) #Alicia
        
        if account_trend:
            fut_data_bc = apply_mean_value_yr(fut_data_bc, mdl_means, apply_yr_start + 15)
            # mdl_means.close()
        
        
        # fut_data_bc = adjust_attrs(fut_data_bc, var).transpose("time", "lat", "lon").chunk({'time': 27, 'lat': -1, 'lon': -1})
        fut_data_bc = adjust_attrs(fut_data_bc, var)#.chunk({'time': chunk_time, 'lat': chunk_lat, 'lon': chunk_lon}).transpose("time", "lat", "lon")#Alicia

        for item in bnds_list:
            fut_data_bc[item] = fut_data[item]
            
        fut_data_bc.to_netcdf(path + f'{var}Adjust_{grid}_{gcm}_{"historical" if year_end <= 2014 else empat}_{mdl_run}_{org}_{rcm}_v1-r1-ACS-QME-{ref}-{train_yr_start}-{train_yr_end}_day_{year_start}0101-{year_end}1231.nc')#, compute = False),encoding={var + 'Adjust': {'chunksizes': (1, fut_data_bc.lat.size, fut_data_bc.lon.size), 'least_significant_digit': 2, 'zlib': True, 'complevel': 5}}
        # tasks.append(task)                          
    # dask.compute(*tasks)
        
            
           
    ######Alicia       
        # # encoding = get_output_encoding(fut_data_bc, 'tasAdjust', fut_data_bc.lat.size, fut_data_bc.lon.size)
        # # encoding={var + 'Adjust': {'chunksizes': (12, 174, 223), 'least_significant_digit': 2, 'zlib': True, 'complevel': 2}}
        # fut_data_bc = fut_data_bc.isel(time=slice(0, 11))#.chunk({'time': 12, 'lat': 5, 'lon': 5}).persist() #Alicia
        # wait(fut_data_bc) #Alicia
        # print(fut_data_bc.chunks) #Alicia


        # fut_data_bc.to_netcdf(path + f'{var}Adjust_{grid}_{gcm}_{"historical" if year_end <= 2014 else empat}_{mdl_run}_{org}_{rcm}_v1-r1-ACS-QME-{ref}-{train_yr_start}-{train_yr_end}_1hr_{year_start}0101-{year_end}1231.nc')#, compute = False) #Alicia

        # # fut_data_bc.to_zarr(path +"output.zarr", mode="w")

        # # encoding = {f'{var}Adjust': {'chunksizes': (12, 5, 5), 'dtype': 'float32', 'zlib': True, 'complevel': 4}}
        # # write_task= fut_data_bc.to_netcdf(
        # # path + f'{var}Adjust_{grid}_{gcm}_{"historical" if year_end <= 2014 else empat}_{mdl_run}_{org}_{rcm}_v1-r1-ACS-QME-{ref}-{train_yr_start}-{train_yr_end}_1hr_{year_start}0101-{year_end}1231_time0-23.nc',
        # # encoding=encoding,
        # # compute=False)
        
        # # dask.compute(write_task)
    
   ####### Alicia     


