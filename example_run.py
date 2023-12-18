import xarray as xr

from qme_train import *
from qme_apply import *

# Set as appropriate
outdir = ""


def standardise_latlon(ds, digits=4):
    """
    This function rounds the latitude / longitude coordinates to the 4th digit, because some dataset
    seem to have strange digits (e.g. 50.00000001 instead of 50.0), which prevents merging of data.
    """
    ds = ds.assign_coords({"lat": np.round(ds.lat, digits)})
    ds = ds.assign_coords({"lon": np.round(ds.lon, digits)})
    return(ds)


if __name__ == "__main__":

    # Set up Dask cluster from the scheduler
    from dask.distributed import Client, LocalCluster
    import os
    client = Client(scheduler_file = os.environ["DASK_PBS_SCHEDULER"])

    # Upload the three files containing the QME functions so that the workers can access them
    client.upload_file("qme_utils.py")
    client.upload_file("qme_train.py")
    client.upload_file("qme_apply.py")


    ######################################
    # START OF OPTIONS - Lines in this section can be modified for usual operation of the code
    ######################################
    
    # List of models to run on
    mdls = []
    mdls.append('BOM-BARPA-R_ERA5')
    # mdls.append('UQ-CCAM_ERA5')
    # mdls.append('CSIRO-CCAM_ERA5')
    # mdls.append('BOM-BARPA-R_ACCESS-ESM1-5')

    # List of vars to run on
    vars = []
    # vars.append("tasmax")
    # vars.append("tasmin")
    vars.append("pr")
    # vars.append("wswd")
    # vars.append("rsds")
    # vars.append("rh")

    empat = 'ssp370' # If using climate projections can select which emission pathway (such as 'rcp85', 'historical' for reanalysis, 'ssp3-7.0', etc.)

    for mdl in mdls:
        
        # Select which time periods to use for a particular application (inclusive range)
        if mdl in ('BOM-BARPA-R_ERA5', 'UQ-CCAM_ERA5', 'CSIRO-CCAM_ERA5'):
            st_year_train = 1980
            en_year_train = 1999

            # Also can include the training period when applying
            st_year_apply = 1980 
            en_year_apply = 2019

            # No future data for these models
            st_year_fut = 0
            en_year_fut = 0

        if mdl == 'BOM-BARPA-R_ACCESS-ESM1-5':
            st_year_train = 1980
            en_year_train = 2019

            # Also can include the training period when applying
            st_year_apply = 1980 
            en_year_apply = 2019

            # Apply bias correction to future data for this model
            st_year_fut = 2080
            en_year_fut = 2099
    
        for var in vars:
            
            # Parameters for training
            params = {
                "xtr": 3,
                "cal_smth": 21,
                "mthd": '_quick',
                "mn_smth": '_3mn' if var == "pr" else '',
                "ssze_lim": 50,
                "mltp": False,
                "lmt": 1.5 if var == "pr" else -1,
                "lmt_thresh": 10
            }
            
            # Whether to account for trend with moving average
            if var in ("tasmax", "tasmin"):
                account_trend = True
            else:
                account_trend = False

            ######################################
            # END OF OPTIONS
            ######################################

        
            # Empty lists to store file paths (data is stored by year but will be loaded simultaneously)
            mdl_files = []
            obs_files = []
            fut_files = []

            # Compile observational data file paths
            for year in range(st_year_train, en_year_train + 1):
                year_string = str(year)
                obs_files.append('/g/data/ia39/npcp/data/'
                                 + var + '/observations/AGCD/raw/task-reference/' 
                                 + var + '_NPCP-20i_AGCD_v1-0-1_day_' 
                                 + year_string + '0101-' + year_string + '1231.nc')

            # Compile model data file paths
            for year in range(st_year_train, en_year_apply + 1):
                year_string = str(year)
                if mdl == 'BOM-BARPA-R_ERA5':
                    mdl_files.append('/g/data/ia39/npcp/data/' 
                                     + var + '/ECMWF-ERA5/BOM-BARPA-R/raw/task-reference/' 
                                     + var + '_NPCP-20i_ECMWF-ERA5_evaluation_r1i1p1f1_BOM-BARPA-R_v1_day_' 
                                     + year_string + '0101-' + year_string + '1231.nc')
                elif mdl == 'UQ-CCAM_ERA5':
                    mdl_files.append('/g/data/ia39/npcp/data/' 
                                     + var + '/ECMWF-ERA5/UQ-DES-CCAM-2105/raw/task-reference/' 
                                     + var + '_NPCP-20i_ECMWF-ERA5_evaluation_r1i1p1f1_UQ-DES-CCAM-2105_v1_day_' 
                                     + year_string + '0101-' + year_string + '1231.nc')
                elif mdl == 'CSIRO-CCAM_ERA5':
                    mdl_files.append('/g/data/ia39/npcp/data/' 
                                     + var + '/ECMWF-ERA5/CSIRO-CCAM-2203/raw/task-reference/' 
                                     + var + '_NPCP-20i_ECMWF-ERA5_evaluation_r1i1p1f1_CSIRO-CCAM-2203_v1_day_' 
                                     + year_string + '0101-' + year_string + '1231.nc')
                elif mdl == 'BOM-BARPA-R_ACCESS-ESM1-5':
                    empat_yr = 'historical' if year <= 2014 else empat
                    mdl_files.append('/g/data/ia39/npcp/data/' 
                                     + var + '/CSIRO-ACCESS-ESM1-5/BOM-BARPA-R/raw/task-reference/' 
                                     + var + f'_NPCP-20i_CSIRO-ACCESS-ESM1-5_{empat_yr}_r6i1p1f1_BOM-BARPA-R_v1_day_' 
                                     + year_string + '0101-' + year_string + '1231.nc')

            # Compile future model data file paths
            if st_year_fut and en_year_fut:
                for year in range(st_year_fut, en_year_fut + 1):
                    year_string = str(year)
                    fut_files.append('/g/data/ia39/npcp/data/' 
                                     + var + '/CSIRO-ACCESS-ESM1-5/BOM-BARPA-R/raw/task-reference/' 
                                     + var + f'_NPCP-20i_CSIRO-ACCESS-ESM1-5_{empat}_r6i1p1f1_BOM-BARPA-R_v1_day_' 
                                     + year_string + '0101-' + year_string + '1231.nc') 
    
            # Load all data and chunk (chunk sizes for lat and lon may be adjusted here)
            lat_chunk_size = 25
            lon_chunk_size = 25
            
            mdl_data = xr.open_mfdataset(mdl_files, preprocess = standardise_latlon)[var].chunk(time = -1, lat = lat_chunk_size, lon = lon_chunk_size)
            obs_data = xr.open_mfdataset(obs_files, preprocess = standardise_latlon)[var].chunk(time = -1, lat = lat_chunk_size, lon = lon_chunk_size)
            if len(fut_files) > 0:
                fut = True
                fut_data = xr.open_mfdataset(fut_files, preprocess = standardise_latlon)[var].chunk(time = -1, lat = lat_chunk_size, lon = lon_chunk_size)
            else:
                fut = False

            # Select training data years
            mdl_training = mdl_data.sel(time = mdl_data.time.dt.year.isin(range(st_year_train, en_year_train + 1))).chunk({"time": -1})
            obs_training = obs_data.sel(time = obs_data.time.dt.year.isin(range(st_year_train, en_year_train + 1))).chunk({"time": -1})
        
            # Select model data years for application
            mdl_apply = mdl_data.sel(time = mdl_data.time.dt.year.isin(range(st_year_apply, en_year_apply + 1))).chunk({"time": -1})

            # Create distributions histograms
            dist_mdl = make_dist(var, mdl_training).chunk({"values": -1, "month": -1})
            dist_obs = make_dist(var, obs_training).chunk({"values": -1, "month": -1})

            # Apply QME method to distributions to calculate bias correction factors
            dist_bc = calc_qme(var, dist_mdl, dist_obs, **params).chunk({"values": -1, "month": -1}).persist()

            # Save the bias correction data (and q_check)
            dist_bc.biascorr.to_netcdf(outdir + f'dist_bc_{var}_{mdl}_{empat}.nc')
            dist_bc.q_check.to_netcdf(outdir + f'dist_bc_{var}_{mdl}_{empat}_q_check.nc')

            mdl_mean = None

            # Apply bias correction to model data
            if account_trend and not fut:
                # Find model means
                mdl_mean = find_means(mdl_data).persist()

                # Save model means
                mdl_mean.to_netcdf(outdir + f'mean_model_{var}_{mdl}_{empat}')
                
                mdl_bc = apply_mean_values(mdl_apply, -mdl_mean, 14)
                mdl_bc = apply_bc(var, mdl_bc, dist_bc.biascorr)
                mdl_bc = apply_mean_values(mdl_bc, mdl_mean, 14).rename(var)
                
            else:
                # Apply the bias correction to model data without means
                mdl_bc = apply_bc(var, mdl_apply, dist_bc.biascorr).rename(var)

            # Save bias corrected model data
            mdl_bc.to_netcdf(outdir + f'{var}_{mdl}_{empat}.nc')

            if fut:
                if account_trend:
                    # Find model means (note that this is calculated slightly differently for future data)
                    mdl_mean = future_means(mdl_data, fut_data).persist()

                    # Save future model means
                    mdl_mean.to_netcdf(outdir + f'mean_model_{var}_{mdl}_{empat}')
                    
                    fut_bc = apply_mean_values(fut_data, -mdl_mean)
                    fut_bc = apply_bc(var, fut_bc, dist_bc.biascorr)
                    fut_bc = apply_mean_values(fut_bc, mdl_mean).rename(var)
                    
                else:
                    # Apply the bias correction to future data without means
                    fut_bc = apply_bc(var, fut_data, dist_bc.biascorr).rename(var)

                # Save bias corrected future data
                fut_bc.to_netcdf(outdir + f'{var}_{mdl}_{empat}_fut.nc')

            # Clean up for next round (just these in particular as they were called with "persist" to keep it in memory for the various operations it was required for)
            del dist_bc
            if mdl_mean is not None:
                del mdl_mean

