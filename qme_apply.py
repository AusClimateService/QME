import numpy as np
import xarray as xr
from qme_utils import *
from qme_vars import *


def apply_mean_values(data, mean_values, start_year = 0):
    """
    Applies the generated mean values to the corresponding years of the given data set.
    Inputs:
    data - the data to apply the mean values to
    mean_values - the mean values generated by qme_train.find_means. Pass the argument with a leading minus sign to subtract the values instead
    start_year - the year (relative to the domain, with 0 being the first) at which to start applying the mean values. Every year before is unchanged
    """
    year_values = data.time.values.astype('datetime64[Y]').astype(int)
    min_year = year_values.min()
    year_values = year_values - min_year
    
    def apply_temp(data_loc, mean_loc):
        adjusted = data_loc.copy()
        for i in range(len(data_loc)):
            year = int(year_values[i])
            if year >= start_year:  
                adjusted[i] += mean_loc[year]
        return adjusted
        
    return xr.apply_ufunc(apply_temp, data, mean_values, input_core_dims = [["time"], ["values"]], 
                          output_core_dims = [["time"]], vectorize = True, keep_attrs = True,
                          output_dtypes = [np.float32], dask = 'parallelized')


def apply_bc(var, mdl, bc):
    """
    Applies the bias correction factors to the model data.
    Inputs:
    mdl - the model data
    bc - the bias correction factors
    var - the variable being corrected
    """
    var = get_qme_var(var)
    reso = var.bin_count()
    month_values = mdl.time.values.astype('datetime64[M]').astype(int) % 12
    def apply(mdl_loc, bc_loc):
        adjusted = var.scale_data(var.limit_data(mdl_loc))

        # special rounding function used to correct Numpy rounding towards evens - see comments in qme_utils
        rounded = round_half_up(adjusted)

        # original version
        # rounded = np.round(adjusted).astype(int)
        
        for i, value in enumerate(rounded):
            # check for out of bounds in case of funky numbers when dealing with NaNs
            if value >= 0 and value < reso:
                adjusted[i] += bc_loc[month_values[i]][value]
        adjusted = var.unscale_data(adjusted)
        return adjusted
        
    return xr.apply_ufunc(apply, mdl, bc, input_core_dims = [["time"], ["month", "values"]], 
                          output_core_dims = [["time"]], vectorize = True, keep_attrs = True, 
                          output_dtypes = [np.float32], dask = 'parallelized')

