import numpy as np
import xarray as xr


DIST_RESO = 501


def scale_data(data, var_name):
    """
    Scale the given 'data' for better representation of extremes in the histograms for quantile matching.
    This scaling can be varied depending on the input variable 'var_name'.
    The scaled data need to range from 0 to reso for the histograms used in quantile matching.
    The limit_data procedure below also ensures data stay within this range from 0 to reso.
    Make sure to add the reverse operation in unscale_data below!
    Inputs:
    data - the data being scaled, which can be anything that mathematical operations can be performed directly on (such as numbers, NumPy arrays, DataArrays etc.)
    var_name - the variable to specify how to scale the data
    """
    
    #***** User inputs here *****
    # If wanting additional variables, users can add to the code below.
    if var_name == 'tasmax':
        data = (data + 35) * 5
    elif var_name == 'tasmin':
        data = (data + 55) * 5
    elif var_name == 'pr':
        data = np.log(data + 1) * 70
    elif var_name == 'wswd':
        data = data * 10
    elif var_name == 'rsds':
        data = data * 10
    elif var_name == 'rh':
        data = data * 4
    else:
        raise ValueError("Unexpected variable name " + var_name) # probably unnecessary when finalized 

    return data


def unscale_data(data, var_name):
    """
    Revert from scale_dat procedure, as above.
    Inputs:
    data - the data being unscaled, which can be anything that mathematical operations can be performed directly on (such as numbers, NumPy arrays, DataArrays etc.)
    var_name - the variable to specify how to unscale the data
    """
    
    #***** User inputs here *****
    # If wanting additional variables, users can add to the code below.
    if var_name == 'tasmax': 
        data = data/5 - 35
    elif var_name == 'tasmin': 
        data = data/5 - 55
    elif var_name == 'pr': 
        data = np.exp(data/70) - 1
    elif var_name == 'wswd':
        data = data / 10
    elif var_name == 'rsds':
        data = data / 10
    elif var_name == 'rh':
        data = data / 4
    else:
        raise ValueError("Unexpected variable name " + var_name) # probably unnecessary when finalized 
        
    return data 


def limit_data(data, var_name):
    """
    Apply upper and lower limits to data using np.clip, with the limits depending on the variable and designed to correspond with scale_data.
    After scale_data(limit_data(data)), all results should be between 0 and 500 inclusive.
    Inputs:
    data - the data to apply limits to
    var_name - the variable to specify how to limit the data
    """

    #***** User inputs here *****
    # If wanting additional variables, users can add to the code below.
    if var_name == 'tasmax':
        lim_upper = 60 # Upper limit of 60 degrees C.
        lim_lower = -30 # Lower limit of -30 degrees C.
    elif var_name == 'tasmin':
        lim_upper = 40 # Upper limit of 40 degrees C.
        lim_lower = -50 # Lower limit of -50 degrees C.
    elif var_name == 'pr':
        lim_upper = 1250 # Upper limit of 1250 mm.
        lim_lower = 0 # Lower limit of 0 mm.
    elif var_name == 'wswd':
        lim_upper = 45 # Upper limit of 40 m/s.
        lim_lower = 0 # Lower limit 0 m/s.
    elif var_name == 'rsds':
        lim_upper = 45 # Upper limit 40 MJ/m/m.
        lim_lower = 0 # Lower limit 0. MJ/m/m.
    elif var_name == 'rh':
        lim_upper = 110 # Upper limit 110%.
        lim_lower = 0 # Lower limit 0%.
    else:
        raise ValueError("Unexpected variable name " + var_name) # probably unnecessary when finalized

    return np.clip(data, lim_lower, lim_upper)


def three_mnth_sum(data, dim = "month"):
    """
    Procedure to make 3-month moving sum, with rolling around the edges.
    Inputs: 
    data - a DataArray with the specified dimension of size 12 (representing months)
    dim (optional) - specify dimension name in case it is not 'month'
    """

    if not(data[dim].size == 12): # or (data[dim].size == 13)): 
        raise ValueError(f'Month dimension must be of size 12, given array had size {data.time.size}') # Check this is 12, ~or 13 for all year also included~

    # calculate rolling sum
    summed_dat = data.rolling({dim: 3}, center = True).sum()

    # manually calculate Jan + Dec since rolling does not work on edges
    summed_dat[{dim: 0}] = data[{dim: 0}] + data[{dim: 1}] + data[{dim: 11}]
    summed_dat[{dim: 11}] = data[{dim: 0}] + data[{dim: 10}] + data[{dim: 11}]

    # # restore original 13th column if there was one
    # if data.time.size == 13:
    #     summed_dat[{"time": 12}] = data[{"time": 12}]
        
    return summed_dat
    

# written to replicate IDL smoothing
def smooth(data, width):
    """
    Smoothes data using a moving box of a given width. Near edges, the border value is repeated to fill the rest of the box
    Inputs: 
    data - the data being smoothed, assumed to be a one-dimensional array. If not, the smoothing will occur across the first dimension
    width - the width of the moving box. If even, it is converted into an odd number by adding 1
    Returns:
    smoothed - the original data with smoothing applied
    """
    # IDL will add 1 to any even number given as an argument
    if width % 2 == 0:
        width += 1

    # the IDL function was called with "/EDGE_TRUNCATE", meaning that along the edges out of bound values were just filled with the edge value
    # instead of being replaced by NaNs
    side = width // 2
    box = side * data[0] + sum(data[:side + 1])

    smoothed = data.copy()
    
    for i in range(data.size):
        smoothed[i] = box/width

        # modify the box by removing the left-most value in the box and adding the next one to the right
        # the checks are for the edge cases to preserve "/EDGE_TRUNCATE" functionality
        box -= data[max(i - side, 0)]
        box += data[min(i + side + 1, data.size - 1)]
        
    return smoothed

