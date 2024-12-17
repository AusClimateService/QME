import numpy as np
from qme_utils import qme_var

def get_qme_var(var):
    """
    Return a default set of options for a given variable name.
    This is convenient for commonly used variables and can be added to or adjusted as required.
    Example comments are included for pr.
    """

    # var is already an instance of the qme_var class
    # this is here to save checking at other times
    if isinstance(var, qme_var):
        return var

    # defaults in case they are not set
    max_bin = 500
    scaling = 'linear'
    
    if var == "pr":
        # expected units: mm/day
        
        # lower limit for the given variable - anything lower will be set to this value
        lower_lim = 0
        
        # upper limit for the variable - anything higher will be set to this value
        upper_lim = 1250

        # The bin count for calculating distributions (noting that 1 will be added later as the range is inclusive)
        # Increasing this increases the resolution of the bias correction at the cost of computation speed and memory
        max_bin = 500

        # Scaling mode for calculating distribution bins
        # Values are mapped from between lower_lim and upper_lim to between 0 and max_bin
        # Mode can either be "linear" or "log" to define how values are distributed
        scaling = "log"

    elif var == "tasmax":
        # expected units: C
        lower_lim = -35
        upper_lim = 65
        max_bin = 500
        scaling = "linear"

    elif var == "tasmin":
        # expected units: C
        lower_lim = -55
        upper_lim = 45
        max_bin = 500
        scaling = "linear"

    elif var == "wswd":
        # expected units: m/s
        lower_lim = 0
        upper_lim = 50
        max_bin = 500
        scaling = "linear"

    elif var == "sfcWindmax":
        # expected units: m/s
        lower_lim = 0
        upper_lim = 50
        max_bin = 500
        scaling = "linear"

    elif var == "rsds":
        # expected units: W m-2
        lower_lim = 0
        upper_lim = 500
        max_bin = 500
        scaling = "linear"

    elif var == "rh":
        # expected units: %
        lower_lim = 0
        upper_lim = 125
        max_bin = 500
        scaling = "linear"

    elif var == "hursmax":
        # expected units: %
        lower_lim = 0
        upper_lim = 100
        max_bin = 500
        scaling = "linear"

    elif var == "hursmin":
        # expected units: %
        lower_lim = 0
        upper_lim = 100
        max_bin = 500
        scaling = "linear"

    else:
        raise ValueError("Cannot find a matching variable from given input, check qme_vars.py")

    return qme_var(lower_lim, upper_lim, max_bin, scaling)

