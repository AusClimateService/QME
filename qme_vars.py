import numpy as np
from qme_utils import qme_var

def get_qme_var(var):
    """
    Return a default set of options for a given variable name.
    This is convenient for commonly used variables and can be added to or adjusted as required.
    Example comments are included for pr. It is important to note that simply changing max_bin
    will be ineffective without also adjusting the scaling and unscaling functions to compensate.
    Additionally, scaling and unscaling should keep in mind the given lower and upper lims.
    """

    # var is already an instance of the qme_var class
    # this is here to save checking at other times
    if isinstance(var, qme_var):
        return var

    # defaults in case they are not set
    max_bin = 500
    scaling = 'auto'
    unscaling = None
    
    if var == "pr":
        
        # lower limit for the given variable - anything lower will be set to this value
        lower_lim = 0
        # upper limit for the variable - anything higher will be set to this value
        upper_lim = 1250

        # The bin count for calculating distributions (noting that 1 will be added later as the range is inclusive)
        # Increasing this increases the resolution of the bias correction at the cost of computation speed and memory
        max_bin = 500

        # Scaling and unscaling functions for calculating distribution bins. 
        # "scaling" should map values from between lower_lim and upper_lim to between 0 and max_bin inclusive
        def scaling(x):
            return np.log(x + 1) * 70
        # "unscaling" should be the reverse operation to "scaling", for obtaining the original values back
        def unscaling(x):
            return np.exp(x / 70) - 1
        # advanced note: scaling and unscaling can be done with lambdas (i.e. anonymous functions), but those may be confusing for newer Python users

    elif var == "tasmax":
        lower_lim = -30
        upper_lim = 60
        
        max_bin = 500

        def scaling(x):
            return (x + 35) * 5
        def unscaling(x):
            return (x / 5) - 35

    elif var == "tasmin":
        lower_lim = -50
        upper_lim = 40
        
        max_bin = 500

        def scaling(x):
            return (x + 55) * 5
        def unscaling(x):
            return (x / 5) - 55

    elif var == "wswd":
        lower_lim = 0
        upper_lim = 45
        
        max_bin = 500

        def scaling(x):
            return x * 10
        def unscaling(x):
            return x / 10

    elif var == "rsds":
        lower_lim = 0
        upper_lim = 45
        
        max_bin = 500

        def scaling(x):
            return x * 10
        def unscaling(x):
            return x / 10

    elif var == "rh":
        lower_lim = 0
        upper_lim = 110
        
        max_bin = 500

        def scaling(x):
            return x * 4
        def unscaling(x):
            return x / 4

    else:
        raise ValueError("Cannot find a matching variable from given input, check qme_vars.py")

    return qme_var(lower_lim, upper_lim, max_bin, scaling, unscaling)



