"""
This file contains code required for assignment 2.

We define a class that deals with power grid models and load profiles, with some functions required for assignment 2.
"""

import numpy as np
import pandas as pd
from power_grid_model import CalculationMethod, CalculationType, LoadGenType, PowerGridModel, initialize_array


class ProfileTimestampsDoesNotMatchError(Exception):
    pass


class ProfileLoadIDSDoesNotMatchError(Exception):
    pass


# add exception 1 here

# add exception 2 here


class PowerGridCalculation:
    """
    add documentation here
    """

    def __init__(
        self,
        power_grid: PowerGridModel,
        active_load_profile: List[float],
        reactive_load_profile: List[float],
    ) -> None:
        """
        add documentation
        """
