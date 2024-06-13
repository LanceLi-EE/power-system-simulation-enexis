import json
import pprint
import unittest
import warnings

import numpy as np
import pandas as pd

with warnings.catch_warnings(action="ignore", category=DeprecationWarning):
    # suppress warning about pyarrow as future required dependency
    from pandas import DataFrame

from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array
from power_grid_model.utils import json_deserialize, json_serialize
from power_grid_model.validation import assert_valid_batch_data, assert_valid_input_data

from power_system_simulation.power_grid_calculation import PowerGridCalculation


class TestMyClass(unittest.TestCase):
    def test_ini_case1(self):
        pgc = PowerGridCalculation()

        path0 = "tests/data/big_network/input/input_network_data.json"
        path1 = "tests/data/big_network/input/active_power_profile.parquet"
        path2 = "tests/data/big_network/input/reactive_power_profile.parquet"
        try:
            # call the class.function, if there is an error then record it
            PGM = pgc.construct_pgm(path0)
            update_dataset = pgc.creat_batch_update_dataset(path1, path2)
            tables = pgc.time_series_power_flow_calculation()
            print(tables[0])
            print(tables[1])

        except Exception as e:
            # if there is an error, print the information and continue to next test case
            print("ini_case1() raise custom error:", e.__class__.__name__)
            print("detail:", e)


if __name__ == "__main__":
    unittest.main()
