import json
import pprint
import unittest
import warnings

import numpy as np
import pandas as pd

with warnings.catch_warnings(action="ignore", category=DeprecationWarning):
    # suppress warning about pyarrow as future required dependency
    from pandas import DataFrame

import power_system_simulation.power_grid_calculation as PGC
import power_system_simulation.graph_processing as GP

from power_grid_model.utils import json_deserialize, json_serialize

from power_system_simulation.power_system_simulation import input_data_validity_check

from power_system_simulation.power_system_simulation import N1_calculation

class TestMyClass(unittest.TestCase):
    def test_ini_case1(self):
        path0 = "tests/data/small_network/input/input_network_data.json"
        path1 = "tests/data/small_network/input/meta_data.json"
        path2 = "tests/data/small_network/input/active_power_profile.parquet"
        path3 = "tests/data/small_network/input/reactive_power_profile.parquet"
        path4 = "tests/data/small_network/input/ev_active_power_profile.parquet"
        pss = input_data_validity_check(path0)
        n1 = N1_calculation(path0, path1, path2, path3)
        try:
            # call the class.function, if there is an error then record it
            #self.grid = PGC.PowerGridCalculation().construct_PGM(path0)
            #with open(path1, 'r' ) as file:
                #meta_data = json.load(file)
            #ac = pd.read_parquet("tests/data/small_network/input/ev_active_power_profile.parquet")
            #print(len(ac.columns))
            #pss.check_grid(path1)
            #pss.check_graph()
            #pss.check_matching(path2, path3, path4)
            #pss.check_EV_charging_profiles()
            print(n1.N1(18))
             
        except Exception as e:
            # if there is an error, print the information and continue to next test case
            print("ini_case1() raise custom error:", e.__class__.__name__)
            print("detail:", e)
            pass

if __name__ == "__main__":
    unittest.main()
