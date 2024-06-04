import json
import pprint
import warnings
import numpy as np
import pandas as pd 
from pathlib import Path

with warnings.catch_warnings(action="ignore", category=DeprecationWarning):
    # suppress warning about pyarrow as future required dependency
    from pandas import DataFrame

from power_grid_model import (
    PowerGridModel,
    CalculationType,
    CalculationMethod,
    initialize_array
)

from power_grid_model.validation import (
    assert_valid_input_data,
    assert_valid_batch_data
)

from power_grid_model.utils import json_deserialize, json_serialize


class TwoProfilesDoesNotHaveMatchingTimestampsOrLoadIds(Exception):
    pass

class power_grid_calculation:
    def __init__(self) -> None:
        pass

    def construct_PGM(self, data_path: str):
        with open(data_path) as fp:
            data = fp.read()
        
        self.dataset = json_deserialize(data)
        assert_valid_input_data(input_data=self.dataset, calculation_type=CalculationType.power_flow)
        print(self.dataset)
        return self.dataset
    
    def creat_batch_update_dataset(self, data_path1: str, data_path2: str):
        self.df_load_profile1 = pd.read_parquet(data_path1)
        self.df_load_profile2 = pd.read_parquet(data_path2)

        if not np.all(self.df_load_profile1.columns == self.df_load_profile2.columns):
            raise TwoProfilesDoesNotHaveMatchingTimestampsOrLoadIds
        if not np.all(self.df_load_profile1.index == self.df_load_profile2.index):
            raise TwoProfilesDoesNotHaveMatchingTimestampsOrLoadIds

        self.timestamp = self.df_load_profile1.index
        self.id = self.df_load_profile1.columns
        load_profile = initialize_array("update", "sym_load", self.df_load_profile1.shape)

        print("loadprofile", self.df_load_profile1.columns)
        print("loadprofile1", self.df_load_profile1)
        # Set the attributes for the batch calculation
        load_profile["id"] = self.df_load_profile1.columns.to_numpy()
        load_profile["p_specified"] = self.df_load_profile1.to_numpy()
        load_profile["q_specified"] = self.df_load_profile2.to_numpy()

        self.update_data = {"sym_load": load_profile}
        
        return self.update_data
    
    def time_series_power_flow_calculation(self):
        assert_valid_batch_data(input_data=self.dataset, update_data=self.update_data, calculation_type=CalculationType.power_flow)
        model = PowerGridModel(input_data=self.dataset)
        self.output_data = model.calculate_power_flow(update_data=self.update_data, calculation_method=CalculationMethod.newton_raphson)
        #print(output_data)
        return self.output_data


    def aggregate_results(self):
        #table for voltages
        table1 = pd.DataFrame()
        table1['Timestamp'] = self.timestamp
        table1.set_index('Timestamp')
        table1['max_id'] = 0
        table1['max_pu'] = 0.0
        table1['min_id'] = 0
        table1['min_pu'] = 0.0
        i = 0
        for node_scenario in self.output_data["node"]:
            df = pd.DataFrame(node_scenario)
            max = df["u_pu"].idxmax()
            min = df["u_pu"].idxmin()
            max_value_id = df.at[max, 'id']
            min_value_id = df.at[min, 'id']
            max_value_pu = df.at[max, 'u_pu']
            min_value_pu = df.at[min, 'u_pu']

            table1.loc[i, 'max_id'] = max_value_id
            table1.loc[i, 'max_pu'] = max_value_pu
            table1.loc[i, 'min_id'] = min_value_id
            table1.loc[i, 'min_pu'] = min_value_pu
            i = i + 1

        print(table1)

        #table for line
        max_loading = self.df_load_profile1.max()
        max_loading_timestamp = self.df_load_profile1.idxmax()
        min_loading = self.df_load_profile1.min()
        min_loading_timestamp = self.df_load_profile1.idxmin()

        table2 = pd.DataFrame({"Line_ID": self.id, "Maximum Loading": max_loading, "Timestamp of Maximum Loading": max_loading_timestamp, "Minimum Loading": min_loading, "Timestamp of Minimum Loading": min_loading_timestamp})
        print(table2)

    