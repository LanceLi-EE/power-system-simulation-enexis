import json
import pprint
import warnings
import numpy as np
import pandas as pd

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
        return self.dataset
    
    def creat_batch_update_dataset(self, data_path1: str, data_path2: str):
        df_load_profile1 = pd.read_parquet(data_path1)
        df_load_profile2 = pd.read_parquet(data_path2)

        if not np.all(df_load_profile1.columns == df_load_profile2.columns):
            raise TwoProfilesDoesNotHaveMatchingTimestampsOrLoadIds
        if not np.all(df_load_profile1.index == df_load_profile2.index):
            raise TwoProfilesDoesNotHaveMatchingTimestampsOrLoadIds


        load_profile = initialize_array("update", "sym_load", df_load_profile1.shape)

        # Set the attributes for the batch calculation
        load_profile["id"] = df_load_profile1.columns.to_numpy()
        load_profile["p_specified"] = df_load_profile1.to_numpy()
        load_profile["q_specified"] = df_load_profile2.to_numpy()

        self.update_data = {"sym_load": load_profile}

        return self.update_data
    
    def time_series_power_flow_calculation(self):
        assert_valid_batch_data(input_data=self.dataset, update_data=self.update_data, calculation_type=CalculationType.power_flow)
        model = PowerGridModel(input_data=self.dataset)
        output_data = model.calculate_power_flow(update_data=self.update_data, calculation_method=CalculationMethod.newton_raphson)
        timestamps = self.update_data["sym_load"]["p_specified"].index
        
        #table for voltages
        voltage_data = []
        for timestamp in timestamps:
            voltage = output_data["voltage"][timestamp]
            max_voltage = np.max(voltage)
            max_voltage_node = np.argmax(voltage)
            min_voltage = np.min(voltage)
            min_voltage_node = np.argmin(voltage)
            voltage_data.append([timestamp, max_voltage, max_voltage_node, min_voltage, min_voltage_node])

        voltage_df = pd.DataFrame(voltage_data, columns=["Timestamp", "Maximum p.u voltage", "Node ID with Maximum p.u voltage", "Minimum p.u voltage", "Node ID with Minimum p.u voltage"])
    



