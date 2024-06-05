import json
import pprint
import warnings
import numpy as np
import pandas as pd 
from pathlib import Path
from datetime import datetime
from scipy import integrate

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

        self.timestamp = df_load_profile1.index

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
        #table for voltages
        table1 = pd.DataFrame()
        table1['Timestamp'] = self.timestamp
        table1.set_index('Timestamp')
        table1['max_id'] = 0
        table1['max_pu'] = 0.0
        table1['min_id'] = 0
        table1['min_pu'] = 0.0
        i = 0
        for node_scenario in output_data["node"]:
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


        
        table2 = pd.DataFrame()
        table2['Line_ID'] = output_data['line']['id'][0]
        table2.set_index('Line_ID')
        table2['max_time'] = datetime.fromtimestamp(0)
        table2['max__loading_pu'] = 0.0
        table2['min_time'] = datetime.fromtimestamp(0)
        table2['min_loading_pu'] = 0.0
        table2['energy_loss_kw'] = 0.0

        
        df_temp = pd.DataFrame()
        df_temp = pd.DataFrame(output_data['line']['loading'])
        df_temp['Timestamp'] = self.timestamp
        i=0
        for column_name, column_data in df_temp.iloc[:, :-1].items():
            table2.loc[i, 'max__loading_pu'] = column_data.max()
            table2.loc[i, 'max_time'] = df_temp['Timestamp'][column_data.idxmax()]
            table2.loc[i, 'min_loading_pu'] = column_data.min()
            table2.loc[i, 'min_time'] = df_temp['Timestamp'][column_data.idxmin()]
            i = i+1


        
        p_loss = pd.DataFrame()
        p_loss = abs(abs(pd.DataFrame(output_data['line']['p_from'])) - abs(pd.DataFrame(output_data['line']['p_to'])))
        i = 0
        for column_name, column_data in p_loss.items():
            table2.loc[i, 'energy_loss_kw'] = integrate.trapezoid(column_data.to_list()) / 1000
            i = i+1
        
        
        print(table1)
        print(table2)



        

        

        



