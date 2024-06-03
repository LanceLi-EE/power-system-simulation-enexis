"""
Module for performing power grid calculations using power flow models.

Classes:
    PowerGridCalculation: Main class to handle power grid calculations.

Exceptions:
    TwoProfilesDoesNotHaveMatchingTimestampsOrLoadIds: Custom exception for mismatched load profiles.
"""

import pprint
import warnings
import numpy as np
import pandas as pd
from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array
from power_grid_model.utils import json_deserialize
from power_grid_model.validation import assert_valid_batch_data, assert_valid_input_data

with warnings.catch_warnings(action="ignore", category=DeprecationWarning):
    # Suppress warning about pyarrow as a future required dependency
    pass


class TwoProfilesDoesNotHaveMatchingTimestampsOrLoadIds(Exception):
    """Exception raised when two load profiles do not have matching timestamps or load IDs."""


class PowerGridCalculation:
    """Class to perform power grid calculations using power flow models."""

    def __init__(self) -> None:
        """Initialize the PowerGridCalculation class."""
        self.dataset = None
        self.update_data = None
        self.output_data = None

    def construct_pgm(self, data_path: str):
        """
        Construct the Power Grid Model (PGM) from the provided JSON data.

        Args:
            data_path (str): Path to the JSON data file.

        Returns:
            dict: Deserialized dataset.
        """
        with open(data_path, encoding='utf-8') as fp:
            data = fp.read()

        self.dataset = json_deserialize(data)
        assert_valid_input_data(input_data=self.dataset, calculation_type=CalculationType.power_flow)
        return self.dataset

    def create_batch_update_dataset(self, data_path1: str, data_path2: str):
        """
        Create a batch update dataset from two load profiles.

        Args:
            data_path1 (str): Path to the first load profile parquet file.
            data_path2 (str): Path to the second load profile parquet file.

        Returns:
            dict: Update dataset.
        """
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
        """
        Perform the time series power flow calculation.

        Returns:
            dict: Output data from the power flow calculation.
        """
        assert_valid_batch_data(
            input_data=self.dataset, update_data=self.update_data, calculation_type=CalculationType.power_flow
        )
        model = PowerGridModel(input_data=self.dataset)
        self.output_data = model.calculate_power_flow(
            update_data=self.update_data, calculation_method=CalculationMethod.newton_raphson
        )
        pprint.pprint(self.output_data)
        return self.output_data

    def aggregate_results(self):
        """
        Aggregate the power flow calculation results.

        Returns:
            DataFrame: Aggregated results with voltage statistics.
        """
        timestamps = self.update_data["sym_load"]["p_specified"].index

        # Table for voltages
        voltage_data = []
        for timestamp in timestamps:
            voltage = self.output_data["voltage"][timestamp]
            max_voltage = np.max(voltage)
            max_voltage_node = np.argmax(voltage)
            min_voltage = np.min(voltage)
            min_voltage_node = np.argmin(voltage)
            voltage_data.append([timestamp, max_voltage, max_voltage_node, min_voltage, min_voltage_node])

        voltage_df = pd.DataFrame(
            voltage_data,
            columns=[
                "Timestamp",
                "Maximum p.u voltage",
                "Node ID with Maximum p.u voltage",
                "Minimum p.u voltage",
                "Node ID with Minimum p.u voltage",
            ],
        )
        return voltage_df
