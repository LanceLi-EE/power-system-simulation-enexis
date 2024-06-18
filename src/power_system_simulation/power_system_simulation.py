"""
Assignment 3 for the advanced grid analysis
"""

import json
import math
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

with warnings.catch_warnings(action="ignore", category=DeprecationWarning):
    # suppress warning about pyarrow as future required dependency
    from pandas import DataFrame

from power_grid_model import CalculationMethod, PowerGridModel, initialize_array
from scipy import integrate

from power_system_simulation.graph_processing import GraphProcessor
from power_system_simulation.power_grid_calculation import PowerGridCalculation


# Input data validity check
class MoreThanOneTransformerOrSource(Exception):
    """
    The LV grid should have exactly one transformer, and one source.
    """


class InvalidLVFeederID(Exception):
    """
    All IDs in the LV Feeder IDs should be valid line IDs.
    """


class MismatchFromAndToNodes(Exception):
    """
    All the lines in the LV Feeder IDs should have the from_node the same as the to_node of the transformer.
    """


class MismatchedTimetamps(Exception):
    """
    The timestamps should be matching between the active load profile, reactive load profile, and EV charging profile.
    """


class MismatchedIDs(Exception):
    """
    The IDs in active load profile and reactive load profile should be matching.
    """


class InvalidIDs(Exception):
    """
    The IDs in active load profile and reactive load profile should be valid IDs of sym_load.
    """


class NotEnoughEVChargingProfiles(Exception):
    """
    The number of EV charging profile should be at least the same as the number of sym_load.
    """


class OptimalTapPositionCriteriaError(Exception):
    """
    The criteria for the tap position is not valid.
    """


class input_data_validity_check:
    """
    The class used to validate all the input data
    """

    # check valid PGM input data and if has cycles and if fully connected
    def __init__(self, network_data: str):
        """
        Read the network input data and construct a power grid model using PowerGridCalculation class.

        Args:
        network_data (str): Path to the network data JSON file.

        Returns:
        None

        Raises:
        None
        """
        self.pgc = PowerGridCalculation()
        self.grid = self.pgc.construct_pgm(network_data)

    # check LV grid has exactly one transformer and one source
    def check_grid(self, meta_data: str):
        """
        Check if the original data of the grid itself is correct:
        1.  The LV grid should have exactly one transformer and one source.
            Raise an error otherwise.
        2.  Check whether lv_feeders are a subset of the set of line IDs. If not,
            raise an error.
        3.  Check for every lv_feeder if the from_node is the same as the to_node of the transformer. If not,
            raise an error.

        Args:
        meta_data (str): Path to the meta data JSON file.

        Returns:
        None

        Raises:
        MoreThanOneTransformerOrSource, InvalidLVFeederID, MismatchFromAndToNodes
        """
        # read lv feeder info
        with open(meta_data, "r") as file:
            self.meta = json.load(file)
        # check transformer and source
        if not (len(self.grid["transformer"]) == 1 and len(self.grid["source"]) == 1):
            raise MoreThanOneTransformerOrSource
        # check lv feeder info
        if not (set(self.meta["lv_feeders"]).issubset(set(self.grid["line"]["id"]))):
            raise InvalidLVFeederID
        # check all the lines in the LV Feeder IDs have the from_nodes the same as the to_node of the transformer
        from_nodes = []
        for line in self.grid["line"]:
            if line["id"] in self.meta["lv_feeders"]:
                from_nodes.append(line["from_node"])
        if not (all(from_node == self.grid["transformer"][0]["to_node"] for from_node in from_nodes)):
            raise MismatchFromAndToNodes

    def check_graph(self):
        """
        Define input arguments for the GraphProcessor class and create a graph:
        1.  Define the input arguments using the grid and meta data.
        2.  Create a graph using the GraphProcessor class.
        """
        vertex_ids = self.grid["node"]["id"].tolist()
        edge_vertex_id_pairs = list(zip(self.grid["line"]["from_node"], self.grid["line"]["to_node"]))
        edge_vertex_id_pairs.append((self.grid["transformer"]["from_node"][0], self.grid["transformer"]["to_node"][0]))
        edge_ids = self.grid["line"]["id"].tolist()
        edge_ids.append(self.grid["transformer"]["id"][0])
        edge_enabled = np.logical_and(self.grid["line"]["from_status"], self.grid["line"]["to_status"]).tolist()
        edge_enabled.append(1)
        source_vertex_id = self.meta["mv_source_node"]
        self.gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    # check if the timestamps and id are matching between the acitve load profile, reactive load profile and EV charging profile and if sym_load id matches
    def check_matching(self, active_load_profile: str, reactive_load_profile: str, ev_active_power_profile: str):
        """
        Check if the data used to update the model is correct:
        1.  Read the parquet files containing the active load profile, reactive load profile and EV charging profile.
        2.  Check if the timestamps are matching between the active load profile, reactive load profile, and EV charging profile.
            Raise an error otherwise.
        3.  Check if the IDs are matching between the active load profile and reactive load profile.
            Raise an error otherwise.
        4.  Check if the IDs of the symmetric load profile IDs match up with the active load profile IDs.
            Raise an error otherwise.

        Args:
        active_load_profile (str), reactive_load_profile (str), ev_active_power_profile (str): path to the active and reactive profile parquet files.
        ev_active_power_profile (str): path to the EV active power profile parquet file.

        Returns:
        None

        Raises:
        MissingTimetamps, MismatchedIDs, InvalidIDs
        """
        self.df1 = pd.read_parquet(active_load_profile)
        self.df2 = pd.read_parquet(reactive_load_profile)
        self.df3 = pd.read_parquet(ev_active_power_profile)
        if not np.all(self.df1.index == self.df2.index):
            raise MismatchedTimetamps
        if not np.all(self.df2.index == self.df3.index):
            raise MismatchedTimetamps
        if not np.all(self.df1.columns == self.df2.columns):
            raise MismatchedIDs
        if not (set(self.grid["sym_load"]["id"]) == set(self.df1.columns)):
            raise InvalidIDs

    # check the number of EV charging profiles is at least the number of sym_load
    def check_ev_charging_profiles(self):
        """
        Check whether there are at least enough EV charging profiles for all the symmetric loads.
        """
        if len(self.df3.columns) < len(self.grid["sym_load"]):
            raise NotEnoughEVChargingProfiles


class ev_penetration_level:
    """
    The class used to randomly assign ev-penetration level to the symmetric loads on the grid.
    """

    def __init__(
        self,
        network_data: str,
        active_load_profile: str,
        reactive_load_profile: str,
        ev_active_power_profile: str,
        meta_data: str,
    ):
        """
        Read from input data of the grid and parquet files, then create the graph:
        1.  Define PowerGridCalculation class.
        2.  Construct the power grid model using the network data JSON file.
        3.  Create batch update dataset using the active and reactive load profile parquet files.
        4.  Create ev active power profiles using the EV active power profile parquet file.
        5.  Define the input arguments for GraphProcessor using the grid and meta data.
        6.  Create a graph using the GraphProcessor class.

        Args:
        network_data (str): Path to the network data JSON file,
        active_load_profile (str), reactive_load_profile (str): Path to the active and reactive load profile parquet files,
        ev_active_power_profile (str): Path to the EV active power profile parquet file,
        meta_data (str): Path to the meta data JSON file.

        Returns:
        None

        Raises:
        None
        """
        self.pgc = PowerGridCalculation()
        self.grid = self.pgc.construct_pgm(network_data)
        self.update_data = self.pgc.creat_batch_update_dataset(active_load_profile, reactive_load_profile)
        self.ev = pd.read_parquet(ev_active_power_profile)
        with open(meta_data, "r") as file:
            self.meta = json.load(file)

        vertex_ids = self.grid["node"]["id"].tolist()
        edge_vertex_id_pairs = list(zip(self.grid["line"]["from_node"], self.grid["line"]["to_node"]))
        edge_vertex_id_pairs.append((self.grid["transformer"]["from_node"][0], self.grid["transformer"]["to_node"][0]))
        edge_ids = self.grid["line"]["id"].tolist()
        edge_ids.append(self.grid["transformer"]["id"][0])
        edge_enabled = np.logical_and(self.grid["line"]["from_status"], self.grid["line"]["to_status"]).tolist()
        edge_enabled.append(1)
        source_vertex_id = self.meta["mv_source_node"]
        self.gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def calculate(self, p_level: float):
        """
        Function that assigns the EV charging profiles to the symmetric loads on the grid, and performs power flow calculation:
        1.  Determine the total number of houses and feeders on the grid, and calculate the number of EVs that should be assigned to each feeder.
        2.  Shuffle the EV charging profiles so that they can be randomly assigned to the symmetric loads.
        3.  For each feeder, make a list of its downstream nodes using the find_downstream_vertices method.
        4.  To find the symmetric loads in the feeder, iterate through the symmetric loads and check if their node is in the list of downstream nodes.
        5.  For each feeder, if:
            - evs_per_feeder is greater than or equal to the number of sym_loads in a feeder, assign an EV charging profile to each load.
            - If evs_per_feeder is less than the number of sym_loads in the feeder, randomly select and assign an EV charging profile to each load.
        6.  Perform and return a time series power flow calculation using the updated data.

        Args:
        p_level (float): EV penetration level.

        Returns:
        time_series_power_flow_calculation: Time series power flow calculation results.

        Raises:
        None
        """
        np.random.seed(0)
        total_houses = len(self.grid["sym_load"]["id"])
        number_of_feeders = len(self.meta["lv_feeders"])
        evs_per_feeder = math.floor(p_level * total_houses / number_of_feeders)
        ramdon_range = self.ev.shape[1]
        ev_seq = np.arange(ramdon_range)
        np.random.shuffle(ev_seq)
        for feeder in self.meta["lv_feeders"]:
            down_stream_node = self.gp.find_downstream_vertices(feeder)
            list_load = []
            for load in self.grid["sym_load"]:
                if load["node"] in down_stream_node:
                    list_load.append(load["id"])
            if evs_per_feeder >= len(list_load):
                update_seq = []
                for load in list_load:
                    ids = self.update_data["sym_load"]["id"][0]
                    update_seq.append(
                        pd.DataFrame(self.update_data["sym_load"]["p_specified"], columns=ids).columns.get_loc(load)
                    )
                for seq in update_seq:
                    self.update_data["sym_load"]["p_specified"][:, seq] += self.ev.iloc[:, ev_seq[0]]
                    ev_seq = ev_seq[1:]
            else:
                select_load = np.random.choice(list_load, evs_per_feeder, replace=False)
                update_seq = []
                for load in select_load:
                    ids = self.update_data["sym_load"]["id"][0]
                    update_seq.append(
                        pd.DataFrame(self.update_data["sym_load"]["p_specified"], columns=ids).columns.get_loc(load)
                    )
                for seq in update_seq:
                    self.update_data["sym_load"]["p_specified"][:, seq] += self.ev.iloc[:, ev_seq[0]]
                    ev_seq = ev_seq[1:]
        self.pgc.set_update_data(self.update_data)
        return self.pgc.time_series_power_flow_calculation()


class optimal_tap_position:
    """
    The class used to find optimmal tap position of the transformer according to the input criteria.
    """

    def __init__(self, low_voltage_network_data: str, active_load_profile: str, reactive_load_profile: str):
        """
        Read from input data of the grid and parquet files, then create the graph:
        1.  Define PowerGridCalculation class
        2.  Construct the power grid model using the low voltage network data JSON file.
        3.  Create batch update dataset using the active and reactive load profile parquet files.


        Args:
        low_voltage_network_data (str): Path to the low voltage network data JSON file,
        active_load_profile (str), reactive_load_profile (str): Path to the active and reactive load profile parquet files.

        Returns:
        None

        Raises:
        None

        """
        self.power_grid_calculation = PowerGridCalculation()
        self.low_voltage_grid = self.power_grid_calculation.construct_pgm(low_voltage_network_data)
        self.load_profile_batch = self.power_grid_calculation.creat_batch_update_dataset(
            active_load_profile, reactive_load_profile
        )

    def find_optimal_tap_position(self, optimization_criteria):
        """
        Function that finds the optimal tap position
        of the LV transformer in the grid based on
        one of the two optimization criteria's:
        - minimal total energy loss of all the lines and the whole time period
        - minimal (averaged accross all nodes) deviation of p.u. voltages with respect to 1 p.u.

        1.  Find the minimum and maximum tap position of the transformer and create a list of tap positions in the range of min to max tap position.
        2.  Store the original tap position.
        3.  Make lists to store line_losses and voltage_deviations for each possible tap position.
        4.  If the criteria is to minimize line losses, loop through each tap position and:
            1.  Update the tap position in the power grid input data.
            2.  Create a power grid model with the updated tap position.
            3.  Run a time series power flow calculation.
            4.  Calculate the line losses of each line by integration over time.
            5.  Add up and store the total line losses in the line_losses list.
            5.  After the loop, find the index of the tap position corresponding with the minimum line losses and store it as optimal_tap_pos.

        5.  If the criteria is to minimize voltage deviations, loop through each tap position and:
            1.  Update the tap position in the power grid input data.
            2.  Create a power grid model with the updated tap position.
            3.  Run a time series power flow calculation.
            4.  For each node, find and store the maximum and minimum voltages.
            5.  Calculate the voltage deviation for each node and store the maximum voltage deviation in the voltage_deviations list.
            6.  After the loop, find the index of the tap position corresponding with the minimum voltage deviations and store it as optimal_tap_pos.

        6.  If the criteria is neither of the two, raise an error.
        7.  Set the tap position back to the initial value.
        8.  Return the optimal tap position.

        Args:
        optimization_criteria (str): The optimization criteria

        Returns:
        optimal_tap_pos (int): The optimal tap position

        Raises:
        OptimalTapPositionCriteriaError
        """

        # find minimum and maximum tap position; create a list of tap positions in range of min to max tap position
        min_tap_pos = self.low_voltage_grid["transformer"]["tap_min"][0]
        max_tap_pos = self.low_voltage_grid["transformer"]["tap_max"][0]
        tap_positions = [*range(min(min_tap_pos, max_tap_pos), max(min_tap_pos, max_tap_pos) + 1)]

        # store original tap position
        original_tap_pos = self.low_voltage_grid["transformer"]["tap_pos"][0]

        # lists to store relevant data for each tap position
        line_losses = []
        voltage_deviations = []

        if optimization_criteria == "minimize_line_losses":
            for tap_pos in tap_positions:
                # update tap position in power grid input data
                self.low_voltage_grid["transformer"]["tap_pos"] = [tap_pos]

                # create power grid model with updated tap position
                pow_grid_model = PowerGridModel(input_data=self.low_voltage_grid)

                # run time series power flow calculation
                pow_flow_result = pow_grid_model.calculate_power_flow(
                    update_data=self.load_profile_batch,
                    calculation_method=CalculationMethod.newton_raphson,
                    threading=0,
                )

                # line losses
                p_loss = pd.DataFrame()
                p_loss = pd.DataFrame(pow_flow_result["line"]["p_from"]) + pd.DataFrame(pow_flow_result["line"]["p_to"])

                table_line_losses = pd.DataFrame()
                table_line_losses["energy_loss_kw"] = 0.0

                for (column_name, column_data), i in zip(p_loss.items(), enumerate(p_loss.columns)):
                    table_line_losses.loc[i[0], "energy_loss_kw"] = integrate.trapezoid(column_data.to_list()) / 1000

                line_losses.append(sum(table_line_losses["energy_loss_kw"]))

            min_loss_idx = line_losses.index(min(line_losses))
            optimal_tap_pos = tap_positions[min_loss_idx]

        elif optimization_criteria == "minimize_voltage_deviations":
            for tap_pos in tap_positions:
                # update tap position in power grid input data
                self.low_voltage_grid["transformer"]["tap_pos"] = [tap_pos]

                # create power grid model with updated tap position
                pow_grid_model = PowerGridModel(input_data=self.low_voltage_grid)

                # run time series power flow calculation
                pow_flow_result = pow_grid_model.calculate_power_flow(
                    update_data=self.load_profile_batch,
                    calculation_method=CalculationMethod.newton_raphson,
                    threading=0,
                )

                # voltage deviations
                table_voltages = pd.DataFrame()
                table_voltages["max_pu"] = 0.0
                table_voltages["min_pu"] = 0.0
                i = 0
                for node_scenario in pow_flow_result["node"]:
                    df_temp = pd.DataFrame(node_scenario)
                    max_value_pu = df_temp.at[df_temp["u_pu"].idxmax(), "u_pu"]
                    min_value_pu = df_temp.at[df_temp["u_pu"].idxmin(), "u_pu"]
                    table_voltages.loc[i, "max_pu"] = max_value_pu
                    table_voltages.loc[i, "min_pu"] = min_value_pu
                    i = i + 1

                max_volt_deviation = max(max(abs(table_voltages["max_pu"] - 1)), max(abs(table_voltages["min_pu"] - 1)))
                voltage_deviations.append(max_volt_deviation)

            min_volt_dev_idx = voltage_deviations.index(min(voltage_deviations))
            optimal_tap_pos = tap_positions[min_volt_dev_idx]

        else:
            raise OptimalTapPositionCriteriaError("Criteria incorrect")

        # set tap position back to intial value
        self.low_voltage_grid["transformer"]["tap_pos"] = [original_tap_pos]

        return optimal_tap_pos


class n1_calculation:
    """
    The class used to do the N-1 calculation.
    """

    def __init__(self, network_data: str, meta_data: str, active_load_profile: str, reactive_load_profile: str):
        """
        Read from input data of the grid, meta data and parquet files, then create the graph:
        1.  Define PowerGridCalculation class
        2.  Construct the power grid model using the low voltage network data JSON file.
        3.  Create batch update dataset using the active and reactive load profile parquet files.
        4.  Define the input arguments for GraphProcessor using the grid and meta data.
        5.  Create a graph using the GraphProcessor class.

        Args:
        network_data (str): Path to the network data JSON file,
        meta_data (str): Path to the meta data JSON file,
        active_load_profile (str), reactive_load_profile (str): Path to the active and reactive load profile parquet files.

        Returns:
        None

        Raises:
        None

        """
        with open(meta_data, "r") as file:
            self.meta = json.load(file)
        self.pgc = PowerGridCalculation()
        self.grid = self.pgc.construct_pgm(network_data)
        self.update_data = self.pgc.creat_batch_update_dataset(active_load_profile, reactive_load_profile)
        self.df1 = pd.read_parquet(active_load_profile)
        self.timestamp = self.df1.index
        vertex_ids = self.grid["node"]["id"].tolist()
        edge_vertex_id_pairs = list(zip(self.grid["line"]["from_node"], self.grid["line"]["to_node"]))
        edge_vertex_id_pairs.append((self.grid["transformer"]["from_node"][0], self.grid["transformer"]["to_node"][0]))
        edge_ids = self.grid["line"]["id"].tolist()
        edge_ids.append(self.grid["transformer"]["id"][0])
        edge_enabled = np.logical_and(self.grid["line"]["from_status"], self.grid["line"]["to_status"]).tolist()
        edge_enabled.append(1)
        source_vertex_id = self.meta["mv_source_node"]
        self.gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        # self.G = self.gp.create()
        # nx.draw(self.G, with_labels=True)
        # plt.show()
        # print(edge_ids)

    def n1_calculate(self, line_id: int):
        """
        Find the list of the alternatives after disabling a given line to make the grid fully connected,
        and do power flow analysis for each one of them and return data as required.
        1.  Create an array to update line status.
        2.  Use the find_alternative_edges method to find the alternative edge IDs to make the graph fully connected.
        3.  Create a table to store the alternative line IDs, the line ID with the maximum loading, the maximum loading time, and the maximum loading value.
        4.  If there are no alternative edges, return a table with NaN values.
        5.  For each alternative edge that is found, create a new PowerGridModel, update the line status, and calculate the power flow.
        6.  Find the relevant parameters stated in step 3, and store them in the table.
        7.  Return the table.

        Args:
        line_id (int): The line ID to be disabled.

        Returns:
        table (DataFrame): DataFrame containing the alternative line IDs, the line ID with the maximum loading, the maximum loading time, and the maximum loading value.

        Raises:
        None
        """
        update_line = initialize_array("update", "line", 2)
        update_line["id"] = [line_id, 0]
        update_line["from_status"] = [0, 1]
        update_line["to_status"] = [0, 1]
        alt = self.gp.find_alternative_edges(line_id)
        table = pd.DataFrame()
        table["alt_Line_ID"] = alt
        table.set_index("alt_Line_ID")
        table["max_Line_ID"] = 0
        table["max_time"] = datetime.fromtimestamp(0)
        table["max__loading_pu"] = 0.0
        if not alt:
            table.iloc[:, :] = np.nan
            return table
        i = 0
        for line_alt in alt:
            model = PowerGridModel(input_data=self.grid)
            update_line["id"][1] = line_alt  # change line ID
            update_data = {"line": update_line}
            model.update(update_data=update_data)
            output_data = model.calculate_power_flow(
                update_data=self.update_data, calculation_method=CalculationMethod.newton_raphson
            )
            df_temp = pd.DataFrame(output_data["line"]["loading"])
            max_index = df_temp.stack().idxmax()
            table.loc[i, "max__loading_pu"] = df_temp.stack().max()
            table.loc[i, "max_time"] = self.timestamp[max_index[0]]
            table.loc[i, "max_Line_ID"] = max_index[1]
            i = i + 1
        return table
