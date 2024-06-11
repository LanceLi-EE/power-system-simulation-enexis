import warnings
import json 
import pandas as pd 
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import pyarrow.parquet as pq

with warnings.catch_warnings(action="ignore", category=DeprecationWarning):
    # suppress warning about pyarrow as future required dependency
    from pandas import DataFrame

from power_system_simulation.power_grid_calculation import PowerGridCalculation
from power_system_simulation.graph_processing import GraphProcessor

from power_grid_model.utils import json_deserialize
from power_grid_model import PowerGridModel

#Input data validity check
class MoreThanOneTransformerOrSource(Exception):
    pass

class InvalidLVFeederID(Exception):
    pass

class MismatchFromAndToNodes(Exception):
    pass

class MismatchedTimetamps(Exception):
    pass

class MismatchedIDs(Exception):
    pass

class InvalidIDs(Exception):
    pass

class NotEnoughEVChargingProfiles(Exception):
    pass

class OptimalTapPositionCriteriaError(Exception):
    pass


class input_data_validity_check:
     #check valid PGM input data and if has cycles and if fully connected
    def __init__(self, network_data: str):
        self.pgc = PowerGridCalculation()
        self.grid = self.pgc.construct_PGM(network_data)
        
    # check LV grid has exactly one transformer and one source
    def check_grid(self, meta_data: str): 
        #read lv feeder info
        with open(meta_data, 'r' ) as file:
            self.meta = json.load(file)
        #check transformer and source
        if not(len(self.grid['transformer']) == 1 and len(self.grid['source']) == 1):
            raise MoreThanOneTransformerOrSource
        #check lv feeder info
        if not(set(self.meta['lv_feeders']).issubset(set(self.grid['line']['id']))):
            raise InvalidLVFeederID
        #check all the lines in the LV Feeder IDs have the from_nodes the same as the to_node of the transformer
        from_nodes = []
        for line in self.grid['line']:
            if line['id'] in self.meta['lv_feeders']:
                from_nodes.append(line['from_node'])
        if not(all(from_node == self.grid['transformer'][0]['to_node'] for from_node in from_nodes)):
            raise MismatchFromAndToNodes
        
    def check_graph(self):
        vertex_ids = self.grid['node']['id'].tolist()
        edge_vertex_id_pairs = list(zip(self.grid['line']['from_node'], self.grid['line']['to_node']))
        edge_vertex_id_pairs.append((self.grid['transformer']['from_node'][0], self.grid['transformer']['to_node'][0]))
        edge_ids = self.grid['line']['id'].tolist()
        edge_ids.append(self.grid['transformer']['id'][0])
        edge_enabled =  np.logical_and(self.grid['line']['from_status'], self.grid['line']['to_status']).tolist()
        edge_enabled.append(1)
        source_vertex_id = self.meta['mv_source_node']
        self.gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        
    #check if the timestamps and id are matching between the acitve load profile, reactive load profile and EV charging profile and if sym_load id matches
    def check_matching(self, active_load_profile: str , reactive_load_profile: str, ev_active_power_profile: str):
        self.df1 = pd.read_parquet(active_load_profile)
        self.df2 = pd.read_parquet(reactive_load_profile)
        self.df3 = pd.read_parquet(ev_active_power_profile)
        if not np.all(self.df1.index == self.df2.index):
            raise MismatchedTimetamps
        if not np.all(self.df2.index == self.df3.index):
            raise MismatchedTimetamps
        if not np.all(self.df1.columns == self.df2.columns):
            raise MismatchedIDs
        if not(set(self.grid['sym_load']['id']) == set(self.df1.columns)):
            raise InvalidIDs
        
    #check the number of EV charging profiles is at least the number of sym_load
    def check_EV_charging_profiles(self):
        if len(self.df3.columns) < len(self.grid['sym_load']):
            raise NotEnoughEVChargingProfiles

class EV_penetration_level:
    pass

class optimal_tap_position:
    def __init__(self, low_voltage_network_data: str, active_load_profile: str, reactive_load_profile:str):
        self.power_grid_calculation = PowerGridCalculation()
        self.low_voltage_grid = self.power_grid_calculation.construct_PGM(low_voltage_network_data)
        self.load_profile_batch = self.power_grid_calculation.creat_batch_update_dataset(active_load_profile, reactive_load_profile)

    def find_optimal_tap_position(self, optimization_criteria):
        """
        Function that finds the optimal tap position
        of the LV transformer in the grid based on
        one of the two optimization criteria's:
        - minimal total energy loss of all the lines and the whole time period
        - minimal (averaged accross all nodes) deviation of p.u. voltages with respect to 1 p.u.
        """

        # find minimum and maximum tap position; create a list of tap positions in range of min to max tap position
        min_tap_pos = self.low_voltage_grid["transformer"]["tap_min"][0]
        max_tap_pos = self.low_voltage_grid["transformer"]["tap_max"][0]
        tap_positions = [*range(min(min_tap_pos, max_tap_pos), max(min_tap_pos, max_tap_pos)+1)]
        
        # store original tap position
        original_tap_pos = self.low_voltage_grid["transformer"]["tap_pos"][0]

        # lists to store relevant data for each tap position
        line_losses = list()
        voltage_deviations = list()

        for tap_pos in tap_positions:
            # update tap position in power grid input data
            self.low_voltage_grid["trandformer"]["tap_pos"] = [tap_pos]

            # create power grid model with updated tap position
            pow_grid_model = PowerGridModel(input_data=self.low_voltage_grid)

            # run time series power flow calculation
            pow_flow_result = pow_grid_model.calculate_power_flow(
                update_data = self.load_profile_batch, calculation_method=CalculationMethod.newton_raphson
            )

            # extract relevant information about lines:

            # line losses
            p_loss = pd.DataFrame()
            p_loss = abs(abs(pd.DataFrame(pow_flow_result["line"]["p_from"])) - abs(pd.DataFrame(pow_flow_result["line"]["p_to"])))

            table_line_losses = pd.DataFrame()
            table_line_losses["energy_loss_kw"] = 0.0
            i = 0
            for column_name, column_data in p_loss.items():
                table_line_losses.loc[i, "energy_loss_kw"] = integrate.trapezoid(column_data.to_list()) / 1000
                i = i + 1

            line_losses.append(sum(table_line_losses["energy_loss_kw"]))

            # voltage deviations
            table_voltages = pd.DataFrame()
            table_voltages["max_pu"] = 0.0
            table_voltages["min_pu"] = 0.0
            i = 0
            for node_scenario in pow_flow_result["node"]:
                df_temp = pd.DataFrame(node_scenario)
                max_value_pu = df_temp.at[df_temp["u_pu"].idxmax(), "u_pu"]
                min_value_pu = df_temp.at[df["u_pu"].idxmin(), "u_pu"]
                table_voltages.loc[i, "max_pu"] = max_value_pu
                table_voltages.loc[i, "min_pu"] = min_value_pu
                i = i + 1
            
            max_volt_deviation = max(max(abs(table_voltages["max_pu"]-1)), max(abs(table_voltages["min_pu"]-1)))
            voltage_deviations.append(max_volt_deviation)

        # find optimal tap bosition based on criteria chosen by user
        if optimization_criteria == "minimize_line_losses":
            min_loss_idx = line_losses.index(min(line_losses))
            optimal_tap_pos = tap_positions[min_loss_idx]
        elif optimization_criteria == "minimize_voltage_deviations":
            min_volt_dev_idx = voltage_deviations.index(min(voltage_deviations))
            optimal_tap_pos = tap_positions[min_volt_dev_idx]
        else:
            raise OptimalTapPositionCriteriaError("Criteria incorrect")

        # set tap position back to intial value
        self.low_voltage_grid["transformer"]["tap_pos"] = [original_tap_pos]

        return optimal_tap_pos
        



            

class N1_calculation:
    def __init__(self, network_data: str, meta_data: str):
        with open(meta_data, 'r' ) as file:
            self.meta = json.load(file)
        self.pgc = PowerGridCalculation()
        self.grid = self.pgc.construct_PGM(network_data)
        vertex_ids = self.grid['node']['id'].tolist()
        edge_vertex_id_pairs = list(zip(self.grid['line']['from_node'], self.grid['line']['to_node']))
        edge_vertex_id_pairs.append((self.grid['transformer']['from_node'][0], self.grid['transformer']['to_node'][0]))
        edge_ids = self.grid['line']['id'].tolist()
        edge_ids.append(self.grid['transformer']['id'][0])
        edge_enabled =  np.logical_and(self.grid['line']['from_status'], self.grid['line']['to_status']).tolist()
        edge_enabled.append(1)
        source_vertex_id = self.meta['mv_source_node']
        self.gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        self.G = self.gp.create()
        nx.draw(self.G, with_labels=True)
        plt.show()
        #print(edge_ids)

    def N1(self):
        for id in self.grid['line']['id']:
         print(self.gp.find_alternative_edges(id))
        

    