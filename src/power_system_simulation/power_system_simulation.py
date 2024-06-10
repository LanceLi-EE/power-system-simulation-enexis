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

class Optimal_tap_position:
    pass

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
        

        






        