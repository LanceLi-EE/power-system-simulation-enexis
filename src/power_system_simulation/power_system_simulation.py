import warnings
import json 
import pandas as pd 
import numpy as np
import pyarrow.parquet as pq

with warnings.catch_warnings(action="ignore", category=DeprecationWarning):
    # suppress warning about pyarrow as future required dependency
    from pandas import DataFrame

import power_system_simulation.power_grid_calculation as PGC
import power_system_simulation.graph_processing as GP
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
        self.pgc = PGC.PowerGridCalculation()
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
        vertex_ids = self.grid['node']['id']
        edge_vertex_id_pairs = list(zip(self.grid['line']['from_node'], self.grid['line']['to_node']))
        edge_ids = self.grid['line']['id']
        edge_enabled = self.grid['line']['to_status']
        source_vertex_id = self.meta['mv_source_node'][0]
        self.gp = GP.GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        
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