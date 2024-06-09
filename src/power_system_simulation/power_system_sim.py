import warnings
import json 

import pyarrow.parquet as pq

with warnings.catch_warnings(action="ignore", category=DeprecationWarning):
    # suppress warning about pyarrow as future required dependency
    from pandas import DataFrame

import power_system_simulation.power_grid_calculation as PGC
import power_system_simulation.graph_processing as GP

#Input data validity check
class InvalidPGMInputData(Exception):
    pass

class MoreThanOneTransformerOrSource(Exception):
    pass

class InvalidLVFeederID(Exception):
    pass

class MismatchFromAndToNodes(Exception):
    pass

class GraphNotFullyConnectedError(Exception):
    pass

class GraphCycleError(Exception):
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
    def __init__(self) -> None:
        pass
    #check valid PGM input data
    def check_valid_PGM_input_data(self, data_path: str):
        try:
            PGC.PowerGridCalculation().construct_PGM(data_path)
        except Exception as e:
            raise InvalidPGMInputData

    # check LV grid has exactly one transformer and one source
    def check_transformer_source(self, input_network_data: str): 
        with open(input_network_data, 'r' ) as file:
            self.input_data = json.load(file)

        if len(self.input_data['data']['transformer']) == 1 and len(self.input_data['data']['source']) == 1:
            return 
        else: 
            raise MoreThanOneTransformerOrSource
            
    #check all IDs in LV Feeder are valid line IDs
    def check_valid_LV_feeder_ID(self, meta_data: str):  
        ids = []
        for i in self.input_data['data']['line']:
            ids.append(i['id'])

        with open(meta_data, 'r' ) as file:
            self.meta_data = json.load(file)

            if all(element in self.meta_data['lv_feeders']for element in ids):
                return
            else:
                raise InvalidLVFeederID
            
    #check all the lines in the LV Feeder IDs have the from_nodes the same as the to_node of the transformer
    def check_from_to_nodes(self):
        #from_nodes of the lines in the LV Feeder
        from_nodes = []
        lines = self.input_data['data']['line']
        for line in lines:
            if line['id'] in self.meta_data['lv_feeders']:
                from_nodes.append(line['from_node'])
        if all(from_node == self.input_data['data']['transformer'][0]['to_node'] for from_node in from_nodes):
            return
        else:
            raise MismatchFromAndToNodes
        
    #check the graph is fully connected
    def check_fully_connected_graph(self):
        self.graph = GP.GraphProcessing().create_graph(self.input_data)
        if GP.GraphProcessing().is_fully_connected(self.graph):
            return
        else:
            raise GraphNotFullyConnectedError
        
    #check the graph has no cycles  
    def check_no_cycles(self):
        if GP.GraphProcessing().has_cycle(self.graph):
            raise GraphCycleError
        else:
            return
        
    #check the timestamps are matching between the acitve load profile, reactive load profile and EV charging profile
    def check_matching_timestamps(self, active_load_profile: str , reactive_load_profile: str, ev_active_power_profile: str):
        self.df1 = pq.read_table(active_load_profile)
        self.df2 = pq.read_table(reactive_load_profile)
        self.df3 = pq.read_table(ev_active_power_profile)

        timestamps1 = self.df1["Timestamp"].to_pandas()
        timestamps2 = self.df2["Timestamp"].to_pandas()
        timestamps3 = self.df3["Timestamp"].to_pandas()

        comparison_1_2 = timestamps1.equals(timestamps2)
        comparison_1_3 = timestamps1.equals(timestamps3)
        comparison_2_3 = timestamps2.equals(timestamps3)

        if comparison_1_2 and comparison_1_3 and comparison_2_3:
            return
        else:
            raise MismatchedTimetamps
        
    #check the IDs are matching between the acitve load profile, reactive load profile
    def check_matching_IDs(self):
        self.table1 = self.df1.to_pandas()
        self.table2 = self.df2.to_pandas()
        if all(self.table1.columns == self.table2.columns):
            return
        else:
            raise MismatchedIDs

    #check the IDs in active load profile and reactive load profile are valid IDs of sym_load
    def check_valid_IDs(self):
        self.sym_load_ids = []
        for i in self.input_data['data']['sym_load']:
            self.sym_load_ids.append(i['id'])
        if all(element in self.sym_load_ids for element in self.table1.columns) & all(element in self.sym_load_ids for element in self.table2.columns):
            return
        else:
            raise InvalidIDs
        
    #check the number of EV charging profiles is at least the number of sym_load
    def check_EV_charging_profiles(self):
        table3 = self.df3.to_pandas()
        if len(table3.columns) >= len(self.sym_load_ids):
            return
        else:
            raise NotEnoughEVChargingProfiles

