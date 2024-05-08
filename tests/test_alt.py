import networkx as nx
from power_system_simulation.GP import GraphProcessor

vertex_ids = ['A', 'B', 'C', 'D', 'E']
edge_vertex_id_pairs = [('A', 'B'), ('B', 'C'), ('B', 'D'), ('D', 'E'), ('A', 'E'), ('A', 'D')]
edge_ids = [1,2,3,4,5,6]
edge_enabled = [1,1,0,0,1,1]
source_vertex_id = 'A'    

def test_GP():
        gp_1 = GraphProcessor(vertex_ids,edge_ids,edge_vertex_id_pairs,edge_enabled,source_vertex_id)
        print(gp_1.find_alternative_edges(1))

def test_GP2():
        gp_1 = GraphProcessor(vertex_ids,edge_ids,edge_vertex_id_pairs,edge_enabled,source_vertex_id)
        print(gp_1.find_alternative_edges(2))

def test_GP3():
        gp_1 = GraphProcessor(vertex_ids,edge_ids,edge_vertex_id_pairs,edge_enabled,source_vertex_id)
        print(gp_1.find_alternative_edges(5))

test_GP()
test_GP2()
test_GP3()