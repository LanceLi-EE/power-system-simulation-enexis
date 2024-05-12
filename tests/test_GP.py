import networkx as nx
from power_system_simulation.GP import GraphProcessor



vertex_ids = ['A', 'B', 'C', 'D','E']
edge_vertex_id_pairs = [('A', 'B'), ('B', 'C'), ('C', 'D'), ('C', 'E'),('B','E')]
edge_ids = [1,2,3,4,5]
edge_enabled = [1,1,1,1,0]
source_vertex_id = 'A'    





def test_GP():
        gp_1 = GraphProcessor(vertex_ids,edge_ids,edge_vertex_id_pairs,edge_enabled,source_vertex_id)
        print(gp_1.find_downstream_vertices(3))
        print(gp_1.find_alternative_edges(2))

test_GP()