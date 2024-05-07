import networkx as nx
from power_system_simulation.GP import GraphProcessor

G = nx.Graph()

nodes = ['A', 'B', 'C', 'D']
for node_id in nodes:
    G.add_node(node_id)

edges = [('A', 'B', {'edge_id': 1}), ('B', 'C', {'edge_id': 2}), ('C', 'D', {'edge_id': 3}), ('D', 'A', {'edge_id': 4})]
G.add_edges_from(edges)

vertex_ids = list(G.nodes())
edge_ids = [(u, v, G[u][v]['edge_id']) for u, v in G.edges()]
edge_vertex_id_pairs = list(G.edges)
edge_enabled = [1,1,1,0]
source_vertex_id = 'A'    


gp_1 = GraphProcessor



def test_GP():
        gp_1(vertex_ids,edge_ids,edge_vertex_id_pairs,edge_enabled,source_vertex_id)


test_GP()