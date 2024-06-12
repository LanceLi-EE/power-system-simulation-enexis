"""
This is a skeleton for the graph processing assignment.

We define a graph processor class with some function skeletons.
"""

import copy
from typing import List, Tuple

import networkx as nx


class IDNotFoundError(Exception):
    '''
    edge_vertex_id_pairs should contain valid vertex ids.
    source_vertex_id should be a valid vertex id.
    '''


class InputLengthDoesNotMatchError(Exception):
    '''
    edge_vertex_id_pairs should have the same length as edge_ids. (InputLengthDoesNotMatchError)
    edge_enabled should have the same length as edge_ids.
    '''


class IDNotUniqueError(Exception):
    '''
    vertex_ids and edge_ids should be unique. (IDNotUniqueError)
    '''


class GraphNotFullyConnectedError(Exception):
    '''
    The graph should be fully connected.
    '''


class GraphCycleError(Exception):
    '''
    The graph should not contain cycles.
    '''


class EdgeAlreadyDisabledError(Exception):
    '''
    The given edge_id is a disabled edge
    '''


class GraphProcessor:
    """
    The class used to check the structure of the grid, which is processed as graph
    """

    def __init__(
        self,
        vertex_ids: List[int],
        edge_ids: List[int],
        edge_vertex_id_pairs: List[Tuple[int, int]],
        edge_enabled: List[bool],
        source_vertex_id: int,
    ) -> None:
        """
        Initialize a graph processor object with an undirected graph.
        Only the edges which are enabled are taken into account.
        Check if the input is valid and raise exceptions if not.
        The following conditions should be checked:
            1. vertex_ids and edge_ids should be unique. (IDNotUniqueError)
            2. edge_vertex_id_pairs should have the same length as edge_ids. (InputLengthDoesNotMatchError)
            3. edge_vertex_id_pairs should contain valid vertex ids. (IDNotFoundError)
            4. edge_enabled should have the same length as edge_ids. (InputLengthDoesNotMatchError)
            5. source_vertex_id should be a valid vertex id. (IDNotFoundError)
            6. The graph should be fully connected. (GraphNotFullyConnectedError)
            7. The graph should not contain cycles. (GraphCycleError)
        If one certain condition is not satisfied, the error in the parentheses should be raised.

        Args:
            vertex_ids: list of vertex ids
            edge_ids: liest of edge ids
            edge_vertex_id_pairs: list of tuples of two integer
                Each tuple is a vertex id pair of the edge.
            edge_enabled: list of bools indicating of an edge is enabled or not
            source_vertex_id: vertex id of the source in the graph
        """

        if len(vertex_ids) != len(set(vertex_ids)) or len(edge_ids) != len(set(edge_ids)):
            raise IDNotUniqueError

        if len(edge_ids) != len(set(edge_vertex_id_pairs)):
            raise InputLengthDoesNotMatchError

        for pair in edge_vertex_id_pairs:
            for vertex_id in pair:
                if vertex_id not in vertex_ids:
                    raise IDNotFoundError

        if len(edge_enabled) != len(edge_ids):
            raise InputLengthDoesNotMatchError

        if source_vertex_id not in vertex_ids:
            raise IDNotFoundError

        self.edge_ids = edge_ids
        self.edge_vertex_id_pairs = edge_vertex_id_pairs
        self.edge_enabled = edge_enabled
        self.source_vertex_id = source_vertex_id

        self.graph = nx.Graph()
        self.graph.add_nodes_from(vertex_ids)
        for (u, v), enabled in zip(edge_vertex_id_pairs, edge_enabled):
            if enabled:
                self.graph.add_edge(u, v)

        if not nx.is_connected(self.graph):
            raise GraphNotFullyConnectedError

        if nx.cycle_basis(self.graph):
            raise GraphCycleError

    def find_downstream_vertices(self, edge_id: int) -> List[int]:
        """
        Given an edge id, return all the vertices which are in the downstream of the edge,
            with respect to the source vertex.
            Including the downstream vertex of the edge itself!

        Only enabled edges should be taken into account in the analysis.
        If the given edge_id is a disabled edge, it should return empty list.
        If the given edge_id does not exist, it should raise IDNotFoundError.


        For example, given the following graph (all edges enabled):

            vertex_0 (source) --edge_1-- vertex_2 --edge_3-- vertex_4

        Call find_downstream_vertices with edge_id=1 will return [2, 4]
        Call find_downstream_vertices with edge_id=3 will return [4]

        Args:
            edge_id: edge id to be searched

        Returns:
            A list of all downstream vertices.
        """
        # put your implementation here

        if edge_id not in self.edge_ids:
            raise IDNotFoundError

        for edge, enabled, (u, v) in zip(self.edge_ids, self.edge_enabled, self.edge_vertex_id_pairs):
            if edge == edge_id:
                if not enabled:
                    return []
                g_copy1 = copy.deepcopy(self.graph)
                g_copy1.remove_edge(u, v)
                for component in nx.connected_components(g_copy1):
                    if self.source_vertex_id not in component:
                        return sorted(component)

    def find_alternative_edges(self, disabled_edge_id: int) -> List[int]:
        """
        Given an enabled edge, do the following analysis:
            If the edge is going to be disabled,
                which (currently disabled) edge can be enabled to ensure
                that the graph is again fully connected and acyclic?
            Return a list of all alternative edges.
        If the disabled_edge_id is not a valid edge id, it should raise IDNotFoundError.
        If the disabled_edge_id is already disabled, it should raise EdgeAlreadyDisabledError.
        If there are no alternative to make the graph fully connected again, it should return empty list.


        For example, given the following graph:

        vertex_0 (source) --edge_1(enabled)-- vertex_2 --edge_9(enabled)-- vertex_10
                 |                               |
                 |                           edge_7(disabled)
                 |                               |
                 -----------edge_3(enabled)-- vertex_4
                 |                               |
                 |                           edge_8(disabled)
                 |                               |
                 -----------edge_5(enabled)-- vertex_6

        Call find_alternative_edges with disabled_edge_id=1 will return [7]
        Call find_alternative_edges with disabled_edge_id=3 will return [7, 8]
        Call find_alternative_edges with disabled_edge_id=5 will return [8]
        Call find_alternative_edges with disabled_edge_id=9 will return []

        Args:
            disabled_edge_id: edge id (which is currently enabled) to be disabled

        Returns:
            A list of alternative edge ids.
        """
        # put your implementation here# Disabled edge is not valid edge id
        if disabled_edge_id not in self.edge_ids:
            raise IDNotFoundError

        # Disabled edge is already disabled
        g_copy2 = copy.deepcopy(self.graph)
        for edge, enabled, (u, v) in zip(self.edge_ids, self.edge_enabled, self.edge_vertex_id_pairs):
            if edge == disabled_edge_id:
                if not enabled:
                    raise EdgeAlreadyDisabledError
                g_copy2.remove_edge(u, v)  # if the edge is abled just remove it

        # find alternative
        alternative = []
        for edge, enabled, (u, v) in zip(self.edge_ids, self.edge_enabled, self.edge_vertex_id_pairs):
            if not enabled:  # check every other disabled edge
                g_copy2.add_edge(u, v)  # if it's disabled then able it
                if nx.is_connected(g_copy2):  # check if the graph is fully connected or not
                    alternative.append(edge)  # add it to the list
                    g_copy2.remove_edge(u, v)  # recover the graph and check next one

        return sorted(alternative)
