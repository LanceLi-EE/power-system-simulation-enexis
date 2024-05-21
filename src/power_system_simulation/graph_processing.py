import copy
from typing import List, Tuple

import networkx as nx


class IDNotFoundError(Exception):
    pass


class InputLengthDoesNotMatchError(Exception):
    pass


class IDNotUniqueError(Exception):
    pass


class GraphNotFullyConnectedError(Exception):
    pass


class GraphCycleError(Exception):
    pass


class EdgeAlreadyDisabledError(Exception):
    pass


class GraphProcessor:
    """
    General documentation of this class:
    The constructor initializes the 'GraphProcessor' object with an undirected graph.
    The following conditions will be checked & exceptions will be raised if not satisfied:
        1. vertex_ids and edge_ids should be unique. (IDNotUniqueError)
        2. edge_vertex_id_pairs should have the same length as edge_ids. (InputLengthDoesNotMatchError)
        3. edge_vertex_id_pairs should contain valid vertex ids. (IDNotFoundError)
        4. edge_enabled should have the same length as edge_ids. (InputLengthDoesNotMatchError)
        5. source_vertex_id should be a valid vertex id. (IDNotFoundError)
        6. The graph should be fully connected. (GraphNotFullyConnectedError)
        7. The graph should not contain cycles. (GraphCycleError)

        Args:
            vertex_ids: list of vertex ids
            edge_ids: liest of edge ids
            edge_vertex_id_pairs: list of tuples of two integer
                Each tuple is a vertex id pair of the edge.
            edge_enabled: list of bools indicating of an edge is enabled or not
            source_vertex_id: vertex id of the source in the graph
    """

    def __init__(
        self,
        vertex_ids: List[int],
        edge_ids: List[int],
        edge_vertex_id_pairs: List[Tuple[int, int]],
        edge_enabled: List[bool],
        source_vertex_id: int,
    ) -> None:

        self.edge_ids = edge_ids
        self.edge_vertex_id_pairs = edge_vertex_id_pairs
        self.edge_enabled = edge_enabled
        self.source_vertex_id = source_vertex_id
        self.graph = nx.Graph()
        self.graph.add_nodes_from(vertex_ids)

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

        for (u, v), enabled in zip(edge_vertex_id_pairs, edge_enabled):
            if enabled:
                self.graph.add_edge(u, v)

        if not nx.is_connected(self.graph):
            raise GraphNotFullyConnectedError

        if nx.cycle_basis(self.graph):
            raise GraphCycleError

    def find_downstream_vertices(self, edge_id: int) -> List[int]:
        """
        This function returns the downstream vertices of the given edge id.
        It only considers the enabled edges.
        If the given edge_id is a disabled edge, it returns empty list.
        If the given edge_id does not exist, it raises IDNotFoundError.

        Args:
            edge_id: edge id to be searched

        Returns:
            A list of all downstream vertices.
        """
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
        This function analyzes the effect of disabling a given edge on the graph connectivity and acyclicity.
        Identifies edges that can be enabled to ensure that the graph is again fully connected and acyclic.
        The following conditions will be checked & exceptions will be raised if not satisfied:
            1. disabled_edge_id should be a valid edge id. (IDNotFoundError)
            2. disabled_edge_id should be an enabled edge. (EdgeAlreadyDisabledError)

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
