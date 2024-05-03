"""
This is a skeleton for the graph processing assignment.

We define a graph processor class with some function skeletons.
"""

from typing import List, Tuple


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
    General documentation of this class.
    You need to describe the purpose of this class and the functions in it.
    We are using an undirected graph in the processor.
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
        if  len(vertex_ids) != len(set(vertex_ids))  or  len(edge_ids) != len(set(edge_ids)) :
            raise IDNotUniqueError
        
        if len(edge_ids) != len(set(edge_vertex_id_pairs)):
            raise InputLengthDoesNotMatchError
        
        for pair in edge_vertex_id_pairs:
            for id in pair:
                if id not in vertex_ids:
                    raise IDNotFoundError
        
        if len(edge_enabled) != len(edge_ids):
            raise InputLengthDoesNotMatchError
        
        if source_vertex_id not in vertex_ids:
            raise IDNotFoundError
        

        def is_fully_connected(edge_list: List[Tuple[int, int]]) -> bool:
            # reconstruct graph
            graph = {}
            for edge in edge_list:
                u, v = edge
                graph.setdefault(u, []).append(v)
                graph.setdefault(v, []).append(u)
    
            # use dfs from the 1st node
            start_node = next(iter(graph))
            visited = set()
            stack = [start_node]
            visited.add(start_node)
            while stack:
                node = stack.pop()
                for neighbor in graph.get(node, []):
                    if neighbor not in visited:
                        stack.append(neighbor)
                        visited.add(neighbor)
    
            # check if fully connected
            return len(visited) == len(graph)

        def has_cycle(edge_list: List[Tuple[int, int]])-> bool:
            graph = {}  # 存储图的邻接表表示

            # 构建图的邻接表
            for edge in edge_list:
                u, v = edge
                graph.setdefault(u, []).append(v)
                graph.setdefault(v, []).append(u)

            visited = set()  # 用于跟踪已经访问过的节点
            stack = set()    # 用于跟踪当前搜索路径上的节点

            # 深度优先搜索函数
            def dfs(node, parent):
                visited.add(node)  # 将当前节点标记为已访问
                stack.add(node)    # 将当前节点添加到搜索路径中

                # 遍历当前节点的邻居节点
                for neighbor in graph.get(node, []):
                    # 如果邻居节点在搜索路径中，并且不是当前节点的父节点，则说明存在环
                    if neighbor in stack and neighbor != parent:
                        return True
                    # 如果邻居节点未访问过，则继续深度优先搜索
                    elif neighbor not in visited:
                        if dfs(neighbor, node):
                            return True

                stack.remove(node)  # 从搜索路径中移除当前节点
                return False

            # 对每个节点进行深度优先搜索
            for node in graph:
                if node not in visited:
                    if dfs(node, None):
                        return True  # 如果发现环，则返回 True

            return False  # 如果遍历完所有节点仍未发现环，则返回 False

        if not is_fully_connected(edge_vertex_id_pairs):
            raise GraphNotFullyConnectedError

        if has_cycle(edge_vertex_id_pairs):
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
        pass

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
        # put your implementation here
        pass
