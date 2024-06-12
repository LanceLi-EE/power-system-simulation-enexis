import unittest

import networkx as nx

from power_system_simulation.graph_processing import GraphProcessor


class TestMyClass(unittest.TestCase):
    def test_ini_case1(self):
        vertex_ids = ["A", "A", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        try:
            # call the class.function, if there is an error then record it
            gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        except Exception as e:
            # if there is an error, print the information and continue to next test case
            print("ini_case1() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_ini_case2(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        try:
            gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        except Exception as e:
            print("ini_case2() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_ini_case3(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "Z")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        try:
            gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        except Exception as e:
            print("ini_case3() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_ini_case4(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1]
        source_vertex_id = "A"
        try:
            gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        except Exception as e:
            print("ini_case4() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_ini_case5(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "X"
        try:
            gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        except Exception as e:
            print("ini_case5() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_ini_case6(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 0, 1, 1, 0]
        source_vertex_id = "A"
        try:
            gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        except Exception as e:
            print("ini_case6() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_ini_case7(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 1]
        source_vertex_id = "A"
        try:
            gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        except Exception as e:
            print("ini_case7() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_downstream_case1(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        try:
            print(gp.find_downstream_vertices(6))
        except Exception as e:
            print("downstream_case1() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_downstream_case2(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        try:
            print("downstream_case2() return:", gp.find_downstream_vertices(5))
        except Exception as e:
            print("downstream_case2() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_downstream_case3(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        try:
            print("downstream_case3() return:", gp.find_downstream_vertices(3))
        except Exception as e:
            print("downstream_case3() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_alternative_case1(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        try:
            print("alternative_case1() return:", gp.find_alternative_edges(6))
        except Exception as e:
            print("alternative_case1() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_alternative_case2(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        try:
            print("alternative_case2() return:", gp.find_alternative_edges(5))
        except Exception as e:
            print("alternative_case2() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_alternative_case3(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        try:
            print("alternative_case3() return:", gp.find_alternative_edges(1))
        except Exception as e:
            print("alternative_case3() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    def test_alternative_case4(self):
        vertex_ids = ["A", "B", "C", "D", "E"]
        edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
        edge_ids = [1, 2, 3, 4, 5]
        edge_enabled = [1, 1, 1, 1, 0]
        source_vertex_id = "A"
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
        try:
            print("alternative_case4() return:", gp.find_alternative_edges(2))
        except Exception as e:
            print("alternative_case4() raise custom error:", e.__class__.__name__)
            print("detail:", e)

    # add new cases here with the same structure


if __name__ == "__main__":
    unittest.main()

# push to Joshua branch