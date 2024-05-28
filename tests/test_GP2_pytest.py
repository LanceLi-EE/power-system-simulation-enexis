import pytest
from power_system_simulation.graph_processing import (
    GraphProcessor, 
    IDNotUniqueError,
    InputLengthDoesNotMatchError,
    IDNotFoundError,
    GraphNotFullyConnectedError,
    GraphCycleError,
    EdgeAlreadyDisabledError
    )


def test_ini_case1():
    vertex_ids = ["A", "A", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"

    with pytest.raises(IDNotUniqueError):
        # Attempt to create an instance of GraphProcessor
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
    

def test_ini_case2():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"

    with pytest.raises(InputLengthDoesNotMatchError):
        # Attempt to create an instance of GraphProcessor
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)


def test_ini_case3():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "Z")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"

    with pytest.raises(IDNotFoundError):
        # Attempt to create an instance of GraphProcessor
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)


def test_ini_case4():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1]
    source_vertex_id = "A"

    with pytest.raises(InputLengthDoesNotMatchError):
        # Attempt to create an instance of GraphProcessor
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)


def test_ini_case5():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "X"

    with pytest.raises(IDNotFoundError):
        # Attempt to create an instance of GraphProcessor
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)


def test_ini_case6():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 0, 1, 1, 0]
    source_vertex_id = "A"

    with pytest.raises(GraphNotFullyConnectedError):
        # Attempt to create an instance of GraphProcessor
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

def test_ini_case7():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 1]
    source_vertex_id = "A"

    with pytest.raises(GraphCycleError):
        # Attempt to create an instance of GraphProcessor
        gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)


def test_downstream_case1():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"
    gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
    
    with pytest.raises(IDNotFoundError):
        print(gp.find_downstream_vertices(6))
    

def test_downstream_case2():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"
    gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
    
    # No error is being checked here
    print("downstream_case2() return:", gp.find_downstream_vertices(5))
    

def test_downstream_case3():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"
    gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
    
    # No error is being checked here
    print("downstream_case3() return:", gp.find_downstream_vertices(3))
    

def test_alternative_case1():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"
    gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
    
    with pytest.raises(IDNotFoundError) as excinfo:
        print("alternative_case1() return:", gp.find_alternative_edges(6))
    

def test_alternative_case2():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"
    gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
    
    with pytest.raises(EdgeAlreadyDisabledError) as excinfo:
        print("alternative_case2() return:", gp.find_alternative_edges(5))
    

def test_alternative_case3():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"
    gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
    
    # No error is being checked here
    print("alternative_case3() return:", gp.find_alternative_edges(1))
    

def test_alternative_case4():
    vertex_ids = ["A", "B", "C", "D", "E"]
    edge_vertex_id_pairs = [("A", "B"), ("B", "C"), ("C", "D"), ("C", "E"), ("B", "E")]
    edge_ids = [1, 2, 3, 4, 5]
    edge_enabled = [1, 1, 1, 1, 0]
    source_vertex_id = "A"
    gp = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
    
    #No error is being checked here
    print("alternative_case4() return:", gp.find_alternative_edges(2))
    
# add new cases here with the same structure



