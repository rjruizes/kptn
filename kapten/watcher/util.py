
def is_mock(graph: str):
    mock_graphs = ["sample", "subtasktest", "combotest", "bundling", "grouping", "bundlegroup", "bundlegroup_R", "output_hashing"]
    return graph in mock_graphs