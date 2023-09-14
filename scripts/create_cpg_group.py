# %%
from typing import List
import sys
import json
import math
import nest_asyncio
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource, __
from gremlin_python.structure.graph import Edge, Vertex
from gremlin_python.process.traversal import (
    Barrier,
    Bindings,
    Cardinality,
    Column,
    Direction,
    Operator,
    Order,
    P,
    Pop,
    Scope,
    T,
    WithOptions,
)

nest_asyncio.apply()  # to support gremlin event loop in jupyter lab

# set the graph traversal from the local machine:
# connection = DriverRemoteConnection(
#     "wss://localhost:8182/gremlin",
#     "g",
# )  # Connect it to the DB server
connection = DriverRemoteConnection(
    "wss://db-bio-annotations.cluster-cu9wyuyqqen8.ap-southeast-1.neptune.amazonaws.com:8182/gremlin",
    "g",
    # transport_factory=lambda: AiohttpTransport(call_from_event_loop=True),
)  # Connect it to the DB server
# connection = DriverRemoteConnection(
#     "ws://172.16.1.100:8182/gremlin", "g"
# )  # Connect it to the DB server
g = traversal().withRemote(connection)


# %% Query for creating groups of CpGs based on user input:
def clean_nan_values(data):
    if isinstance(data, list):
        return [clean_nan_values(x) for x in data]
    elif isinstance(data, dict):
        return {k: clean_nan_values(v) for k, v in data.items()}
    elif isinstance(data, float) and math.isnan(data):
        return None
    else:
        return data


def create_cpg_group(
    g: GraphTraversalSource, cpg_names_input: List[str], cpg_group_name: str
) -> List[dict]:
    # print("Received cpg_names_input:", cpg_names_input)
    # print("Received cpg_group_name:", cpg_group_name)

    cpg_vertices_dict = {}
    # Get vertices of the requested CpGs and their property values based on the name property
    cpg_vertices = (
        g.V()
        .hasLabel('cpg')
        .has('name', P.within(*cpg_names_input))
        .valueMap()
        .toList()
    )  # If we want to save memory space and get specific properties instead of all properties, we can use the by modulator.
    sys.stderr.write("Fetched cpg_vertices: " + str(cpg_vertices) + "\n")
    # Store the retrieved vertices in the dictionary with the user-provided group name as the key
    cpg_vertices_dict[cpg_group_name] = cpg_vertices
    cleaned_data = clean_nan_values(cpg_vertices_dict)
    print(json.dumps(cleaned_data))
    return cpg_vertices_dict


# %% Testing create_cpg_group with hard-coded input:
# cpg_names_input_test = ["cg24115571", "cg26498966", "cg13181537", "cg01261464"]
# cpg_group_name_test = "Nicole's Favorite Sleep-Related CpGs"
# fav_sleep_cpgs = create_cpg_group(g, cpg_names_input_test, cpg_group_name_test)
# print(fav_sleep_cpgs)


# %% Accept and use command-line arguments to access cpg file & group_name inputs from the frontend:
if __name__ == "__main__":
    data_list = json.loads(sys.argv[1])
    group_name = sys.argv[2]
    create_cpg_group(g, data_list, group_name)
