# %%
from typing import Optional
from typing import List

import nest_asyncio
import pandas as pd
from gremlin_python.driver.aiohttp.transport import AiohttpTransport
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
from tqdm import tqdm

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


# %% Validate connection, should return ‘0’
# g.V().drop().iterate()  # DANGEROUS: drop the whole graph
# run command below to check if database is connected (# of vertices):
g.V().count().next()


# %% Print the names associated with each library (e.g. gremlin 'desc' = python Order.desc)
print("T: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(T)))))
print("Order: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Order)))))
print("Cardinality: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Cardinality)))))
print("Column: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Column)))))
print("Direction: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Direction)))))
print("Operator: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Operator)))))
print("P: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(P)))))
print("Pop: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Pop)))))
print("Scope: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Scope)))))
print("Barrier: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Barrier)))))
print("Bindings: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(Bindings)))))
print("WithOptions: " + str(list(filter(lambda x: not str(x).startswith("_"), dir(WithOptions)))))


# %%
cpg_df = pd.read_csv("data/Cpgs.csv", index_col=0)
article_df = pd.read_csv("data/Articles.csv", index_col=0)
assoc_df = pd.read_csv("data/Associations.csv", index_col=0)
# assoc_df


# %%
a = [idx * 2 for idx in range(10)]

a = []
for idx in range(10):
    a[idx] = idx * 2


# %% Add data
# def get_edges(
#     g: GraphTraversalSource, out_vertex_id: str, edge_label: str, in_vertex_id: str
# ):
#     edges = g.V(out_vertex_id).outE(edge_label).where(__.inV().hasId(in_vertex_id)).toList()
#     print(edges)


def add_data(
    g: GraphTraversalSource, article_df: pd.DataFrame, assoc_df: pd.DataFrame, cpg_df: pd.DataFrame
):
    # ARTICLE_FIRST_ID = 1000
    # CPG_FIRST_ID = 1000000

    # check if addV returns a new ID for each vertex
    # declare dicts:
    article_id_dict = {}
    assoc_id_dict = {}
    cpg_id_dict = {}

    for row_idx, article_data in article_df.iterrows():
        article_sql_id = row_idx
        article_node: Vertex = (
            g.addV("article")
            .property("article ID", article_sql_id)
            .property("doi", article_data["DOI"])
            .property("normalization method", article_data["Normalization"])
            .next()
        )

        # print(g.V(article_node).id().next())
        # print(article_node, type(article_node), article_node.__dict__)
        # print(article_node.id)

        # write gremlin query to get id of the newly added node, newID -> pass in to the code below
        article_id_dict[article_sql_id] = article_node.id

    for row_idx, assoc_data in assoc_df.iterrows():
        assoc_sql_id = row_idx
        # print(assoc_id)
        assoc_node: Vertex = (
            g.addV("health factor")
            .property("assoc ID", assoc_sql_id)
            .property("name", assoc_data["Association"])
            .next()
        )
        assoc_id_dict[assoc_sql_id] = assoc_node.id

    cpg_list: list = cpg_df["CpG"].unique().tolist()
    limit = 10
    for count, (row_idx, cpg_data) in enumerate(tqdm(cpg_df.iterrows(), total=cpg_df.shape[0])):
        if count >= limit:
            break

        assoc_sql_id = cpg_data["Association ID"]
        article_sql_id = cpg_data["Article ID"]
        cpg_name = cpg_data["CpG"]
        cpg_sql_id = cpg_list.index(cpg_name)
        cpg_occurences = cpg_data["Occurrences"]
        cpg_has_direction = cpg_data["Has Direction"]
        cpg_direction = cpg_data["Direction"]
        cpg_has_baseline = cpg_data["Has Baseline"]
        cpg_mvalue_baseline = cpg_data["M-Value Baseline"]
        cpg_beta_baseline = cpg_data["Beta Baseline"]

        # Add cpg vertices:
        if not g.V().has("cpg", "name", cpg_name).hasNext():
            cpg_node: Vertex = (
                g.addV("cpg")
                .property("cpg ID", cpg_sql_id)
                .property("name", cpg_name)
                .property("occurrences", cpg_occurences)
                .property("has direction", cpg_has_direction)
                .property("direction", cpg_direction)
                .property("has baseline", cpg_has_baseline)
                .property("m-value baseline", cpg_mvalue_baseline)
                .property("beta baseline", cpg_beta_baseline)
                .next()
            )
            cpg_id_dict[cpg_sql_id] = cpg_node.id

        # Add edges between cpg and article vertices:
        # Getting incoming vertex article_node:
        article_node = g.V().has("article", "article ID", article_sql_id).next()
        # Adding an edge labeled 'analyzed by' between the cpg_node and the article_node
        g.V(cpg_node.id).addE("analyzed by").to(article_node).next()

        # Add edges between cpg and health factor (assoc) vertices:
        if cpg_direction is not None and isinstance(cpg_direction, str) and len(cpg_direction) > 0:
            methylation_effect = (
                "hypermethylate" if cpg_direction == "increase" else "hypomethylate"
            )
            assoc_node = g.V().has("health factor", "assoc ID", assoc_sql_id).next()
            # Adding an edge with conditional labels between the cpg_node and the assoc_node
            g.V(cpg_node.id).addE(methylation_effect).to(assoc_node).next()

    print("article ID dict:")
    print(article_id_dict)
    print("assoc ID dict:")
    print(assoc_id_dict)
    print("cpg ID dict:")
    print(cpg_id_dict)

    # testing edges:
    edges_cpg_to_article = (
        g.V(cpg_node.id)
        .outE("analyzed by")
        .where(__.inV().hasId(article_node.id))
        .toList()
    )
    print("getting edges between cpg_node and article_node:", edges_cpg_to_article)
    edges_cpg_to_assoc_hypo = (
        g.V(cpg_node.id)
        .outE("hypomethylate")
        .where(__.inV().hasId(assoc_node.id))
        .toList()
    )
    print("getting hypomethylate edges between cpg_node and assoc_node:", edges_cpg_to_assoc_hypo)


# %%
g.V().drop().iterate()  # DANGEROUS: drop the whole graph

# %%
add_data(g, article_df, assoc_df, cpg_df)

# %% Testing:
a = g.V().hasLabel("article").valueMap(True).toList()[0]
print(a)
print(a[T.id], a[T.label])

# b = g.V().hasLabel('"health factor"').valueMap(True).toList()[0]
# print(b)

# %%
g.V().hasLabel("cpg").order().by(__.inE("hypomethylate").count(), Order.desc).limit(
    10
).properties().to_list()

# g.V(1044891).bothE().bothV()


# %% Query for all CpGs analysed by a specific article
# (can be generalized to find by a specific property of either "article" or "health factor")
def get_cpg_by_article_doi(g: GraphTraversalSource, doi_value: str):
    results = (
        g.V()
        .has("article", "doi", doi_value)
        .bothE()
        .outV()
        .valueMap(True, "cpg ID", "name")  # This can be updated to include more properties as needed for the Data Access Portal
        .dedup()
        .toList()
    )

    for result in results:
        vertex_id = result.get(T.id)[0]  # Get the vertex ID
        cpg_id = result.get("cpg ID", [None])[
            0
        ]  # Get the value of 'cpg ID', or default to None if it's not available
        name = result.get("name", [None])[
            0
        ]  # Get the value of 'name', or default to None if it's not available
        print(f"graph id: {vertex_id}, cpg sql ID: {cpg_id}, Name: {name}")

    simplified_results = (
        g.V()
        .has("article", "doi", doi_value)
        .bothE()
        .outV()
        .valueMap("name")
        .dedup()
        .toList()
    )
    print(simplified_results)
    return simplified_results


# %% Test get_cpg_by_article_doi() function
doi_value = "10.1089/omi.2016.0041"
cpgs_analyzedby_articleID1 = get_cpg_by_article_doi(g, doi_value)
# print(type(cpgs_analyzedby_articleID1))


# %% Compare between graph DB result and csv
def compareGraphToCsv(
    csv_column1: str, csv_criteria: int, csv_column2: str, graph_result: List[str]
):
    cpg_values_from_graph = [item["name"][0] for item in graph_result]  # Extract from dicts
    print(">>", cpg_values_from_graph)

    cpg_values_from_csv = cpg_df.loc[cpg_df[csv_column1] == csv_criteria, csv_column2].tolist()
    print(">>", cpg_values_from_csv)

    # Convert lists to sets
    set1 = set(cpg_values_from_graph)
    set2 = set(cpg_values_from_csv)

    # Find the intersection of the two sets
    matches = set1 & set2

    # Get the number of matches and %
    num_matches = len(matches)
    match_percentage = num_matches / len(set2) * 100  # Multiplying by 100 to get the percentage

    print(num_matches)
    print(
        f"Match result: {match_percentage:.2f}%"
    )  # Formatting the percentage to two decimal places


# %% Test compare function:
compareGraphToCsv("Article ID", 1, "CpG", cpgs_analyzedby_articleID1)


# %% Query for CpGs associated with a health factor (phenotype?)
def get_cpg_by_health_factor(g: GraphTraversalSource, health_factor_name: str):
    results = (
        g.V()
        .has("health factor", "name", health_factor_name)
        .bothE()
        .outV()
        .valueMap(True, "cpg ID", "name")  # This can be updated to include more properties as needed for the Data Access Portal
        .dedup()
        .toList()
    )

    for result in results:
        vertex_id = result.get(T.id)[0]  # Get the vertex ID
        cpg_id = result.get("cpg ID", [None])[
            0
        ]  # Get the value of 'cpg ID', or default to None if it's not available
        name = result.get("name", [None])[
            0
        ]  # Get the value of 'name', or default to None if it's not available
        print(f"graph id: {vertex_id}, cpg sql ID: {cpg_id}, Name: {name}")

    simplified_results = (
        g.V()
        .has("health factor", "name", health_factor_name)
        .bothE()
        .outV()
        .valueMap("name")
        .dedup()
        .toList()
    )
    print(simplified_results)
    return simplified_results


# %% Test get_cpg_by_health_factor() function
health_factor_name = "sleep"
cpgs_hypo_sleep = get_cpg_by_health_factor(g, health_factor_name)


# %% Compare to csv:
compareGraphToCsv("Association ID", 1, "CpG", cpgs_hypo_sleep)


# %% Checking database for CpG vertices:
cpg_all_vertices = g.V().hasLabel('cpg').properties().toList()
print(cpg_all_vertices)
article_all_vertices = g.V().hasLabel('article').properties().toList()
print(article_all_vertices)


# %% Query for creating groups of CpGs based on user input:
# From the front end, user input will probably come in from a form submission (?)
# cpg_names_input = input("Which CpG to add to group?")  # CpG names will come as a LIST of strings from a dropdown search box?? But the entire list of CpGs is very long...
# # # cpg_names = [name.strip() for name in cpg_names_input.split(",")]  # This is just in case the input comes as a long string...
# cpg_group_name = input("Enter a name for this group of CpGs: ")  # User-provided CpG group name will come from an input field (str)


def create_cpg_group(
    g: GraphTraversalSource, cpg_names_input: List[str], cpg_group_name: str
) -> List[dict]:
    cpg_vertices_dict = {}
    # Get vertices of the requested CpGs and their property values based on the name property
    cpg_vertices = (
        g.V()
        .hasLabel('cpg')
        .has('name', P.within(*cpg_names_input))
        .valueMap()
        .toList()
    )  # If we want to save memory space and get specific properties instead of all properties, we can use the by modulator.
    # Store the retrieved vertices in the dictionary with the user-provided group name as the key
    cpg_vertices_dict[cpg_group_name] = cpg_vertices
    return cpg_vertices_dict


# %% Testing create_cpg_group with hard-coded input:
cpg_names_input_test = ["cg24115571", "cg26498966", "cg13181537", "cg01261464"]
cpg_group_name_test = "Nicole's Favorite Sleep-Related CpGs"
fav_sleep_cpgs = create_cpg_group(g, cpg_names_input_test, cpg_group_name_test)
print(fav_sleep_cpgs)


# %% Testing queries learned from book:
def testGroupStep(g: GraphTraversalSource):
    collection = (
        g.V()
        .hasLabel('article')
        .group().by('doi').by('article ID')
        .toList()
    )
    print(collection)


testGroupStep(g)


# %%
g.addV("bacteria").property(T.id, 9999).property("name", "E. coli").next()
g.V().has("health factor", "name", "diabetes").addE(9999, "increase risk")
# %%
