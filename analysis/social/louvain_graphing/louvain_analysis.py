import io
import os
import logging as log
import threading
import pandas as pd
import numpy as np
import networkx as nx
from itertools import combinations
import matplotlib.pyplot as plt
from community import community_louvain
from matplotlib.patches import FancyArrowPatch

from utilities.data_loading import load_data
from utilities.persistence.results.azure.image_storage import BlobApi
from utilities.plotting import Plt2Pil



def do_persist_analysis(df,
                        name=None,
                        col="IDs",
                        sep=";",
                        width=25,
                        height=25,
                        outer_scale=14,
                        inner_scale=7,
                        node_size=750,
                        cmap='Accent',
                        node_alpha=1.0,
                        edge_alpha=0.5,
                        label_size=10,
                        img_format='png'):
    G, pos, modularity_communities = do_louvain_analysis(df, name, col, sep, outer_scale, inner_scale)
    log.info(f"Analysis complete. Attempting to persist")
    plot_thread = threading.Thread(target=plot_louvain_analysis, args=(
        G, pos, modularity_communities, name, width, height, node_size, cmap, node_alpha, edge_alpha, label_size, img_format
    ))
    plot_thread.start()




def plot_louvain_analysis(G,
                          pos,
                          modularity_communities,
                          name,
                          width=25,
                          height=25,
                          node_size=750,
                          cmap='Accent',
                          node_alpha=1.0,
                          edge_alpha=0.5,
                          label_size=10,
                          img_format='png'
                          ):
    fig = plt.figure(figsize=(width, height))
    nx.draw_networkx_nodes(
        G, pos, node_size=node_size,
        node_color=[modularity_communities[n] for n in G.nodes()],
        cmap=plt.get_cmap(cmap), alpha=node_alpha
    )
    ax = plt.gca()
    G = nx.Graph(G)
    log.info("Number of edges: %d", len(G.edges()))
    for idx, edge in enumerate(G.edges()):
        log.info("Processing edge %d of %d", idx, len(G.edges()))
        src, dst = edge
        patch = FancyArrowPatch(
            pos[src], pos[dst],
            connectionstyle="arc3,rad=0.2",
            color="gray", alpha=edge_alpha, lw=1,
            arrowstyle="-",
            mutation_scale=10
        )
        ax.add_patch(patch)
    nx.draw_networkx_labels(G, pos, font_size=label_size)
    storage_client = BlobApi()
    img_buffer = io.BytesIO()
    fig.savefig(img_buffer, format=img_format, bbox_inches='tight', pad_inches=0, transparent=True if img_format.lower() == 'png' else False)
    img_buffer.seek(0)  # Move to the beginning of the BytesIO buffer

    # Upload the image to Azure Blob storage
    storage_client.upload_blob("louvain", f"{name}_louvain.{img_format}", img_buffer)
    img_buffer.close()

def build_database(df, col="KW IDs", sep=" "):
    data = {}
    col_idx = df.columns.get_loc(col) + 1
    for row in df.itertuples():
        ids = row[col_idx]
        ids = ids.split(sep)
        ids = list(set([f.replace("SGB", "") for f in ids if "?" not in f and len(f) > 0]))
        data[f"Group {row.Index}"] = ids
    largest_group = len(max(data.values(), key=lambda x: len(x)))
    for key, value in data.items():
        if len(value) < largest_group:
            value += [np.nan] * (largest_group - len(value))
    return pd.DataFrame(data)

def do_louvain_analysis(df, name=None, col="IDs", sep=";", outer_scale=14.0, inner_scale=7.0):
    df = build_database(df, col=col, sep=sep)
    edge_list = []
    df = df.transpose()
    for _, row in df.iterrows():
        ids = row.dropna().values
        ids = [i.replace("SGB", "") for i in ids]
        edge_list.extend(combinations(ids, 2))
    edge_list = list(set(edge_list))
    G = nx.Graph()
    G.add_edges_from(edge_list)

    modularity_communities = community_louvain.best_partition(G)
    nx.set_node_attributes(G, modularity_communities, 'community')

    communities = {}
    for node, comm in modularity_communities.items():
        communities.setdefault(comm, []).append(node)

    community_graph = nx.Graph()
    community_graph.add_edges_from(
        [(com, com) for com in communities.keys()]
    )
    community_pos = nx.spring_layout(community_graph, seed=42, scale=outer_scale)

    pos = {}
    for comm, nodes in communities.items():
        subG = G.subgraph(nodes)
        sub_pos = nx.spring_layout(subG, seed=42, scale=inner_scale)
        for node, p in sub_pos.items():
            pos[node] = community_pos[comm] + p

    return G, pos, modularity_communities

