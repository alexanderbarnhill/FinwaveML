import concurrent
import io
import os
import logging as log
import threading
import concurrent.futures
from datetime import datetime

import networkx as nx
from itertools import combinations
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from community import community_louvain
from matplotlib.patches import FancyArrowPatch
from multipart import file_path

from jobs.models.louvain import LouvainJob
from utilities.data_loading import load_data
from utilities.persistence.results.azure.image_storage import blob_api
from utilities.plotting import Plt2Pil

from utilities.messaging.azure.pub_sub_client import PubSubClient

def do_persist_analysis(job: LouvainJob):
    G, pos, modularity_communities = do_louvain_analysis(job.df, job.name, job.col, job.sep, job.outer_scale, job.inner_scale)
    log.info(f"Analysis complete. Attempting to persist")
    container, path = plot_louvain_analysis(G, pos, modularity_communities, job.name, job.width, job.height, job.node_size, job.cmap, job.node_alpha, job.edge_alpha, job.label_size, job.img_format, job.container)
    client = PubSubClient()
    client.send_message({
        "type": "louvain",
        "subject": "louvain_analysis_complete",
        "status": "Finished",
        "results": {
            "errors": [],
            "container": container,
            "file_path": path,
        },
        "id": job.id
    })




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
                          img_format='png',
                          container="louvain"
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
    img_buffer = io.BytesIO()
    fig.savefig(img_buffer, format=img_format, bbox_inches='tight', pad_inches=0, transparent=True if img_format.lower() == 'png' else False)
    img_buffer.seek(0)  # Move to the beginning of the BytesIO buffer
    blob_name = f"{datetime.strftime(datetime.now(), '%Y-%m-%d_%H-%M-%S')}_{name}_louvain.{img_format}"
    # Upload the image to Azure Blob storage
    blob_api.upload_blob(container, blob_name, img_buffer)
    img_buffer.close()
    return container, blob_name

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

