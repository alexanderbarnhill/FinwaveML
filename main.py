import os

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
import logging as log
import sys
from analysis.social.louvain_graphing.louvain_analysis import do_louvain_analysis, plot_louvain_analysis, do_persist_analysis
from datetime import datetime
import pandas as pd
import io

from omegaconf import OmegaConf

from utilities.file import generate_random_string
from utilities.persistence.results.azure.image_storage import BlobApi

log.basicConfig(level=log.INFO, format="%(asctime)s : %(message)s")

logger = log.getLogger("azure")
logger.setLevel(log.ERROR)

# Set the logging level for the azure.storage.blob library
logger = log.getLogger("azure.storage.blob")
logger.setLevel(log.ERROR)

# Direct logging output to stdout. Without adding a handler,
# no logging output is visible.
handler = log.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

settings_file = os.path.join(os.getcwd(), "settings.yaml")
BlobApi(OmegaConf.load(settings_file))
app = FastAPI()

@app.get("/api")
def read_root():
    return {"message": "Hello, FastAPI!"}

@app.get("/api/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "query": q}


@app.get("/api/health")
def check_health():
    return {"health": "ok", "check": datetime.now()}

@app.post("/api/analysis/social/louvain")
async def do_louvain(
        file: UploadFile = File(...),
        name: str=None,
        col:str="IDs",
        sep: str=";",
        width: int=25,
        height: int=25,
        outer_scale: float=14,
        inner_scale: float=7,
        node_size: int=750,
        cmap: str='Accent',
        node_alpha: float=1.0,
        edge_alpha: float=0.5,
        label_size: int=10,
        img_format: str='png'
):
    name = name if name else generate_random_string(10)
    df = pd.read_csv(io.BytesIO(await file.read()))
    do_persist_analysis(df, name, col, sep, width, height, outer_scale, inner_scale,node_size,cmap,node_alpha,edge_alpha,label_size, img_format)
    log.info("Analysis complete")
    return {'success': True}
