from dataclasses import dataclass, field

import pandas as pd



@dataclass
class LouvainJob:
    id: str
    type = "louvain"
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    name: str = ""
    col: str = "IDs"
    sep: str = ";"
    width: int = 25
    height: int = 25
    outer_scale: float = 14
    inner_scale: float = 7
    node_size: int = 750
    cmap: str = 'Accent'
    node_alpha: float = 1.0
    edge_alpha: float = 0.5
    label_size: int = 10
    img_format: str = 'png'
    container: str = "louvain"



