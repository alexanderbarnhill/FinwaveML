
import pandas as pd

from errors.file_errors import FileError


def load_data(file: str):
    if file.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            return df
        except Exception as e:
            return FileError(f"Could not load {file} as a csv")
    return ValueError(f"Cannot process {file}")