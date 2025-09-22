import argparse
from pathlib import Path

import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('output', type=lambda s: Path(s))
parser.add_argument('files', nargs=argparse.REMAINDER, type=lambda s: Path(s))
cfg = parser.parse_args()

dataframes = []
for file in cfg.files:
    data = pd.read_csv(file)
    dataframes.append(data)

data = pd.concat(dataframes).reset_index(drop=True)


with open(cfg.output, 'w') as fh:
    data.to_csv(fh, index=False)
