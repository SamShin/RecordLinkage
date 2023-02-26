import pandas as pd
import numpy as np
import os
from pathlib import Path


np.random.seed(999)


root_folder = Path(__file__).parent
data_folder = root_folder / "data"
df_folder = root_folder / "sample_df"

df = pd.read_csv(os.path.join(data_folder, "clean_county.csv"))
missing_percent = [0,0.1,0.1,0.1,0.1,0.1,0]

x = [2000,4000,6000,8000,10000,12000,14000,16000,18000,20000,22000,24000,26000,28000,30000,32000,34000,36000,38000,40000]
for size in x:
    sample_size = round(size * 1.5)
    print(sample_size)
    sample_set = df.sample(sample_size)
    cut = round(sample_size / 3)

    dfA_first = sample_set[0:cut]
    dfB_first = sample_set[0:cut]

    dfA_last = sample_set[(cut):(2 * cut)]
    dfB_last = sample_set.tail(cut)

    frame_a = [dfA_first, dfA_last]
    frame_b = [dfB_first, dfB_last]

    dfA = pd.concat(frame_a)
    dfB = pd.concat(frame_b)

    for sample in [dfA,dfB]:
        for i, col in enumerate(sample.columns):
            sample.loc[sample.sample(frac=missing_percent[i]).index, col] = None

    dfA.to_csv(os.path.join(df_folder, str(size) + "_dfA.csv"), index=False)
    dfB.to_csv(os.path.join(df_folder, str(size) + "_dfB.csv"), index=False)