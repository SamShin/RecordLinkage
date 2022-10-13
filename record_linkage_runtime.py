import recordlinkage
from recordlinkage.index import Block
import time
import pandas as pd

recordlinkage.logging.set_verbosity(recordlinkage.logging.ERROR)

columns = ["first_name", "middle_name", "last_name", "res_street_address", "zip_code"]
missing_percent = [0.1, 0.1, 0.1, 0.1, 0.0]

data = pd.read_csv("data/clean_county.csv", low_memory=False)
df = pd.DataFrame(data)
df = df[columns]

x = [5000]
for size in x:
    sample_size = round(size * 1.5)
    sample_set = df.sample(sample_size)
    cut = round(sample_size / 3)

    for i, col in enumerate(df.columns):
        sample_set.loc[sample_set.sample(frac=missing_percent[i]).index, col] = None

    dfA_first = sample_set[0:cut]
    dfB_first = sample_set[0:cut]

    dfA_last = sample_set[(cut):(2 * cut)]
    dfB_last = sample_set.tail(cut)

    frame_a = [dfA_first, dfA_last]
    frame_b = [dfB_first, dfB_last]

    dfA = pd.concat(frame_a)
    dfB = pd.concat(frame_b)

    time_start = time.time()

    indexer = recordlinkage.Index()
    indexer.add(Block("zip_code", "zip_code"))

    pairs = indexer.index(dfA, dfB)

    compare_cl = recordlinkage.Compare(n_jobs=-1)
    compare_cl.string(left_on="last_name", right_on="last_name", method="levenshtein", threshold=1, label="last_name")
    compare_cl.string(left_on="first_name", right_on="first_name", method="levenshtein", threshold=1, label="first_name")
    compare_cl.string(left_on="middle_name", right_on="middle_name", method="levenshtein", threshold=1, label="middle_name")
    compare_cl.string(left_on="res_street_address", right_on="res_street_address", method="levenshtein", threshold=1, label="res_street_address")
    features = compare_cl.compute(pairs, dfA, dfB)

    cl = recordlinkage.ECMClassifier(init='jaro', binarize=None, max_iter=100, atol=1e-4, use_col_names=True)
    cl.fit(features)

    links_pred = cl.predict(features)

    time_end = time.time()

    print("[" + str(size) + "] " + str(len(links_pred)) + " links" +
          " | " + str(round((time_end - time_start),3)) + " seconds")

