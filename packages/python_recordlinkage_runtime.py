import recordlinkage
from recordlinkage.index import Block
import time
import pandas as pd
from pathlib import Path
import os

recordlinkage.logging.set_verbosity(recordlinkage.logging.ERROR)


root_folder = Path(__file__).parents[1]
df_folder = root_folder / "sample_df"
results_folder = root_folder / "results"

columns = ["first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code"]

x = [2000,4000,6000,8000,10000,12000,14000,16000,18000,20000,22000,24000,26000,28000,30000,32000,34000,36000,38000,40000]
for size in x:

    dfA = pd.read_csv(os.path.join(df_folder, str(size) + "_dfA.csv"), names=columns)
    dfB = pd.read_csv(os.path.join(df_folder, str(size) + "_dfB.csv"), names=columns)

    time_start = time.time()

    indexer = recordlinkage.Index()
    indexer.add(Block("zip_code", "zip_code")) #Blocking used here

    pairs = indexer.index(dfA, dfB)

    compare_cl = recordlinkage.Compare()
    compare_cl.string(left_on="first_name", right_on="first_name", method="levenshtein", threshold=1, label="first_name")
    compare_cl.string(left_on="middle_name", right_on="middle_name", method="levenshtein", threshold=1, label="middle_name")
    compare_cl.string(left_on="last_name", right_on="last_name", method="levenshtein", threshold=1, label="last_name")
    compare_cl.string(left_on="res_street_address", right_on="res_street_address", method="levenshtein", threshold=1, label="res_street_address")
    compare_cl.exact(left_on="birth_year", right_on="birth_year")
    features = compare_cl.compute(pairs, dfA, dfB)

    cl = recordlinkage.ECMClassifier()
    cl.fit(features)
    links_pred = cl.predict(features)

    time_end = time.time()

    dfA_links = dfA.index.tolist()[0 : (size // 2)]
    dfB_links = dfB.index.tolist()[0 : (size // 2)]
    links_true = pd.MultiIndex.from_arrays([dfA_links,dfB_links])

    with open(os.path.join(results_folder, "python_recordlinkage.txt"), "a") as f:
        f.writelines(
            "Sample Size: " + str(size) +
            "|Links Predicted: " + str(len(links_pred)) +
            "|Time Taken: " + str(round(time_end - time_start, 2)) +
            "|Precision: " + str(recordlinkage.precision(links_true, links_pred)) +
            "|Recall: " + str(recordlinkage.recall(links_true, links_pred)) +
            "|Linkage Pairs: " + str(len(pairs)) +
            "\n")

    time.sleep(600)

