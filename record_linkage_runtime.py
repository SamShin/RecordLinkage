import recordlinkage
from recordlinkage.index import Block
import time
import rl_helper

recordlinkage.logging.set_verbosity(recordlinkage.logging.ERROR)

rl = rl_helper.Data(file_name= "county.csv", columns=["first_name", "middle_name", "last_name", "res_street_address", "zip_code"])

x = [5000]
for size in x:
    dfA, dfB = rl.data_set(size, [0.1, 0.1, 0.1, 0.1, 0.0])

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

