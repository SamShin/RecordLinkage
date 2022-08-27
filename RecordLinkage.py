import recordlinkage
import pandas as pd
import time

recordlinkage.logging.set_verbosity(recordlinkage.logging.ERROR)

dataset = pd.read_table("county.txt", encoding="ISO-8859-1", low_memory=False)
df = pd.DataFrame(dataset)
df_name = df[["last_name", "first_name", "middle_name"]]

x = [100,1000]
for size in x:
    sample_size = round(size * 1.5)
    sample_set = df_name.sample(sample_size)
    cut_a = round(sample_size / 3)

    for col in df_name.columns:
        sample_set.loc[sample_set.sample(frac = 0.1).index, col] = None

    dfA_first_half = sample_set[0:cut_a]
    dfB_first_half = sample_set[0:cut_a]

    dfA_second_half = sample_set[(cut_a):(2*cut_a)]
    dfB_second_half = sample_set.tail(cut_a)

    frame_a = [dfA_first_half, dfA_second_half]
    frame_b = [dfB_first_half, dfB_second_half]

    dfA = pd.concat(frame_a)
    dfB = pd.concat(frame_b)

    time_start = time.time()


    indexer = recordlinkage.Index()
    indexer.full()
    pairs = indexer.index(dfA, dfB)

    compare_cl = recordlinkage.Compare()
    compare_cl.string("last_name", "last_name", method="jarowinkler", label="last_name")
    compare_cl.string("first_name", "first_name", method="jarowinkler", label="first_name")
    compare_cl.string("middle_name", "middle_name", method="jarowinkler", label="middle_name")
    features = compare_cl.compute(pairs, dfA, dfB)

    cl = recordlinkage.ECMClassifier(binarize = 0.95)
    cl.fit(features)

    links_pred = cl.predict(features)


    time_end = time.time()

    print("[" + str(size) + "] " + str(len(links_pred)) + " links" +
          " | " + str(round((time_end - time_start),3)) + " seconds")

