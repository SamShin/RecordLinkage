from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import logging
import time
import pandas as pd

#TODO: Should I still remove logs?
logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session", "comparison_level"]
for log in logs:
    logging.getLogger(log).setLevel(logging.ERROR)

columns = ["first_name", "middle_name", "last_name", "res_street_address", "zip_code"]
missing_percent = [0.1, 0.1, 0.1, 0.1, 0.0, 0.0]

data = pd.read_csv("data/clean_county.csv", low_memory=False)
df = pd.DataFrame(data)
df = df[columns]

unique_id = df.copy(deep=True)
unique_id = unique_id.groupby(columns).ngroup()
df['unique_id'] = unique_id

settings = {
    "link_type": "link_only",
    "comparisons": [
        cl.levenshtein_at_thresholds(col_name="first_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="last_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="middle_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="res_street_address", distance_threshold_or_thresholds=1, include_exact_match_level=False)
    ],
    "blocking_rules_to_generate_predictions": [
       "l.zip_code = r.zip_code"
    ],
    "retain_matching_columns": False,
    "max_iterations": 100,
    "em_convergence": 1e-4
}

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

    linker = DuckDBLinker([dfA, dfB], settings)
    linker.estimate_u_using_random_sampling(target_rows=1e6)
    training = ["l.first_name = r.first_name",
                "l.middle_name = r.middle_name",
                "l.last_name = r.last_name",
                "l.res_street_address = r.res_street_address"
                ]

    for i in training:
        linker.estimate_parameters_using_expectation_maximisation(i)

    df_predict = linker.predict(threshold_match_probability=0.95)
    output = df_predict.as_record_dict()

    time_end = time.time()

    print("[" + str(size) + "] " + str(len(output)) + " links" +
          " | " + str(round((time_end - time_start), 3)) + " seconds")