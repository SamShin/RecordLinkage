from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import logging
import time
import rl_helper

logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session"]
for log in logs:
    logging.getLogger(log).setLevel(logging.ERROR)

rl = rl_helper.Data(file_name = "county.csv", columns=["first_name", "middle_name", "last_name", "res_street_address", "zip_code"], unique_col=True)

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
    dfA, dfB = rl.data_set(size, [0.1, 0.1, 0.1, 0.1, 0.0])

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