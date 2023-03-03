from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import logging
import time
import pandas as pd
from  pathlib import Path
import os

# logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session", "comparison_level"]
# for log in logs:
#     logging.getLogger(log).setLevel(logging.ERROR)


root_folder = Path(__file__).parents[1]
df_folder = root_folder / "sample_df"
results_folder = root_folder / "results"

columns = ["id", "first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code"]

blocking_rules_for_prediction = [
        "l.first_name = r.first_name and l.last_name = r.last_name",
        "l.first_name = r.first_name and l.middle_name = r.middle_name",
        "l.res_street_address = r.res_street_address",
        "l.birth_year = r.birth_year and l.middle_name = r.middle_name",
        "l.birth_year = r.birth_year and l.last_name = r.last_name"
]

settings = {
    "link_type": "link_only",
    "unique_id_column_name": "id",
    "comparisons": [
        cl.jaro_winkler_at_thresholds(col_name="first_name", distance_threshold_or_thresholds=0.9, term_frequency_adjustments=True),
        cl.jaro_winkler_at_thresholds(col_name="last_name", distance_threshold_or_thresholds=0.9, term_frequency_adjustments=True),
        cl.jaro_winkler_at_thresholds(col_name="middle_name", distance_threshold_or_thresholds=0.9, term_frequency_adjustments=True),
        cl.levenshtein_at_thresholds(col_name="res_street_address", distance_threshold_or_thresholds=[1,3,5], term_frequency_adjustments=False),
        cl.levenshtein_at_thresholds(col_name="birth_year", distance_threshold_or_thresholds=1, term_frequency_adjustments=True)
    ],
    #Blocking used here
    "blocking_rules_to_generate_predictions": blocking_rules_for_prediction,
    "retain_intermediate_calculation_columns": False,
    "retain_matching_columns": False

}

x = [2000,4000,6000,8000,10000,12000,14000,16000,18000,20000,22000,24000,26000,28000,30000,32000,34000,36000,38000,40000]
x = [40000]
for size in x:

    dfA = pd.read_csv(os.path.join(df_folder, str(size) + "_dfA.csv"), names = columns)
    dfB = pd.read_csv(os.path.join(df_folder, str(size) + "_dfB.csv"), names = columns)

    time_start = time.time()

    linker = DuckDBLinker([dfA, dfB], settings)

    linker.estimate_probability_two_random_records_match(["l.first_name = r.first_name and l.last_name = r.last_name and l.birth_year = r.birth_year",
                                                          "l.birth_year = r.birth_year and l.res_street_address = r.res_street_address"], recall=0.8)
    linker.estimate_u_using_random_sampling(target_rows=1e6)

    training = [
        "l.first_name = r.first_name and l.last_name = r.last_name",
        "l.res_street_address = r.res_street_address"
    ]

    for i in training:
        linker.estimate_parameters_using_expectation_maximisation(i)
    predict = linker.predict(0.85) #Has None as a dafault value, thus 0.95 was needed for any analysis

    time_end = time.time()

    df_predict = predict.as_pandas_dataframe()
    pairs_data = linker.cumulative_comparisons_from_blocking_rules_records(blocking_rules_for_prediction)
    pairs = sum([r['row_count'] for r in pairs_data])

    false_positive = len(df_predict.loc[df_predict["id_l"] != df_predict["id_r"]])
    true_positive = len(df_predict.loc[df_predict["id_l"] == df_predict["id_r"]])
    false_negative = round(size / 2) - true_positive

    precision = true_positive / (true_positive + false_positive)
    recall = true_positive / (true_positive + false_negative)

    with open(os.path.join(results_folder, "splink_outputs_complex_model.txt"), "a") as f:
        f.writelines(
            "Sample Size: " + str(size) +
            "|Links Predicted: " + str(len(df_predict)) +
            "|Time Taken: " + str(round((time_end - time_start),2)) +
            "|Precision: " + str(precision) +
            "|Recall: " + str(recall) +
            "|Linkage Pairs: " + str(pairs) +
            "\n"
        )

# A few diagnostics
# linker.match_weights_chart()

# false_positives = linker.prediction_errors_from_labels_column("id", include_false_negatives=False, include_false_positives=True).as_pandas_dataframe(limit=10)
# false_negatives = linker.prediction_errors_from_labels_column("id", include_false_negatives=True, include_false_positives=False).as_pandas_dataframe(limit=10)

# Note for this to work you need to set retain_intermediate_calculation_columns and retain_matching_columns to True
# linker.waterfall_chart(false_positives.to_records())
# linker.waterfall_chart(false_negatives.to_records())