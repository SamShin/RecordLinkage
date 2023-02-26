from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import logging
import time
import pandas as pd
from pathlib import Path
import os
from typing import Union

import recordlinkage
from recordlinkage.index import Block


class Packages:
    def __init__(self):
        pass

    def python_splink(
            self,
            sample_size: Union[int, list],
            linkage_field: list,
            block: bool,
            block_field: str,
            output_file: str,
    ) -> None:
        # Logs to keep quite
        logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings",
                "splink.em_training_session",
                "comparison_level"]
        for log in logs:
            logging.getLogger(log).setLevel(logging.WARNING)

        root_folder = Path(__file__).parents[1]
        df_folder = root_folder / "sample_df"
        results_folder = root_folder / "results"

        comparison = []
        # TODO: See if we should add selector for different methods(jaro, lv) and other parameters
        for i, col in enumerate(linkage_field):
            comparison.append(cl.levenshtein_at_thresholds(col_name=col, distance_threshold_or_thresholds=1,
                                                           include_exact_match_level=False))

        # TODO: Should other option like link_type and unique_id be parameters?
        settings = {
            "link_type": "link_only",
            "unique_id_column_name": "id",
            "comparisons": comparison
        }

        training = []
        for col in linkage_field:
            training.append("l." + col + " = r." + col)

        linkage_field.insert(0, "id")

        if block:
            linkage_field.append(block_field)
            settings["blocking_rules_to_generate_predictions"] = ["l." + block_field + " = r." + block_field]

        if isinstance(sample_size, int):
            x = [sample_size]
        else:
            x = sample_size

        for size in x:
            dfA = pd.read_csv(os.path.join(df_folder, str(size) + "_dfA.csv"), names=linkage_field)
            dfB = pd.read_csv(os.path.join(df_folder, str(size) + "_dfB.csv"), names=linkage_field)

            time_start = time.time()

            linker = DuckDBLinker([dfA, dfB], settings)
            linker.estimate_u_using_random_sampling(target_rows=1000000)

            for i in training:
                linker.estimate_parameters_using_expectation_maximisation(i)
            predict = linker.predict(0.95)

            time_end = time.time()

            df_predict = predict.as_pandas_dataframe()

            if block:
                pairs = linker.count_num_comparisons_from_blocking_rule("l." + block_field + " = r." + block_field)
            else:
                pairs = size * size

            # TODO: What if we have a different unique column?
            false_positive = len(df_predict.loc[df_predict["id_l"] != df_predict["id_r"]])
            true_positive = len(df_predict.loc[df_predict["id_l"] == df_predict["id_r"]])
            false_negative = round(size / 2) - true_positive

            precision = true_positive / (true_positive + false_positive)
            recall = true_positive / (true_positive + false_negative)

            with open(os.path.join(results_folder, output_file), "a") as f:
                f.writelines(
                    "Sample Size: " + str(size) +
                    "|Links Predicted: " + str(len(df_predict)) +
                    "|Time Taken: " + str(round((time_end - time_start), 2)) +
                    "|Precision: " + str(precision) +
                    "|Recall: " + str(recall) +
                    "|Linkage Pairs: " + str(pairs) +
                    "\n"
                )

    def python_recordlinkage(
            self,
            sample_size: Union[int, list],
            linkage_field: list,
            block: bool,
            block_field: str,
            output_file: str,
    ) -> None:
        # Keep logs quiet
        recordlinkage.logging.set_verbosity(recordlinkage.logging.ERROR)

        root_folder = Path(__file__).parents[1]
        df_folder = root_folder / "sample_df"
        results_folder = root_folder / "results"

        if block:
            linkage_field.append(block_field)

        if isinstance(sample_size, int):
            x = [sample_size]
        else:
            x = sample_size

        for size in x:
            dfA = pd.read_csv(os.path.join(df_folder, str(size) + "_dfA.csv"), names=linkage_field)
            dfB = pd.read_csv(os.path.join(df_folder, str(size) + "_dfB.csv"), names=linkage_field)

            time_start = time.time()

            indexer = recordlinkage.Index()
            compare_cl = recordlinkage.Compare()

            if block:
                indexer.add(Block(block_field, block_field))

                for i in linkage_field[:-1]:  # Should there be a selector for the method and threshold?
                    # TODO: What about birth_year which should be exact instead of string comparison?
                    compare_cl.string(left_on=i, right_on=i, method="levenshtein", threshold=1, label=i)

            else:
                indexer.full()

                for i in linkage_field:
                    compare_cl.string(left_on=i, right_on=i, method="levenshtein", threshold=1, label=i)

            pairs = indexer.index(dfA, dfB)
            features = compare_cl.compute(pairs, dfA, dfB)

            classify = recordlinkage.ECMClassifier()
            classify.fit(features)
            links_pred = classify.predict(features)

            time_end = time.time()

            dfA_links = dfA.index.tolist()[0:(size // 2)]
            dfB_links = dfB.index.tolist()[0:(size // 2)]
            links_true = pd.MultiIndex.from_arrays([dfA_links, dfB_links])

            with open(os.path.join(results_folder, output_file), "a") as f:
                f.writelines(
                    "Sample Size: " + str(size) +
                    "|Links Predicted: " + str(len(links_pred)) +
                    "|Time Taken: " + str(round(time_end - time_start, 2)) +
                    "|Precision: " + str(recordlinkage.precision(links_true, links_pred)) +
                    "|Recall: " + str(recordlinkage.recall(links_true, links_pred)) +
                    "|Linkage Pairs: " + str(len(pairs)) +
                    "\n")
