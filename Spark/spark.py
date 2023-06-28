from splink.spark.jar_location import similarity_jar_location

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types

import splink.spark.spark_comparison_library as cl
from splink.spark.spark_linker import SparkLinker

import logging
import time

conf = SparkConf()
conf.setAppName("Splink Spark")
conf.setMaster("UPDATE")
conf.set("spark.driver.memory", "12g")
conf.set("spark.default.parallelism", "16")
conf.set("spark.executor.memory", "30g")

path = similarity_jar_location()
conf.set("spark.jars", path)

sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession(sc)
spark.sparkContext.setCheckpointDir("./tmp_checkpoints")

spark.udf.registerJavaFunction(
   "jaro_winkler", "uk.gov.moj.dash.linkage.JaroWinklerSimilarity", types.DoubleType())


logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session", "comparison_level"]
for log in logs:
    logging.getLogger(log).setLevel(logging.ERROR)

settings = {
    "link_type": "link_only",
    "unique_id_column_name": "id",
    "comparisons": [
        cl.levenshtein_at_thresholds(col_name="first_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="last_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="middle_name", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="res_street_address", distance_threshold_or_thresholds=1, include_exact_match_level=False),
        cl.levenshtein_at_thresholds(col_name="birth_year", distance_threshold_or_thresholds=1, include_exact_match_level=False)
    ],
    #Blocking used here
    "blocking_rules_to_generate_predictions": [
       "l.zip_code = r.zip_code"
    ]
}

dfA = spark.read.csv("20000_dfA.csv", header = True)
dfB = spark.read.csv("20000_dfB.csv", header = True)

time_start = time.time()

linker = SparkLinker([dfA, dfB], settings)

linker.estimate_u_using_random_sampling(target_rows=1e6)
training = ["l.first_name = r.first_name",
            "l.middle_name = r.middle_name",
            "l.last_name = r.last_name",
            "l.res_street_address = r.res_street_address",
            "l.birth_year = r.birth_year"
            ]

for i in training:
    linker.estimate_parameters_using_expectation_maximisation(i)
predict = linker.predict(0.95) #Has None as a dafault value, thus 0.95 was needed for any analysis

time_end = time.time()

df_predict = predict.as_pandas_dataframe()
pairs = linker.count_num_comparisons_from_blocking_rule("l.zip_code = r.zip_code")

false_positive = len(df_predict.loc[df_predict["id_l"] != df_predict["id_r"]])
true_positive = len(df_predict.loc[df_predict["id_l"] == df_predict["id_r"]])
false_negative = round(20000 / 2) - true_positive

precision = true_positive / (true_positive + false_positive)
recall = true_positive / (true_positive + false_negative)

with open("spark.txt", "a") as f:
    f.writelines(
        "Sample Size: " + str(20000) +
        "|Links Predicted: " + str(len(df_predict)) +
        "|Time Taken: " + str(round((time_end - time_start),2)) +
        "|Precision: " + str(precision) +
        "|Recall: " + str(recall) +
        "|Linkage Pairs: " + str(pairs) +
        "\n"
    )
