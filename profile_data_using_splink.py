from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import logging
import time
import pandas as pd
from  pathlib import Path
import os

logs = ["splink.estimate_u", "splink.expectation_maximisation", "splink.settings", "splink.em_training_session", "comparison_level"]
for log in logs:
    logging.getLogger(log).setLevel(logging.ERROR)


root_folder = Path(__file__).parents[0]

df_folder = root_folder / "sample_df"
results_folder = root_folder / "results"

columns = ["id", "first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code"]

settings = {
    "link_type": "link_only",
    "unique_id_column_name": "id",

}


size = 40000


dfA = pd.read_csv(os.path.join(df_folder, str(size) + "_dfA.csv"), names = columns)
dfB = pd.read_csv(os.path.join(df_folder, str(size) + "_dfB.csv"), names = columns)

linker = DuckDBLinker([dfA, dfB], settings)
linker.profile_columns(list(dfA.columns))

linker.profile_columns(["first_name||middle_name"])
linker.missingness_chart()


# Looking at cardinality blocking on res_street_address and

linker.count_num_comparisons_from_blocking_rule("l.first_name = r.first_name and l.last_name = r.last_name")
linker.count_num_comparisons_from_blocking_rule("l.first_name = r.first_name and l.middle_name = r.middle_name")
linker.count_num_comparisons_from_blocking_rule("l.res_street_address = r.res_street_address")
linker.count_num_comparisons_from_blocking_rule("l.birth_year = r.birth_year and l.middle_name = r.middle_name")
linker.count_num_comparisons_from_blocking_rule("l.birth_year = r.birth_year and l.last_name = r.last_name")


# This takes ages to compute because there are over a billion! = 1,109,496,951
linker.count_num_comparisons_from_blocking_rule("l.zip_code = r.zip_code")
