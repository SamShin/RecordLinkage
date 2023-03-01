library("fastLink")
library("dplyr")

linkageFields <- c("first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code")


dfA <- read.csv("sample_df/test.csv", na.strings = c("", "NA"))

fl_out_dedupe <- fastLink(
  dfA = dfA, dfB = dfA,
  varnames = linkageFields)

dfA_dedupe <- getMatches(dfA = dfA, dfB = dfA, fl.out = fl_out_dedupe)
