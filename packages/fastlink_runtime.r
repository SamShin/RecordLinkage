library("fastLink")
library("dplyr")

myOptions <- options(digits.secs = 2)
linkageFields <- c("first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code")

#x <- c(2000,4000,6000,8000,10000,12000,14000,16000,18000,20000,22000,24000,26000,28000,30000,32000,34000,36000,38000,40000)
x <- c(2000)
for (size in x) {
  dfAName <- file.path("sample_df", paste(size, "_dfA.csv", sep = ""))
  dfBName <- file.path("sample_df", paste(size, "_dfB.csv", sep = ""))

  dfA <- read.csv(dfAName, na.strings = c("", "NA"))
  dfB <- read.csv(dfBName, na.strings = c("", "NA"))

  timeStart <- Sys.time()

  blockOut <- blockData(dfA, dfB, varnames = c("zip_code")) #Blocking used here
  fOut <- vector(mode = "list", length = length(blockOut))

  dfA <- dfA[c("first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code")]
  dfB <- dfB[c("first_name", "middle_name", "last_name", "res_street_address", "birth_year", "zip_code")]

  for (i in 1:length(blockOut)) {
    sub1 <- dfA[blockOut[[i]]$dfA.inds, ]
    sub2 <- dfB[blockOut[[i]]$dfB.inds, ]

    fOut[[i]] <- fastLink(
      dfA = sub1,
      dfB = sub2,
      varnames = c("first_name", "middle_name", "last_name", "res_street_address", "birth_year"),
      stringdist.match = c("first_name", "middle_name", "last_name", "res_street_address", "birth_year"),
      stringdist.method = "lv",
      return.all = TRUE)
  }
  timeEnd <- Sys.time()

  links <- 0L
  truePositive <- 0L
  falsePositive <- 0L
  linksPair <- 0L
  timeTaken <- as.character(difftime(timeEnd, timeStart, units = "secs")[[1]])

  for (i in 1:length(fOut)) {
    matches <- fOut[[i]]

    indsA <- matches[["matches"]][["inds.a"]]
    indsB <- matches[["matches"]][["inds.b"]]

    if (length(indsA) != 0) {
      inds <- data.frame(indsA, indsB)
      truePositive <- truePositive + nrow(inds[inds$indsA == inds$indsB, ])
      falsePositive <- falsePositive + nrow(inds[inds$indsA != inds$indsB, ])
      currentLinks <- nrow(matches[["matches"]])

      links <- (links + currentLinks)

      blocks <- blockOut[[i]]
      pairsA <- length(blocks[["dfA.inds"]])
      pairsB <- length(blocks[["dfB.inds"]])

      linksPair <- linksPair + (pairsA * pairsB)
    }
  }

  falseNegative <- round(size / 2) - truePositive

  precision <- truePositive / (truePositive + falsePositive)
  recall <- truePositive / (truePositive + falseNegative)

  output <- paste("Sample Size: ", size,
                  "|Links Predicted: ", links,
                  "|Time Taken: ", timeTaken,
                  "|Precision: ", precision,
                  "|Recall: ", recall,
                  "|Linkage Pairs: ", linksPair, sep = "")

  write(output, file = "results/fastlink.txt", append = TRUE)

  Sys.sleep(600)
}

