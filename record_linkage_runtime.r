library(RecordLinkage)
library(tictoc)

dataSet <- read.csv("data/clean_county.csv", header=TRUE)

linkageFields <- c("first_name", "middle_name", "last_name", "res_street_address", "zip_code")
dataSet <- dataSet[linkageFields]

x <- c(5000)

for (size in x) {
  sampleSize <- size
  sampleSize <- as.integer(sampleSize) * 1.5
  sampleCutA <- sampleSize / 3

  sampleSet <- dataSet
  sampleSet <- sampleSet[sample(nrow(sampleSet), sampleSize), ]

  columns <- colnames(sampleSet)
  for (name in columns[1:4]) {
    sampleSet[[name]][sample(nrow(sampleSet), sampleSize * 0.1)] <- NA
  }

  dfAFirstHalf <- sampleSet[1:sampleCutA, ]
  dfBFirstHalf <- sampleSet[1:sampleCutA, ]

  dfASecondHalf <- sampleSet[(sampleCutA + 1):(2 * sampleCutA), ]
  dfBSecondHalf <- tail(sampleSet, n = sampleCutA)

  dfA <- rbind(dfAFirstHalf, dfASecondHalf)
  dfB <- rbind(dfBFirstHalf, dfBSecondHalf)

  tic("ReconrdLinkage")

  rPairsRL <- compare.linkage(dataset1 = dfA,
                              dataset2 = dfB,
                              blockfld = c(5),
                              strcmpfun = levenshteinDist)

  rPairsWeights <- emWeights(rpairs = rPairsRL,
                             cutoff = 0.95,
                             maxit = 100,
                             tol = 1e-4)

  rPairsClassify <- emClassify(rpairs = rPairsWeights,
                               threshold.upper = 11)

  print(paste("[",as.character(size),"]",  sep = ""))
  toc()
}
