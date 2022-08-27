library(fastLink)
library(tictoc)

linkageFields <- c("first_name", "middle_name", "last_name", "res_street_address")

dataSet <- read.csv("data/clean_county.csv", header=TRUE)
dataSet <- dataSet[linkageFields]

x <- c(5000)

for (size in x) {
  sampleSize <- size
  sampleSize <- as.integer(sampleSize) * 1.5
  sampleCutA <- sampleSize / 3

  sampleSet <- dataSet
  sampleSet <- sampleSet[sample(nrow(sampleSet), sampleSize), ]

  for (name in colnames(sampleSet)) {
    sampleSet[[name]][sample(nrow(sampleSet), sampleSize * 0.1)] <- NA
  }

  dfAFirstHalf <- sampleSet[1:sampleCutA, ]
  dfBFirstHalf <- sampleSet[1:sampleCutA, ]

  dfASecondHalf <- sampleSet[(sampleCutA + 1):(2 * sampleCutA), ]
  dfBSecondHalf <- tail(sampleSet, n = sampleCutA)

  dfA <- rbind(dfAFirstHalf, dfASecondHalf)
  dfB <- rbind(dfBFirstHalf, dfBSecondHalf)

  tic("FastLink")

  rPairsFL <- fastLink(dfA = dfA,
                       dfB = dfB,
                       varnames = linkageFields,
                       stringdist.match = linkageFields,
                       stringdist.method = "lv",
                       n.cores = 6,
                       return.all = FALSE)

  print(summary(rPairsFL))
  print(paste("[",as.character(size),"]", " ---------------------------",  sep = ""))
  toc()
}
