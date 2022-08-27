library(RecordLinkage)
library(tictoc)

dataSet <- read.table("county.txt", header = TRUE)

linkageFields <- c("first_name", "middle_name", "last_name")
dataSet <- dataSet[linkageFields]

x <- c(100,1000)

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
  
  tic("ReconrdLinkage")
  
  rPairsRL <- compare.linkage(dfA,
                              dfB,
                              blockfld = FALSE,
                              phonetic = FALSE,
                              strcmpfun = jarowinkler)
  
  rPairsWeights <- emWeights(rPairsRL)
  rPairsClassify <- emClassify(rPairsWeights, 11)
  
  print(paste("[",as.character(size),"]",  sep = ""))
  toc()
}
