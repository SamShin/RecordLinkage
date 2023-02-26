library("RecordLinkage")
library("dplyr")
library("fastLink")

r_recordlinkage <- function(sampleSize,
                           linkageField,
                           block,
                           blockName,
                           id,
                           outputFile) {

  myOptions <- options(digits.secs = 2)

  if (is.integer(sampleSize)) {
    sampleSize <- c(sampleSize)
  }

  for (size in sampleSize) {
    dfAName <- file.path("sample_df", paste(size, "_dfA.csv", sep = ""))
    dfBName <- file.path("sample_df", paste(size, "_dfB.csv", sep = ""))

    if (!block) {
      blockField <- FALSE
    } else {
      blockField <- blockName
    }

    linkageField <- append(linkageField, blockField)
    linkageField <- append(linkageField, id)

    dfA <- read.csv(dfAName, header = TRUE, na.strings = c("", "NA"))
    dfB <- read.csv(dfBName, header = TRUE, na.strings = c("", "NA"))

    linkageField <- unlist(linkageField)
    dfA <- dfA[linkageField]
    dfB <- dfB[linkageField]

    timeStart <- Sys.time()


    rPairsRL <- compare.linkage(dataset1 = dfA,
                                dataset2 = dfB,
                                blockfld = head(tail(linkageField,2),1),
                                strcmpfun = levenshteinDist,
                                identity1 = dfA[[idName]],
                                identity2 = dfB[[idName]])

    rPairsWeights <- emWeights(rpairs = rPairsRL)

    rPairsClassify <- emClassify(rpairs = rPairsWeights,
                                 threshold.upper = 11)

    timeEnd <- Sys.time()

    linkagePairs <- nrow(rPairsRL[[3]])

    pred <- rPairsClassify[["prediction"]]
    predDf <- data.frame(pred)

    levels <- dplyr::count(predDf, pred)
    linksPredicted <- levels[levels$pred == "L", ][["n"]]

    timeTaken <- as.character(difftime(timeEnd, timeStart, units = "secs")[[1]])

    falsePositive <- 0L
    pairs <- rPairsClassify[["pairs"]]

    for (i in 1:nrow(predDf)) {
      if (predDf[i, ] == "L") {
        match <- pairs[i, ]

        idA <- match["id1"]
        idB <- match["id2"]

        if (idA != idB) {
          falsePositive <- falsePositive + 1
        }
      }
    }

    truePositive <- linksPredicted - falsePositive
    falseNegative <- (size / 2) - truePositive

    precision <- truePositive / (truePositive + falsePositive)
    recall <- truePositive / (truePositive + falseNegative)

    output <- paste("Sample Size: ", size,
                    "|Links Predicted: ", linksPredicted,
                    "|Time Taken: ", timeTaken,
                    "|Precision: ", precision,
                    "|Recall: ", recall,
                    "|Linkage Pairs: ", linkagePairs, sep = "")

    write(output, file = file.path("results", outputFile), append = TRUE)
  }
}


r_fastlink <- function(sampleSize,
                       linkageField,
                       block,
                       blockName,
                       outputFile) {

  myOptions <- options(digits.secs = 2)

  if (is.integer(sampleSize)) {
    sampleSize <- c(sampleSize)
  }


  for (size in sampleSize) {
    dfAName <- file.path("sample_df", paste(size, "_dfA.csv", sep = ""))
    dfBName <- file.path("sample_df", paste(size, "_dfB.csv", sep = ""))

    linkageField <- append(linkageField, blockName)
    linkageField <- unlist(linkageField)

    dfA <- read.csv(dfAName, header = TRUE, na.string = c("", "NA"))
    dfB <- read.csv(dfBName, header = TRUE, na.string = c("", "NA"))

    dfA <- dfA[linkageField]
    dfB <- dfB[linkageField]

    timeStart <- Sys.time()

    if (!block) {
      blockOut <- 1
    } else {
      blockOut <- blockData(dfA, dfB, varnames = blockName)
    }
    fOut <- vector(mode = "list", length = length(blockOut))


    for (i in 1:length(blockOut)) {
      if (block == FALSE) {
        sub1 <- dfA
        sub2 <- dfB
      } else {
        sub1 <- dfA[blockOut[[i]]$dfA.inds, ]
        sub2 <- dfB[blockOut[[i]]$dfB.inds, ]
      }

      print(head(linkageField), 5)
      fOut[[i]] <- fastLink(
        dfA = sub1,
        dfB = sub2,
        varnames = c(head(linkageField, 5)),
        #varnames = c("first_name", "middle_name", "last_name", "res_street_address", "birth_year"),
        stringdist.match = c(head(linkageField, 5)),
        #stringdist.match = c("first_name", "middle_name", "last_name", "res_street_address", "birth_year"),
        stringdist.method = "lv",
        return.all = TRUE,
        return.df = TRUE) #maybe this can be false?
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

        if (!is.null(currentLinks)) { #Maybe unnecessary
          links <- (links + currentLinks)
        }
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

    write(output, file = file.path("results", outputFile), append = TRUE)
  }
}