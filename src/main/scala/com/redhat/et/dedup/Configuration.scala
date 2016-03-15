package com.redhat.et.dedup

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val workDir = opt[String](required = true)

  val importDataMode = new Subcommand("import-data") {
    val filterWords = opt[String](required = true)
    val replacementWords = opt[String](required = true)
    val articles = opt[String](required = true)
    val minWordCount = opt[Int]()
    val maxWordCount = opt[Int]()
    val windowSize = opt[Int](default = Some(1))
  }

  val likelihoodMode = new Subcommand("compute-likelihood") {
    val binWidth = opt[Double](default = Some(0.05))
    val likelihoodFile = opt[String](required = true)
    val duplicateSets = opt[String](required = true)
    val binarize = opt[Boolean]()
    val tfidf = opt[Boolean]()
    val normalize = opt[Boolean]()
    val jaccard = opt[Boolean]()
    mutuallyExclusive(tfidf, normalize)
  }

  val rankingsMode = new Subcommand("rankings") {
    val rankingsFile = opt[String](required = true)
    val threshold = opt[Double](default = Some(0.2))
    val binarize = opt[Boolean]()
    val tfidf = opt[Boolean]()
    val normalize = opt[Boolean]()
    val jaccard = opt[Boolean]()
    mutuallyExclusive(tfidf, normalize)
  }

  val approxRankingsMode = new Subcommand("approx-rankings") {
    val rankingsFile = opt[String](required = true)
    val threshold = opt[Double](default = Some(0.2))
    val binarize = opt[Boolean]()
    val tfidf = opt[Boolean]()
    val normalize = opt[Boolean]()
    val jaccard = opt[Boolean]()
    mutuallyExclusive(tfidf, normalize)
  }

  val annMode = new Subcommand("ann") {
    val duplicateSets = opt[String](required = true)
    val binarize = opt[Boolean]()
    val tfidf = opt[Boolean]()
    val normalize = opt[Boolean]()
    val jaccard = opt[Boolean]()
    mutuallyExclusive(tfidf, normalize)    
  }
}
