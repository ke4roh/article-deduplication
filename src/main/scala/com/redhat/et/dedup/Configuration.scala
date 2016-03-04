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
  }

  val likelihoodMode = new Subcommand("compute-likelihood") {
    val likelihoodFile = opt[String](required = true)
    val duplicateSets = opt[String](required = true)
    val binarize = opt[Boolean]()
    val tfidf = opt[Boolean]()
    val normalize = opt[Boolean]()
    mutuallyExclusive(tfidf, normalize)
  }
}
