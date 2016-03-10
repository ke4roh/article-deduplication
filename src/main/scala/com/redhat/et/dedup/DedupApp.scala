package com.redhat.et.dedup

import scala.collection.JavaConversions._
import scala.util.Try

import org.apache.log4j.{Level, Logger}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}

import org.apache.spark.mllib.linalg.{Vectors, SparseVector}

import com.redhat.et.dedup.FeatureMatrixSparkContext.implicits._

import com.github.karlhigley.spark.neighbors.ANN

import java.io._

object DedupFunctions {
  def distance(vec1 : SparseVector, vec2 : SparseVector, jaccard : Boolean) : Double = {
    if(jaccard) {
      // Jaccard distance is equal to Euclidean distance
      // on binary vectors, as long as we divide by max
      // possible mismatched entries
      val dist = Vectors.sqdist(vec1, vec2)
      // maximum distance would be if vectors' entries are
      // disjoint
      val maxDist = vec1.indices.size + vec2.indices.size
      // scale to distance produced range of squared
      // Euclidean distance to make comparisons easier
      dist / maxDist * 2.0
    } else {
      Vectors.sqdist(vec1, vec2)
    }
  }
}

object DedupApp {
  val enrichmentThreshold = 0.01

  val DUPLICATE = 0
  val NOT_DUPLICATE = 1

  def main(args: Array[String]) {
    val parser = new Conf(args)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("parquet").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("Optics App")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val workDir = parser.workDir()

    parser.subcommand.map {
      case parser.importDataMode =>
        val mode = parser.importDataMode

        val filterWordsFilename = mode.filterWords()
        val replaceWordsFilename = mode.replacementWords()
        val articlesFile = mode.articles()
        val windowSize = mode.windowSize()
        
        val textCleaner = TextCleaner(sc, filterWordsFilename,
                                      replaceWordsFilename,
                                      articlesFile,
                                      mode.minWordCount.get,
                                      mode.maxWordCount.get,
                                      windowSize)

        val cleanedText = textCleaner.cleanedText
        val vocab = textCleaner.extractedVocab

        val wordCountFeatures = Vectorizer.wordCountsToVectors(cleanedText, vocab)

        wordCountFeatures.save(workDir + "/documents")

      case parser.likelihoodMode =>
        val mode = parser.likelihoodMode

        val jaccard = mode.jaccard()

        val binWidth = mode.binWidth()

        val likelihoodFilename = mode.likelihoodFile()
        val duplicatesFilename = mode.duplicateSets() 
        val duplicateSets = IOFuncs.readDuplicateSets(duplicatesFilename)

        val wordCounts = sc.featureMatrix(workDir + "/documents")
        
        val binarizedWordCounts = if (mode.binarize()) {
          wordCounts.binarize()
        } else {
          wordCounts
        }

        val scaledWordCounts = if (mode.tfidf()) {
          binarizedWordCounts.tfidf()
        } else if (mode.normalize()) {
          binarizedWordCounts.normalizeL2()
        } else {
          binarizedWordCounts
        }

        val duplicatePairs = duplicateSets.flatMap {
          case duplicateSet =>
            duplicateSet.combinations(2)
              .map(_.toSet)
          }
          .toSet

        val duplicatePairsBC = sc.broadcast(duplicatePairs)

        val labeledVectors = scaledWordCounts.labeledVectors
          .repartition(sc.defaultParallelism)

        val likelihood = labeledVectors.cartesian(labeledVectors)
          .filter {
            case ((label1, vec1), (label2, vec2)) =>
              label1 != label2 && label1.toInt < label2.toInt
          }
          .map {
            case ((label1, vec1), (label2, vec2)) =>
              val labels = Set(label1, label2)
              val duplicate = duplicatePairsBC.value.contains(labels)
              val dist = DedupFunctions.distance(vec1, vec2, jaccard)
              val binIdx = (dist / binWidth).floor.toInt
              ((binIdx, duplicate), 1L)
          }
          .reduceByKey(_+_)
          .map {
            case ((binIdx, duplicate), counts) =>
              (binIdx, (duplicate, counts))
          }
          .groupByKey()
          .map {
            case (binIdx, iter) =>
              // Due to the reduceByKey, we should only have
              // two values for the bin: duplicates and non-duplicates
              val nDuplicatedArray = iter.filter {
                case (duplicate, counts) =>
                  duplicate
              }
              .map(_._2)
              .toArray

              val nDuplicated = if (nDuplicatedArray.size > 0) {
                nDuplicatedArray.head
                  .toDouble
              } else {
                0.0
              }

              val nUnduplicatedArray = iter.filter {
                case (duplicate, counts) =>
                  !duplicate
              }
              .map(_._2)
              .toArray

              val nUnduplicated = if (nUnduplicatedArray.size > 0) {
                nUnduplicatedArray.head
                  .toDouble
              } else {
                0.0
              }

              val likelihood = nDuplicated / (nDuplicated + nUnduplicated)
              val lowerbound = binIdx.toDouble * binWidth

              (lowerbound, likelihood)
          }
          .collect()
        
        val pw = new PrintWriter(likelihoodFilename)
        likelihood.map {
            case (lowerbound, likelihood) =>
            pw.print(lowerbound)
            pw.print("\t")
            pw.println(likelihood)
          }
        pw.close()


      case parser.annMode =>
        val mode = parser.annMode
        
        val duplicatesFilename = mode.duplicateSets() 
        val duplicateSets = IOFuncs.readDuplicateSets(duplicatesFilename)

        val wordCounts = sc.featureMatrix(workDir + "/documents")
        
        val binarizedWordCounts = if (mode.binarize()) {
          wordCounts.binarize()
        } else {
          wordCounts
        }

        val scaledWordCounts = if (mode.tfidf()) {
          binarizedWordCounts.tfidf()
        } else if (mode.normalize()) {
          binarizedWordCounts.normalizeL2()
        } else {
          binarizedWordCounts
        }

        val duplicatePairs = duplicateSets.flatMap {
          case duplicateSet =>
            duplicateSet.combinations(2)
              .map(_.toSet)
          }
          .toSet

        val duplicatePairsBC = sc.broadcast(duplicatePairs)

        val nDim = scaledWordCounts.nCols

        val indexedVec = scaledWordCounts.labeledVectors
          .repartition(sc.defaultParallelism)
          .zipWithUniqueId

        val relabeledVectors = indexedVec.map {
          case ((label, vec), id) =>
            (id.toInt, vec)
        }

        val labelIds = indexedVec.map {
          case ((label, vec), id) =>
            (id.toInt, label)
        }
        .collectAsMap

        val labelIdsBC = sc.broadcast(labelIds)

        val annModel = new ANN(dimensions = nDim,
                               measure = "jaccard")
                                 .setTables(4)
                                 .setSignatureLength(128)
                                  // needs to be larger than dimensions according to docs
                                 .setPrimeModulus(15485863)
                                 .setBands(16)
                                 .train(relabeledVectors)

        val neighbors = annModel.neighbors(10)
          .flatMap {
            case (source, iter) =>
              iter.map {
                 case (target, dist) =>
                   val labelIds = labelIdsBC.value
                   val pair = Set(labelIds(source), labelIds(target))
                   (duplicatePairsBC.value.contains(pair), 1)
              }
          }
          .reduceByKey(_ + _)
          .collectAsMap()

        val prec = neighbors(true).toDouble /
          (neighbors(true).toDouble + neighbors(false).toDouble)

        val recall = neighbors(true).toDouble / labelIds.size

        println(neighbors)
        println("Precision: " + prec)
        println("Recall: " + recall)

      case parser.rankingsMode =>
        val mode = parser.rankingsMode

        val jaccard = mode.jaccard()
        val rankingsFilename = mode.rankingsFile()
        val threshold = mode.threshold()

        val wordCounts = sc.featureMatrix(workDir + "/documents")
        
        val binarizedWordCounts = if (mode.binarize()) {
          wordCounts.binarize()
        } else {
          wordCounts
        }

        val scaledWordCounts = if (mode.tfidf()) {
          binarizedWordCounts.tfidf()
        } else if (mode.normalize()) {
          binarizedWordCounts.normalizeL2()
        } else {
          binarizedWordCounts
        }

        val labeledVectors = scaledWordCounts.labeledVectors
          .repartition(sc.defaultParallelism)

        val likelihood = labeledVectors.cartesian(labeledVectors)
          .filter {
            case ((label1, vec1), (label2, vec2)) =>
              label1 != label2 && label1.toInt < label2.toInt
          }
          .filter {
            case ((label1, vec1), (label2, vec2)) =>
              DedupFunctions.distance(vec1, vec2, jaccard) < threshold
          }
          // faster to recompute distance on the pairs that pass the filter
          // since doing the filter afterwards touches ever pair again
          .map {
            case ((label1, vec1), (label2, vec2)) =>
              val dist = DedupFunctions.distance(vec1, vec2, jaccard)
              ((label1, label2), dist)
          }
          .sortBy { 
            case (labels, dist) =>
              dist
          }
          .collect()
        
        val pw = new PrintWriter(rankingsFilename)
        likelihood.map {
          case ((label1, label2), dist) =>
            pw.print(label1)
            pw.print("\t")
            pw.print(label2)
            pw.print("\t")
            pw.println(dist)
        }
        pw.close()

      case _ =>
        System.out.println("Need to specify a mode.")
    }
  }
}
