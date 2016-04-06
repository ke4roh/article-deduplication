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

import java.io._
import java.nio.file._

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

  def getWordCounts(workDir : String, binarize : Boolean, tfidf : Boolean,
                    normalize : Boolean, sc : SparkContext) : FeatureMatrix = {

    val wordCounts = sc.featureMatrix(workDir + "/documents")
        
    val binarizedWordCounts = if (binarize) {
      wordCounts.binarize()
    } else {
      wordCounts
    }
    
    val tfidfWordCounts = if (tfidf) {
      binarizedWordCounts.tfidf()
    } else {
      binarizedWordCounts
    }

    val scaledWordCounts = if (normalize) {
      tfidfWordCounts.normalizeL2()
    } else {
      tfidfWordCounts
    }

    scaledWordCounts
  }

  def histogram(workDir : String, scaledWordCounts : FeatureMatrix, histogramFilename : String, binWidth : Double, jaccard : Boolean) = {
    
    val labeledVectors = scaledWordCounts.labeledVectors
      .cache()
    
    val histogram = labeledVectors.cartesian(labeledVectors)
      .filter {
        case ((label1, vec1), (label2, vec2)) =>
          label1 != label2 && label1.toInt < label2.toInt
      }
      .map {
        case ((label1, vec1), (label2, vec2)) =>
          val dist = DedupFunctions.distance(vec1, vec2, jaccard)
          val lowerbound = (dist / binWidth).floor * binWidth
          (lowerbound, 1L)
      }
      .reduceByKey(_+_)
      .collect()
        
      val pw = new PrintWriter(workDir + "/" + histogramFilename)
      histogram.map {
        case (lowerbound, counts) =>
          pw.print(lowerbound)
          pw.print("\t")
          pw.println(counts)
      }
      pw.close()
  }

  def likelihood(workDir : String, scaledWordCounts : FeatureMatrix, likelihoodFilename : String, duplicatesFilename : String, binWidth : Double, jaccard : Boolean) = {

    val duplicateSets = IOFuncs.readDuplicateSets(duplicatesFilename)
    val duplicatePairs = duplicateSets.flatMap {
      case duplicateSet =>
        duplicateSet.combinations(2)
          .map(_.toSet)
      }
      .toSet

    val duplicatePairsBC = scaledWordCounts.labeledVectors
      .context
      .broadcast(duplicatePairs)

    val labeledVectors = scaledWordCounts.labeledVectors
      .cache()

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

    val pw = new PrintWriter(workDir + "/" + likelihoodFilename)
    likelihood.map {
      case (lowerbound, likelihood) =>
        pw.print(lowerbound)
        pw.print("\t")
        pw.println(likelihood)
    }
    pw.close()
  }

  def rankings(workDir : String, scaledWordCounts : FeatureMatrix, rankingsFilename : String, threshold : Double, jaccard : Boolean) = {
    
    val labeledVectors = scaledWordCounts.labeledVectors
      .cache()

    val distances = labeledVectors.cartesian(labeledVectors)
      .filter {
        case ((label1, vec1), (label2, vec2)) =>
          label1 != label2 && label1.toInt < label2.toInt 
      }
      .map {
        case ((label1, vec1), (label2, vec2)) =>
          val dist = DedupFunctions.distance(vec1, vec2, jaccard)
          ((label1, label2), dist)
      }
      .filter {
        case (labels, dist) =>
          dist < threshold
      }
      .collect()
      .sortBy { 
        case (labels, dist) =>
          dist
      }
        
    val pw = new PrintWriter(workDir + "/" + rankingsFilename)
    distances.map {
      case ((label1, label2), dist) =>
        pw.print(label1)
        pw.print("\t")
        pw.print(label2)
        pw.print("\t")
        pw.println(dist)
    }
    pw.close()
  }
  
  def main(args: Array[String]) {
    val parser = new Conf(args)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("parquet").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("Dedup App")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val checkpointPath = Files.createTempDirectory("dedup")
    checkpointPath.toFile.deleteOnExit()
    sc.setCheckpointDir(checkpointPath.toString)

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

        val likelihoodFilename = mode.likelihoodFile()
        val duplicatesFilename = mode.duplicateSets() 

        val scaledWordCounts = getWordCounts(workDir, mode.binarize(), mode.tfidf(), mode.normalize(), sc)

        likelihood(workDir, scaledWordCounts, likelihoodFilename, duplicatesFilename,
                   mode.binWidth(), mode.jaccard())

      case parser.histogramMode =>
        val mode = parser.histogramMode

        val histogramFilename = mode.histogramFile()
        
        val scaledWordCounts = getWordCounts(workDir, mode.binarize(), mode.tfidf(), mode.normalize(), sc)

        histogram(workDir, scaledWordCounts, histogramFilename, mode.binWidth(),
                  mode.jaccard())

      case parser.rankingsMode =>
        val mode = parser.rankingsMode

        val rankingsFilename = mode.rankingsFile()

        val scaledWordCounts = getWordCounts(workDir, mode.binarize(), mode.tfidf(), mode.normalize(), sc)

        rankings(workDir, scaledWordCounts, rankingsFilename, mode.threshold(), mode.jaccard())

      case _ =>
        System.out.println("Need to specify a mode.")
    }
  }
}
