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
        
        val textCleaner = TextCleaner(sc, filterWordsFilename,
                                      replaceWordsFilename,
                                      articlesFile,
                                      mode.minWordCount.get,
                                      mode.maxWordCount.get)

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
          .map {
            case ((label1, vec1), (label2, vec2)) =>
              val labels = Set(label1, label2)
              val duplicate = duplicatePairsBC.value.contains(labels)
              val dist = if(jaccard) {
                // Jaccard distance is equal to Euclidean distance
                // on binary vectors, as long as we divide by max
                // possible mismatched entries
                val dist = math.sqrt(Vectors.sqdist(vec1, vec2))
                // maximum distance would be if vectors' entries are
                // disjoint
                val maxDist = vec1.indices.size + vec2.indices.size
                // scale to distance produced range of squared
                // Euclidean distance to make comparisons easier
                dist / maxDist * 2.0
              } else {
                Vectors.sqdist(vec1, vec2)
              }
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
              val nDuplicated = iter.filter {
                case (duplicate, counts) =>
                  duplicate
              }
              .map(_._2)
              .toArray
              .head
              .toDouble

              val nUnduplicated = iter.filter {
                case (duplicate, counts) =>
                  !duplicate
              }
              .map(_._2)
              .toArray
              .head
              .toDouble

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
        

      case _ =>
        System.out.println("Need to specify a mode.")
    }
  }
}
