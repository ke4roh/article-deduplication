package com.redhat.et.dedup

import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector}

import java.io._

import org.json4s._

case class EnrichedWord(word : String, enrichment: Double)

case class TopicModel(clusterModel : ClusterModel,
                      enrichedWords : Map[Int, Seq[EnrichedWord]],
                      enrichmentThreshold: Double) {

  lazy val nTopics = clusterModel.nClusters
  lazy val documentAssignments = clusterModel.assignments

  def save(basename: String) = {
    clusterModel.toFeatureMatrix
      .save(basename)

    /* ran into a bunch of problems with
     * json4s.  Temporary measure to produce
     * JSON output.  Confirmed that the
     * output is parseable with Python's
     * json library.
     */

//    import native.Serialization.write
//    implicit val formats = native.Serialization.formats(NoTypeHints)
//    val json = write(enrichedWords)

    val enrichedWordsString = enrichedWords.map {
      case (topicIdx, words) =>
        val wordsString = words.map {
          word =>
            "\"" + word.word + "\" : " + word.enrichment.toString
        }
        .mkString(", ")

        "{ \"cluster\" : " + topicIdx.toString + " , \"enrichedWords\" : { " + wordsString + " } }"
    }
    .mkString(", ")

    val json = "{ \"enrichmentThreshold\" : " + enrichmentThreshold.toString + ", \n" + "\"clusters\" : [ " + enrichedWordsString + " ] }"

    val pw = new PrintWriter(basename + ".json")
    pw.print(json)
    pw.close()
  }
}

object TopicAnalysis {
  /**
   *
   */
  def extractTopics(wordCounts : FeatureMatrix,
                    clusterModel : ClusterModel,
                    threshold : Double) : TopicModel = {
    
    val wordCountClusters = wordCounts.labeledVectors
      .join(clusterModel.assignments)
      .map {
        case (sampleId, (wordCounts, clusterId)) =>
          (clusterId, (wordCounts, 1L))
    }

    val perClusterAverages = wordCountClusters.reduceByKey {
      case ((wordCounts1, counts1), (wordCounts2, counts2)) =>
        (VectorOps.add(wordCounts1, wordCounts2), counts1 + counts2)
    }

    perClusterAverages.persist()

    val vocab = wordCounts.featureLabels
      .collectAsMap()

    val clusterEnrichments = (0 until clusterModel.nClusters).map {
      currentClusterIdx =>
        val insidePair = perClusterAverages.filter { _._1 == currentClusterIdx }
          .take(1)(0)._2
        val inside = VectorOps.scale(insidePair._1, 1.0 / insidePair._2.toDouble)

        val outsidePair = perClusterAverages.filter { _._1 != currentClusterIdx }
          .values
          .reduce {
            case ((wordCounts1, counts1), (wordCounts2, counts2)) =>
              (VectorOps.add(wordCounts1, wordCounts2), counts1 + counts2)
          }
        val outside = VectorOps.scale(outsidePair._1, 1.0 / outsidePair._2.toDouble)

        // closer to 1.0 = enriched inside cluster
        // closer to -1.0 = enriched outside cluster
        val enrichment = VectorOps.add(inside, VectorOps.scale(outside, -1.0))

        val enrichedWords = enrichment.indices
          .zip(enrichment.values)
          .sortBy(-1.0 * _._2)
          .filter {
            case (wordIdx, enrichment) =>
              enrichment > threshold
          }
          .map {
            case (wordIdx, enrichment) =>
              new EnrichedWord(vocab.getOrElse(wordIdx, "unknown word"),
               enrichment)
          }

      (currentClusterIdx, enrichedWords.toSeq)
    }
    .toMap
   
   new TopicModel(clusterModel,
                  clusterEnrichments,
                  threshold)
  }

}
