package com.redhat.et.dedup

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector, Vectors}


object TopicProjection {
  def project(documentTopics : FeatureMatrix, pageviews : FeatureMatrix)
             : FeatureMatrix = {

    val nTopics = documentTopics.nCols.toInt
    
    // assume that documents only have 1 topic
    val documentAssignments = documentTopics.labeledVectors
      .map {
        case (pageLabel, vec) =>
          (pageLabel, vec.indices(0))
      }

    val projected = pageviews.labeledVectors
      .flatMap {
        case (userIdx, vec) =>
          vec.indices.zip(vec.values)
          .map {
            case (pageIdx, pageCount) =>
              (pageIdx, (userIdx, pageCount))
          }
      }
      .join(pageviews.featureLabels)
      .map {
        case (pageIdx, ((userIdx, pageCount), pageLabel)) =>
          (pageLabel, (userIdx, pageCount))
      }
      .join(documentAssignments)
      .map {
        case (pageLabel, ((userIdx, pageCount), topicIdx))=>
          ((topicIdx, userIdx), pageCount)
      }
      .reduceByKey(_ + _)
      .map {
        case ((topicIdx, userIdx), topicCount) =>
          (userIdx, (topicIdx, topicCount))
      }
      .groupByKey()
      .mapValues {
        pairs =>
          val sorted = pairs.toSeq
            .sortBy(_._1)
          val indices = sorted.map { _._1.toInt }
          val values = sorted.map { _._2.toDouble }

          new SparseVector(nTopics, indices.toArray, values.toArray)
      }
              
    new FeatureMatrix(projected, documentTopics.featureLabels)
  }
}
