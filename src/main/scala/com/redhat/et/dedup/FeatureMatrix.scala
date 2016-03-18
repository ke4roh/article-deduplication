package com.redhat.et.dedup

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.feature.{IDF, IDFModel}
import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector, Vectors}

import scala.language.implicitConversions

import java.io._

class FeatureMatrix(val labeledVectors: RDD[(String, SparseVector)],
                    val featureLabels: RDD[(Int, String)]) {

  lazy val nRows = labeledVectors.count()

  lazy val nCols = labeledVectors.take(1)(0) match {
    case (_, vec) =>
      vec.size
  }

  def binarize() : FeatureMatrix = {
    val binaryVectors = labeledVectors.mapValues {
      vec =>
        val binaryValues = (1 to vec.values.size)
          .map { i => 1.0 }
          .toArray

        new SparseVector(vec.size, vec.indices, binaryValues)
    }

    new FeatureMatrix(binaryVectors, featureLabels)
  }

  def normalize(p : Double) : FeatureMatrix = {
    val normedVectors = labeledVectors.mapValues {
      vec =>
        VectorOps.normalize(vec, p)
    }

    new FeatureMatrix(normedVectors, featureLabels)
  }

  def normalizeL1() : FeatureMatrix = {
    normalize(1.0)
  }

  def normalizeL2() : FeatureMatrix = {
    normalize(2.0)
  }

  def tfidf() : FeatureMatrix = {
    val vectors = labeledVectors.values
      .map {
        vec =>
          vec.asInstanceOf[SparkVector]
      }

    val labels = labeledVectors.keys

    val model = new IDF().fit(vectors)
    val tfidfVectors = model.transform(vectors)
      .map {
        case vec =>
          vec.toSparse
      }

    val labeledTfidf = labels.zip(tfidfVectors)

    new FeatureMatrix(labeledTfidf,
                      featureLabels)
  }

  def save(basename : String) = {
    val matrixPW = new PrintWriter(basename + ".data")

    val vectorIds = labeledVectors.zipWithUniqueId
    
    val indexedVectors = vectorIds.map {
      case ((label, vec), idx) =>
        (idx, vec)
    }

    val sampleLabels = vectorIds.map {
      case ((label, vec), idx) =>
        (idx, label)
    }

    val localVectors = indexedVectors.collect()
      .foreach {
        case (rowId, vec) =>
          vec.indices.foreach {
            c =>
              val v = vec(c)
              if (v != 0.0) {
                matrixPW.print(rowId)
                matrixPW.print("\t")
                matrixPW.print(c)
                matrixPW.print("\t")
                matrixPW.println(v)
              }
          }
      }
    matrixPW.close()

    val sampleLabelPW = new PrintWriter(basename + ".sample_labels")
    sampleLabels.collect()
      .foreach {
        case (rowId, label) =>
          sampleLabelPW.print(rowId)
          sampleLabelPW.print("\t")
          sampleLabelPW.println(label)
      }
    sampleLabelPW.close()

    val featureLabelPW = new PrintWriter(basename + ".feature_labels")
    featureLabels.collect()
      .foreach {
        case (colId, label) =>
          featureLabelPW.print(colId)
          featureLabelPW.print("\t")
          featureLabelPW.println(label)
      }
    featureLabelPW.close()
  }
        
}

private[dedup] case class SparkContextWithLoadFeatureMatrix(sc : SparkContext) {
  def featureMatrix(basename : String) : FeatureMatrix = {
    val sampleLabels = sc.textFile(basename + ".sample_labels")
      .map {
        line =>
          val cols = line.split("\t")
          (cols(0).toInt, cols(1))
      }

    val featureLabels = sc.textFile(basename + ".feature_labels")
      .map {
        line =>
          val cols = line.split("\t")
          (cols(0).toInt, cols(1))
      }
      .repartition(sc.defaultParallelism)

    val nFeatures = featureLabels.count().toInt

    val indexedVectors = sc.textFile(basename + ".data")
      .map {
        line => 
          val cols = line.split("\t")
          (cols(0).toInt, (cols(1).toInt, cols(2).toDouble))
    }
    .groupByKey()
    .mapValues {
      pairs =>
        val sorted = pairs.toSeq.sortBy { _._1 }
        val indices = pairs.map { _._1 }
        val values = pairs.map { _._2 }

        new SparseVector(nFeatures, indices.toArray, values.toArray)
    }

    val labeledVectors = sampleLabels.join(indexedVectors)
      .map {
        case (idx, (label, vec)) =>
          (label, vec)
      }
      .repartition(sc.defaultParallelism)

    new FeatureMatrix(labeledVectors, featureLabels)
  }
}

object FeatureMatrixSparkContext {
  object implicits {
    implicit def contextWithFeatureMatrix(sc : SparkContext) = SparkContextWithLoadFeatureMatrix(sc)
  }
}
