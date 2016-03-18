package com.redhat.et.dedup

import math.Ordering

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector, Vectors}

import org.scalatest._

class FeatureMatrixSpec extends FlatSpec with Matchers with PerTestSparkContext {
  "FeatureMatrix" should "return correct dimensions" in {
    val vec1 = new SparseVector(3, Array(0), Array(1))
    val vec2 = new SparseVector(3, Array(1), Array(1))
    val vec3 = new SparseVector(3, Array(2), Array(1))
    
    val labeledVectors = Seq(("0", vec1),
                             ("1", vec2),
                             ("2", vec3),
                             ("3", vec1),
                             ("4", vec2),
                             ("5", vec3))
    
    val featureLabels = Seq((0, "Feature 1"),
                            (1, "Feature 2"),
                            (2, "Feature 3"))
    
    val featureMatrix = new FeatureMatrix(context.parallelize(labeledVectors),
                                          context.parallelize(featureLabels))
    
    val expectedRows = labeledVectors.size
    val expectedCols = vec1.size

    assert(featureMatrix.nRows === expectedRows)
    assert(featureMatrix.nCols === expectedCols)
  }

  "FeatureMatrix.normalize methods" should "return normalized rows" in {
    val vec1 = new SparseVector(3, Array(0), Array(1))
    val vec2 = new SparseVector(3, Array(1), Array(1))
    val vec3 = new SparseVector(3, Array(2), Array(1))

    val labeledVectors = Seq(("0", vec1),
                             ("1", vec2),
                             ("2", vec3),
                             ("3", vec1),
                             ("4", vec2),
                             ("5", vec3))
    
    val featureLabels = Seq((0, "Feature 1"),
                            (1, "Feature 2"),
                            (2, "Feature 3"))
    
    val featureMatrix = new FeatureMatrix(context.parallelize(labeledVectors),
                                          context.parallelize(featureLabels))

    val normalizedL1Matrix = featureMatrix.normalizeL1()
    normalizedL1Matrix.labeledVectors
      .collect()
      .map {
        case (label, vec) =>
          assert(Vectors.norm(vec, 1.0) === 1.0)
      }

    val normalizedL2Matrix = featureMatrix.normalizeL2()
    normalizedL2Matrix.labeledVectors
      .collect()
      .map {
        case (label, vec) =>
          assert(Vectors.norm(vec, 2.0) === 1.0)
      }
  }

  "FeatureMatrix.labeledVectors" should "return (sampleLabel, vector) pairs" in {
    val vec1 = new SparseVector(3, Array(0), Array(1))
    val vec2 = new SparseVector(3, Array(1), Array(1))
    val vec3 = new SparseVector(3, Array(2), Array(1))

    val labeledVectors = Seq(("a", vec1),
                             ("b", vec2),
                             ("c", vec3),
                             ("d", vec1),
                             ("e", vec2),
                             ("f", vec3))
    
    val featureLabels = Seq((0, "Feature 1"),
                            (1, "Feature 2"),
                            (2, "Feature 3"))
    
    val featureMatrix = new FeatureMatrix(context.parallelize(labeledVectors),
                                          context.parallelize(featureLabels))

    val obsLabeledVectors = featureMatrix.labeledVectors
      .collect()

    assert(obsLabeledVectors.size === labeledVectors.size)

    obsLabeledVectors.map {
      case (label, vec) =>
        label match {
          case "a" =>
            assert(vec === vec1)
          case "b" =>
            assert(vec === vec2)
          case "c" =>
            assert(vec === vec3)
          case "d" =>
            assert(vec === vec1)
          case "e" =>
            assert(vec === vec2)
          case "f" =>
            assert(vec === vec3)
        }
    }
  }

  "FeatureMatrix.tfidf" should "compute text-frequency inverse-document frequency" in {
    val vec1 = new SparseVector(3, Array(0), Array(1))
    val vec2 = new SparseVector(3, Array(1), Array(1))
    val vec3 = new SparseVector(3, Array(2), Array(1))

    val labeledVectors = Seq(("0", vec1),
                             ("1", vec2),
                             ("2", vec3),
                             ("3", vec1),
                             ("4", vec2),
                             ("5", vec3))

    val featureLabels = Seq((0, "Feature 1"),
                            (1, "Feature 2"),
                            (2, "Feature 3"))
    
    val featureMatrix = new FeatureMatrix(context.parallelize(labeledVectors),
                                          context.parallelize(featureLabels))

    val tfidfVectors = featureMatrix
      .tfidf()
      .labeledVectors
      .collect()

    tfidfVectors.map {
      case (label, vec) =>
        assert(vec.values.toSet === Set(0.8472978603872037))
    }
  }

  "FeatureMatrix.binarize" should "return all 1s for entries" in {
    val vec1 = new SparseVector(3, Array(0), Array(2))
    val vec2 = new SparseVector(3, Array(1), Array(3))
    val vec3 = new SparseVector(3, Array(2), Array(4))

    val labeledVectors = Seq(("0", vec1),
                             ("1", vec2),
                             ("2", vec3),
                             ("3", vec1),
                             ("4", vec2),
                             ("5", vec3))

    val featureLabels = Seq((0, "Feature 1"),
                            (1, "Feature 2"),
                            (2, "Feature 3"))
    
    val featureMatrix = new FeatureMatrix(context.parallelize(labeledVectors),
                                          context.parallelize(featureLabels))

    val binaryVectors = featureMatrix
      .binarize()
      .labeledVectors
      .collect()

    binaryVectors.map {
      case (label, vec) =>
        assert(Vectors.norm(vec, 2.0) === 1.0)
    }
  }
}
