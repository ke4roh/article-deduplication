package com.redhat.et.dedup

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector, Vectors}

import org.scalatest._

class VectorOpsSpec extends FlatSpec with Matchers with PerTestSparkContext {

  val eps = 1e-5

  val wordCounts1 = Array(1.0, 0.0, 1.0, 1.0)
  val indices = Array(0, 1, 2, 3)

  val testVector = new SparseVector(wordCounts1.size, indices, wordCounts1)

  val expectedNormalizedL1 = Array(1.0 / 3.0, 0.0, 1.0 / 3.0, 1.0 / 3.0)
  val expectedNormalizedL2 = Array(1.0 / math.sqrt(3.0), 0.0, 
                                   1.0 / math.sqrt(3.0), 1.0 / math.sqrt(3.0))

  "VectorOps.normalize" should "normalize output" in {
    val normalizedL1 = VectorOps.normalize(testVector, 1.0)
    assert(normalizedL1.values === expectedNormalizedL1)
    assert(math.abs(Vectors.norm(normalizedL1, 1.0) - 1.0) < eps)

    val normalizedL2 = VectorOps.normalize(testVector, 2.0)
    assert(normalizedL2.values === expectedNormalizedL2)
    assert(math.abs(Vectors.norm(normalizedL2, 2.0) - 1.0) < eps)
  }

  "VectorOps.scale" should "scale output" in {
    val scale = 5.0
    val scaledVec = VectorOps.scale(testVector, scale)

    scaledVec.values
      .zip(wordCounts1)
      .map {
        case (scaled, original) =>
          assert(math.abs(scaled - scale * original) < eps)
      }
  }
}
