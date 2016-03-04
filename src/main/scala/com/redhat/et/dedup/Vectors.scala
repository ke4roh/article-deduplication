package com.redhat.et.dedup

import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector, Vectors}

object VectorOps {
  /**
   * Add two <code>SparseVectors</code> of the same length.
   */
  def add(vec1: SparseVector, vec2: SparseVector) : SparseVector = {
    val indices = (vec1.indices.toSet ++ vec2.indices.toSet)
      .toArray
      .sortBy { idx => idx }

    val values = indices.map {
      idx =>
        vec1(idx) + vec2(idx)
    }
    
    new SparseVector(vec1.size, indices, values.toArray)
  }

  /**
   * Scale values of <code>SparseVector</code> by <code>factor</code>.
   */
  def scale(vec: SparseVector, factor: Double) : SparseVector = {
    val scaled = vec.values.map {
      v =>
        if (factor == 0.0) {
          0.0
        } else {
          v * factor
        }
    }

    new SparseVector(vec.size, vec.indices, scaled)
  }

  /**
   * Normalize the <code>SparseVector</code> using the <code>p</code>-norm
   * of the vector.
   */
  def normalize(vec: SparseVector, p: Double) : SparseVector = {
    val normFactor = Vectors.norm(vec, p) match {
      case 0.0 => 0.0
      case x => 1.0 / x
    } 
    scale(vec, normFactor)
  }
}

