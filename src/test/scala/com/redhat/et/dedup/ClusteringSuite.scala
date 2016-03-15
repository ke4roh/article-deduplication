package com.redhat.et.dedup

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector, Vectors}

import org.scalatest._

import scala.util.Random

class KCentersSpec extends FlatSpec with Matchers with PerTestSparkContext {
  "KCenters.train" should "generate 3 clusters with radius < 5" in {
    val maxRadius = 5.0
    val centers = List((1.0, 1.0, 1.0),
                       (10.0, 10.0, 10.0),
                       (-10.0, -10.0, -10.0))

    val rng = new Random()
    val std = 0.1
    val points = centers.flatMap {
      case (x, y, z) =>
        (1 to 10).map {
            case _ =>
              (x + std * rng.nextGaussian(),
               y + std * rng.nextGaussian(),
               z + std * rng.nextGaussian())
        }
    }
    .map {
      case (x, y, z) =>
        new SparseVector(3, Array(0, 1, 2), Array(x, y, z))
    }
    .zipWithIndex
    .map {
      case (vec, label) =>
        (label.toString, vec)
    }

    val featureLabels = List((0, "x"),
                             (1, "y"),
                             (2, "z"))

    val featureMatrix = new FeatureMatrix(context.parallelize(points),
                                          context.parallelize(featureLabels))

    val model = KCenters.train(maxRadius, featureMatrix)
    val assignments = model.assign(featureMatrix)

    println(model.centers.toList)
    assert(model.centers.size === 3)

    model.centers
      .combinations(2)
      .map {
        iter =>
          val array = iter.take(2)
          val center1 = array(0)._2
          val center2 = array(1)._2
          val distance = math.sqrt(Vectors.sqdist(center1, center2))

          assert(distance < maxRadius)
    }
  }
}

class ClusteringSpec extends FlatSpec with Matchers with PerTestSparkContext {
  "ClusterModel.toFeatureMatrix" should "return FeatureMatrix" in {
    val center1 = new SparseVector(3, Array(0, 1, 2), Array(1, 0, 1))
    val center2 = new SparseVector(3, Array(0, 1, 2), Array(0, 1, 0))
    val assignments = Seq(("Cat", 0), ("Dog", 1), ("Cow", 0), ("Mouse", 1))
    val expectedFeatureLabels = Seq((0.toInt, "0"), (1.toInt, "1"))
    val expectedVectors = Seq(("Cat", (2, Seq(0), Seq(1.0))),
                              ("Dog", (2, Seq(1), Seq(1.0))),
                              ("Cow", (2, Seq(0), Seq(1.0))),
                              ("Mouse", (2, Seq(1), Seq(1.0))))

    val model = new ClusterModel(2,
                                 Array(center1, center2),
                                 context.parallelize(assignments))

    val featureMatrix = model.toFeatureMatrix()

    val featureLabels = featureMatrix.featureLabels.collect().toSeq
    val labeledVectors = featureMatrix.labeledVectors.collect()
      .map {
        case (label, vec) =>
          (label, (vec.size, vec.indices.toList, vec.values.toList))
      }.toList

    assert(featureLabels === expectedFeatureLabels)
    assert(labeledVectors === expectedVectors) 
  }

  "Clustering.sweepClusters" should "return costs" in {
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
    val nClusters = Seq(2, 3)

    val featureMatrix = new FeatureMatrix(context.parallelize(labeledVectors),
                                          context.parallelize(featureLabels))

    val metrics = Clustering.sweepClusters(nClusters,
                                           featureMatrix)

    assert(metrics.inertia.size === (1 to nClusters.max).size)
  }

  "Clustering.cluster" should "return ClusterModel" in {
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
    val nClusters = 3

    val featureMatrix = new FeatureMatrix(context.parallelize(labeledVectors),
                                          context.parallelize(featureLabels))
    
    val model = Clustering.cluster(nClusters, featureMatrix)
    val centers = model.centers
      .map {
        vec =>
          val sparse = vec.toSparse
          (sparse.size, sparse.indices.toList, sparse.values.toList)
      }
      .toSet

    val expectedCenters = Set((vec1.size, vec1.indices.toList, vec1.values.toList),
                              (vec2.size, vec2.indices.toList, vec2.values.toList),
                              (vec3.size, vec3.indices.toList, vec3.values.toList))

    assert(centers === expectedCenters)
  }
}
