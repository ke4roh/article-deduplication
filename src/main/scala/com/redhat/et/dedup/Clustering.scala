package com.redhat.et.dedup

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector, Vectors}
import org.apache.spark.mllib.clustering.KMeans

case class ClusterMetrics(inertia : SparseVector)

class ClusterModel(val nClusters : Int,
                   val centers: Array[SparkVector],
                   val assignments : RDD[(String, Int)]) {

  def toFeatureMatrix() : FeatureMatrix = {
    val nClusters = this.nClusters

    val labeledVectors = assignments.mapValues {
      clusterIdx =>
        new SparseVector(nClusters, Array(clusterIdx), Array(1.0))
    }

    val featureLabels = assignments.context
      .parallelize(0 until nClusters)
      .map {
        clusterIdx =>
          (clusterIdx, clusterIdx.toString)
    }

    new FeatureMatrix(labeledVectors,
                      featureLabels)
  }
}

case class KCentersModel(val nCenters : Int,
                         val centers: Vector[(Int, SparseVector)]) {

  def assign(featureMatrix : FeatureMatrix) : FeatureMatrix = {

    val sc = featureMatrix.labeledVectors.context
    val centersBC = sc.broadcast(centers)

    val assignedVectors = featureMatrix.labeledVectors
      .map {
        case (label, vector) =>
          val (assignIdx, _) = centersBC.value.minBy {
            case (centerIdx, center) =>
              math.sqrt(Vectors.sqdist(vector, center))
          }
          
          (label, new SparseVector(nCenters, Array(assignIdx), Array(1.0)))
    }

    new FeatureMatrix(assignedVectors,
                      featureMatrix.featureLabels)
  }
}

object KCenters {

  def train(maxRadius : Double, featureMatrix : FeatureMatrix) : KCentersModel = {
    var vectors = featureMatrix.labeledVectors
      .map {
        case (label, vec) =>
          (label, vec, java.lang.Double.POSITIVE_INFINITY)
      }
      .persist()

    var radius = java.lang.Double.POSITIVE_INFINITY
    var currentCenter = vectors.takeSample(false, 1)(0) match {
      case (label, vec, dist) =>
        (0, vec)
    }
    var centers = Vector[(Int, SparseVector)]()

    var i = 1
    while (radius > maxRadius) {
      val (centerIdx, center) = currentCenter
      val updatedVectors = vectors.map {
        case (label, vec, assignDist) =>
          val newDist = math.sqrt(Vectors.sqdist(center, vec))
          (label, vec, math.min(newDist, assignDist))
      }
      .persist()

      if (i % 100 == 0) {
        updatedVectors.checkpoint()
      }

      vectors.unpersist()
      
      val (_, newCenter, newRadius) = updatedVectors.reduce {
        case ((label1, vec1, assignDist1),
              (label2, vec2, assignDist2)) =>
          if (assignDist1 > assignDist2) {
            (label1, vec1, assignDist1)
          } else {
            (label2, vec2, assignDist2)
          }
      }

      println(i.toString + ", Radius: " + newRadius)

      vectors = updatedVectors
      centers = centers :+ currentCenter
      currentCenter = (centerIdx + 1, newCenter)
      radius = newRadius
      i += 1
    }

    new KCentersModel(centers.size,
                      centers)
  }
}

object Clustering {

  def sweepClusters(clusterCounts : Seq[Int],
                    featureMatrix: FeatureMatrix) : ClusterMetrics = {

    val sc = featureMatrix.labeledVectors
      .context

    val vectors : RDD[SparkVector] = featureMatrix.labeledVectors
      .map {
        case (label, sparse) =>
          sparse.asInstanceOf[SparkVector]
      }
      .repartition(sc.defaultParallelism)

    vectors.cache()

    val inertiaPairs = clusterCounts.map {
      k =>
        val model = new KMeans()
          .setInitializationSteps(20)
          // set higher iterations, runs based on scikit-learn's defaults
          .setMaxIterations(100)
          .setRuns(10)
          .setK(k)
          .run(vectors)

        val cost = model.computeCost(vectors)

        println(k.toString + " clusters, inertia: " + cost)

        (k, cost)
    }
    
    val costs = inertiaPairs.map { _._2 }

    vectors.unpersist()
    
    new ClusterMetrics(new SparseVector(clusterCounts.max,
                                        clusterCounts.toArray, 
                                        costs.toArray))
  }

  

  def cluster(nClusters: Int, 
              featureMatrix : FeatureMatrix) : ClusterModel = {

    val sc = featureMatrix.labeledVectors
      .context

    val labeledVectors = featureMatrix.labeledVectors
      .repartition(sc.defaultParallelism)
      .cache()

    val vectors : RDD[SparkVector] = labeledVectors.values
      .map {
        vec =>
          vec.asInstanceOf[SparkVector]
      }

    val model = new KMeans()
      .setInitializationSteps(20)
      // set higher iterations, runs based on scikit-learn's defaults
      .setMaxIterations(100)
      .setRuns(10)
      .setK(nClusters)
      .run(vectors)
    
    val centers = model.clusterCenters
    val assignments = model.predict(vectors)

    val labelAssignments = labeledVectors.keys
      .zip(assignments)

    vectors.unpersist()
      
    new ClusterModel(nClusters, centers, labelAssignments)
  }

}
