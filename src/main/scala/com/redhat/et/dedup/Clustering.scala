package com.redhat.et.dedup

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector}
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
