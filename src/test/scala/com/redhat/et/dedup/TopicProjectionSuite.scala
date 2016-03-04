package com.redhat.et.dedup

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.SparseVector

import org.scalatest._

class TopicProjectionSpec extends FlatSpec with Matchers with PerTestSparkContext {

  val nTopics = 5
  val nDocuments = 7
  val documentTopics = Seq(("a", new SparseVector(nTopics, Array(4), Array(1.0))),
                           ("b", new SparseVector(nTopics, Array(3), Array(1.0))),
                           ("c", new SparseVector(nTopics, Array(1), Array(1.0))),
                           ("d", new SparseVector(nTopics, Array(1), Array(1.0))),
                           ("e", new SparseVector(nTopics, Array(3), Array(1.0))),
                           ("f", new SparseVector(nTopics, Array(2), Array(1.0))),
                           ("g", new SparseVector(nTopics, Array(0), Array(1.0))))

  val topicLabels = Seq((0, "topic0"), (1, "topic1"), (2, "topic2"),
                        (3, "topic3"), (4, "topic4"))

  val nUsers = 3
  val userDocuments = Seq(("user0", new SparseVector(nDocuments,
                                               Array(1, 3, 5),
                                               Array(25.0, 75.0, 100.0))),
                          ("user1", new SparseVector(nDocuments,
                                               Array(2, 4, 6),
                                               Array(33.0, 66.0, 99.0))),
                          ("user2", new SparseVector(nDocuments,
                                               Array(1, 4),
                                               Array(20.0, 30.0))))

  val expectedProjected = Seq(("user0", new SparseVector(nTopics,
                                                   Array(1, 2, 3),
                                                   Array(75.0, 100.0, 25.0))),
                              ("user1", new SparseVector(nTopics,
                                                   Array(0, 1, 3),
                                                   Array(99.0, 33.0, 66.0))),
                              ("user2", new SparseVector(nTopics,
                                                   Array(3),
                                                   Array(50.0))))

  val documentLabels = Seq((0, "a"),
                           (1, "b"),
                           (2, "c"),
                           (3, "d"),
                           (4, "e"),
                           (5, "f"),
                           (6, "g"))
                               

  "TopicProjection.project" should "project properly" in {

    val documentTopicsMatrix = new FeatureMatrix(context.parallelize(documentTopics),
                                                 context.parallelize(topicLabels))

    val userDocumentsMatrix = new FeatureMatrix(context.parallelize(userDocuments),
                                                context.parallelize(documentLabels))

    val projected = TopicProjection.project(documentTopicsMatrix,
                                            userDocumentsMatrix)

    val observedProjected = projected.labeledVectors.collect()

    assert(observedProjected.toSet === expectedProjected.toSet)
  }

}
