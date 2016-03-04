package com.redhat.et.dedup

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Vector => SparkVector}

import org.scalatest._

import java.nio.file.Files
import java.util.Arrays

class TopicModelSpec extends FlatSpec with Matchers with PerTestSparkContext {               

  "TopicModel.save" should "successfully generate a file" in {

    val tempFile = Files.createTempFile("topicModel", "json").toFile
    tempFile.deleteOnExit()

    val nClusters = 5
    val centers = Array[SparkVector]() // we don't use centers for these tests
    val assignments = List[(String, Int)](("a", 0),
                                          ("b", 1),
                                          ("c", 2),
                                          ("d", 3),
                                          ("e", 4))

    val enrichedWords = Map[Int, Seq[EnrichedWord]](
      0 -> List(EnrichedWord("oh", 1.0),
                EnrichedWord("the", 0.9),
                EnrichedWord("places", 0.8),
                EnrichedWord("you'll", 0.7),
                EnrichedWord("go", 0.6)),
      1 -> List(EnrichedWord("and", 1.0),
                EnrichedWord("to", 0.95),
                EnrichedWord("think", 0.9),
                EnrichedWord("i", 0.85),
                EnrichedWord("saw", 0.8),
                EnrichedWord("it", 0.75),
                EnrichedWord("on", 0.7),
                EnrichedWord("mulberry", 0.65),
                EnrichedWord("street", 0.6)),
      2 -> List(EnrichedWord("horton", 1.0),
                EnrichedWord("hatches", 0.95),
                EnrichedWord("the", 0.9),
                EnrichedWord("egg", 0.85)),
      3 -> List(EnrichedWord("yertle", 1.0),
                EnrichedWord("the", 0.9),
                EnrichedWord("turtle", 0.8)),
      4 -> List(EnrichedWord("green", 1.0),
                EnrichedWord("eggs", 0.95),
                EnrichedWord("and", 0.9),
                EnrichedWord("ham", 0.85)))

    val clusterModel = new ClusterModel(nClusters,
                                        centers, 
                                        context.parallelize(assignments))

    val topicModel = TopicModel(clusterModel,
                                enrichedWords,
                                0.5)

    topicModel.save(tempFile.toString)

    assert(tempFile.exists === true)
  }

  "TopicModel.save" should "successfully generate a file when data is missing" in {

    val tempFile = Files.createTempFile("topicModel", "json").toFile
    tempFile.deleteOnExit()

    val nClusters = 5
    val centers = Array[SparkVector]() // we don't use centers for these tests
    val assignments = List[(String, Int)](("a", 0),
                                          ("b", 1),
                                          ("c", 2),
                                          ("d", 3),
                                          ("e", 4))

    val enrichedWords = Map[Int, Seq[EnrichedWord]](
      0 -> List[EnrichedWord](),
      1 -> List(EnrichedWord("and", 1.0),
                EnrichedWord("to", 0.95),
                EnrichedWord("think", 0.9),
                EnrichedWord("i", 0.85),
                EnrichedWord("saw", 0.8),
                EnrichedWord("it", 0.75),
                EnrichedWord("on", 0.7),
                EnrichedWord("mulberry", 0.65),
                EnrichedWord("street", 0.6)),
      2 -> List(EnrichedWord("horton", 1.0),
                EnrichedWord("hatches", 0.95),
                EnrichedWord("the", 0.9),
                EnrichedWord("egg", 0.85)),
      3 -> List(EnrichedWord("yertle", 1.0),
                EnrichedWord("the", 0.9),
                EnrichedWord("turtle", 0.8)),
      4 -> List(EnrichedWord("green", 1.0),
                EnrichedWord("eggs", 0.95),
                EnrichedWord("and", 0.9),
                EnrichedWord("ham", 0.85)))

    val clusterModel = new ClusterModel(nClusters,
                                        centers, 
                                        context.parallelize(assignments))

    val topicModel = TopicModel(clusterModel,
                                enrichedWords,
                                0.5)

    topicModel.save(tempFile.toString)

    assert(tempFile.exists === true)
  }
}
