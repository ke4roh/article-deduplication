package com.redhat.et.dedup

import org.apache.spark.rdd._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}

import org.scalatest._

class VectorizerSpec extends FlatSpec with Matchers with PerTestSparkContext {

  val vocab = Map("one" -> 0L, "fish" -> 1L, "two" -> 2L, "red" -> 3L, "blue" -> 4L)

  val text = Seq(0L -> Seq("one", "fish", "two", "fish", "dog"),
                 1L -> Seq("red", "fish", "blue", "fish", "cat"))

  "Vectorizer.wordCountsToVectors" should "produce proper counts" in {
    val textRDD = context.parallelize(text)

    val features = Vectorizer.wordCountsToVectors(textRDD, vocab)

    val wordCounts = features.labeledVectors
      .collect()
      .map { _._2 }

    // dog and cat should be removed
    assert(wordCounts(0).size === vocab.size)
    assert(wordCounts(0).indices === Array(0, 1, 2))
    assert(wordCounts(0).values === Array(1, 2, 1))

    assert(wordCounts(0).size === vocab.size)
    assert(wordCounts(1).indices === Array(1, 3, 4))
    assert(wordCounts(1).values === Array(2, 1, 1))
    
  }

}
