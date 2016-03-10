package com.redhat.et.dedup

import org.apache.spark.rdd._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}

import org.scalatest._

case class Article(article_id : Long, text : String)

class TextCleanerSpec extends FlatSpec with Matchers with PerTestSparkContext {

  val testArticles = Seq(Article(1L, "one fish two fish 525 'quote' test"),
                         Article(2L, "red fish blue fish 725 'quote' test"))

  val filterWords = Set("fish", "test")
  
  val replacementWords = Map("one" -> "ten",
                             "red" -> "green")

  val expectedWords1 = Set((1L, Seq("ten", "two", "quote")),
                           (2L, Seq("green", "blue", "quote")))

  val expectedVocab1 = Set("ten", "two", "green", "blue", "quote")

  val expectedWords2 = Set((1L, Seq("ten,two", "two,quote")),
                           (2L, Seq("green,blue", "blue,quote")))

  val expectedVocab2 = Set("ten,two", "two,quote", "green,blue", "blue,quote")


  "TextCleaner.cleanedText" should "produce windows of length 1" in {
    val sql = sqlContext

    import sql.implicits._

    val articlesDF = context.parallelize(testArticles).toDF()

    val cleaner = new TextCleaner(filterWords, replacementWords,
                                  articlesDF, None, None, 1)

    val cleanedText = cleaner.cleanedText.collect()

    assert(cleanedText.toSet === expectedWords1)

    val vocab = cleaner.extractedVocab.keySet

    assert(vocab == expectedVocab1)
  }

  "TextCleaner.cleanedText" should "produce windows of length 2" in {
    val sql = sqlContext

    import sql.implicits._

    val articlesDF = context.parallelize(testArticles).toDF()

    val cleaner = new TextCleaner(filterWords, replacementWords,
                                  articlesDF, None, None, 2)

    val cleanedText = cleaner.cleanedText.collect()

    assert(cleanedText.toSet === expectedWords2)

    val vocab = cleaner.extractedVocab.keySet

    assert(vocab == expectedVocab2)
  }

}
