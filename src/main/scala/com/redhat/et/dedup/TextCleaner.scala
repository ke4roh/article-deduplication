package com.redhat.et.dedup

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source
import scala.util.Try

object IOUtils {
  def readJSON(filterWordsFilename : String, replaceWordsFilename : String) : (Set[String], Map[String, String]) = {
    implicit val formats = DefaultFormats

    val filterText = Source.fromFile(filterWordsFilename).getLines.mkString
    val filterWords : Set[String] = parse(filterText).extract[List[String]].toSet
    
    val replaceText = Source.fromFile(replaceWordsFilename).getLines.mkString
    val replaceWords : Map[String, String] = parse(replaceText).extract[Map[String, String]]
    
    (filterWords, replaceWords)
  }
}

object TextCleanerFunctions {
  def cleanText(filteredWords : Set[String],
                replacementWords: Map[String, String],
                articles : DataFrame,
                minWordCounts : Option[Int],
                maxWordCounts : Option[Int],
                windowSize : Int) : RDD[(Long, Seq[String])] = {

    val sc = articles.sqlContext.sparkContext

    val rdd = articles.rdd.map {
      row =>
      (row.getAs[Long]("article_id"),
       row.getAs[String]("text"))
      }

    val replaceWordsBC = sc.broadcast(replacementWords)
    val cleaned = rdd.mapValues {
      case text =>
        val replaceWords = replaceWordsBC.value
        text.trim()
          .toLowerCase()
          .split(Array(' ', '\t'))
          .map ( w => w.stripPrefix("'").stripSuffix("'") )
          .filter( s => s.length != 0 )
          .map ( w => replaceWords.getOrElse(w, w) )
          .filter( s => !filteredWords.contains(s) )
          .filter( s => Try(s.toUpperCase.toLong).isFailure )
          .filter( s => Try(java.lang.Long.parseLong(s.toUpperCase, 16)).isFailure)
          .filter( s => !s.startsWith("0x") )
          .filter( s => !s.startsWith("ffff") )
          .sliding( windowSize )
          .map { shingle => shingle.mkString(",") }
          .toSeq
      }
      // HACK for filtering out test solutions.
      .filter {
        case (articleId, words) =>
          ! (words.size < 10 && words.contains("test"))
      }

    cleaned.cache()

    val filteredBottom = if (minWordCounts.isDefined) {
      val bottomWords = getBottomWords(cleaned, minWordCounts.get)
      println("Bottom words " + bottomWords.size)

      val bottomWordsBC = sc.broadcast(bottomWords)
      val filtered = cleaned.map {
        case (articleId, words) =>
          (articleId, 
           words.filter( w => !bottomWordsBC.value.contains(w) ))
      }

      filtered.cache()

      cleaned.unpersist()

      filtered
    } else {
      cleaned
    }

    val filteredTop = if (maxWordCounts.isDefined) {
      val topWords = getTopWords(filteredBottom, maxWordCounts.get)
      println("Top words " + topWords.size)

      val topWordsBC = sc.broadcast(topWords)
      val filtered = filteredBottom.map {
        case (articleId, words) =>
          (articleId,
           words.filter( w => !topWordsBC.value.contains(w) ))
      }

      filtered.cache()

      filteredBottom.unpersist()

      filtered
    } else {
      filteredBottom
    }

    filteredTop
  }

  def getTopWords(cleanedText : RDD[(Long, Seq[String])], threshold : Int) : Set[String] = {
    cleanedText.flatMap {
      case (articleId, words) =>
        words.map { w => (w, 1) }
    }
    .reduceByKey(_ + _)
    .filter {
      case (word, count) => count > threshold
    }
    .map {
      case (word, count) => word
    }
    .collect()
    .toSet
  }

  def getBottomWords(cleanedText : RDD[(Long, Seq[String])], threshold : Int) : Set[String] = {
    cleanedText.flatMap {
      case (articleId, words) =>
        words.map { w => (w, 1) }
    }
    .reduceByKey(_ + _)
    .filter {
      case (word, count) => count < threshold
    }
    .map {
      case (word, count) => word
    }
    .collect()
    .toSet
  }

  def extractVocab(cleanedText : RDD[(Long, Seq[String])]) : Map[String, Long] = {
    val vocab : Map[String, Long] = cleanedText.flatMap {
      case (articleId, words) =>
        words
      }
      .distinct()
      .zipWithIndex()
      .collect()
      .toMap

    vocab
  }
}

class TextCleaner(filteredWords : Set[String],
                  replacementWords: Map[String, String],
                  articles : DataFrame,
                  minWordCounts : Option[Int],
                  maxWordCounts : Option[Int],
                  windowSize : Int) {

  lazy val cleanedText : RDD[(Long, Seq[String])] = {
    TextCleanerFunctions.cleanText(filteredWords, replacementWords,
                                   articles, minWordCounts, maxWordCounts,
                                   windowSize)

  }

  lazy val extractedVocab : Map[String, Long] = {
    TextCleanerFunctions.extractVocab(cleanedText)
  }

}

object TextCleaner {
  def apply(sc: SparkContext,
            filteredWordsFilename : String,
            replacementWordsFilename : String,
            articlesFilename : String,
            minWordCounts : Option[Int],
            maxWordCounts : Option[Int],
            windowSize : Int) : TextCleaner = {
    val (filteredWords, replacementWords) = IOUtils.readJSON(filteredWordsFilename,
                                                             replacementWordsFilename)

    val sql = new SQLContext(sc)
    val articles = sql.jsonFile(articlesFilename).cache()

    new TextCleaner(filteredWords, replacementWords, articles, minWordCounts, maxWordCounts,
                  windowSize)
  }
}
