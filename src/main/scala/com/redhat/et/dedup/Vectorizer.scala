/*
 * Copyright (c) 2015 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redhat.et.dedup

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.clustering._

import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector}

import java.io._

import com.redhat.et.silex.util._

import scala.util.Try

object Vectorizer {
  type Username = String
  type URL = String
  type Count = Long
  type UsernameIdx = Long
  type URLIdx = Long

  def toIndices(rdd : RDD[(Username, URL, Count)]) : (RDD[(Username, Long)],
                                   RDD[(URL, Long)],
                                   RDD[(UsernameIdx, URLIdx, Count)]) = {
    
    // assign each Username and URL an index
    val usernameIds : RDD[(Username, UsernameIdx)] = rdd.map{_._1}.distinct.zipWithIndex
    val urlIds : RDD[(URL, URLIdx)] = rdd.map{_._2}.distinct.zipWithIndex

    // convert username and URL to indices
    val byIdx = rdd.map {
      case (username, url, count) =>
        (url, (username, count))
    }.join(urlIds)
    .map { 
      case (url, ((username, count), urlIdx)) =>
        (username, (urlIdx, count))
    }.join(usernameIds)
    .map {
      case (username, ((urlIdx, count), usernameIdx)) =>
        (usernameIdx, urlIdx, count)
    }

    (usernameIds, urlIds, byIdx)
  }

  def wordCountsToVectors(cleanedText : RDD[(Long, Seq[String])], 
                          vocab : Map[String, Long]) : FeatureMatrix = {

    val sc = cleanedText.context

    val vocabInt = vocab.mapValues { _.toInt }

    val vocabBC = sc.broadcast(vocabInt)
    val labeledVectors = cleanedText.map {
      case (articleId, words) =>
        val wordCounts : Map[String, Int] = words.foldLeft(Map.empty[String, Int]) {
          case (counts : Map[String, Int], word : String) =>
            counts.+((word, counts.getOrElse(word, 0) + 1))
        }

        val vocab : Map[String, Int] = vocabBC.value
        
        val wordTuples : Seq[(Int, Int)] = wordCounts.toSeq
          .map {
            case (word : String, count : Int) =>
              (vocab.get(word), count)
          }
          .filter {
            case (option, count) =>
              option.isDefined
          }
          .map {
            case (option, count) =>
              (option.get, count)
          }
          .sortBy {
            case (idx, count) =>
              idx
          }

        val ids = wordTuples.map { _._1.toInt }.toArray
        val counts = wordTuples.map { _._2.toDouble }.toArray

        (articleId.toString, new SparseVector(vocab.size, ids, counts))
    }

    val featureLabels = sc.parallelize(vocabInt.toSeq.map(_.swap))

    new FeatureMatrix(labeledVectors, featureLabels)
  }
}
