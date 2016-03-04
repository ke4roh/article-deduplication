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

import org.apache.spark.mllib.linalg.SparseVector

import org.json4s._
import org.json4s.native.JsonMethods._

import java.io._
import scala.io.Source

import scala.reflect.ClassTag

object IOFuncs {
  def writeVector(vector : SparseVector,
                  flname : String) = {
    val pw = new PrintWriter(flname)
    vector.indices.map {
      idx =>
        val value = vector(idx)
        pw.print(idx)
        pw.print("\t")
        pw.println(value)
    }
    pw.close()
  }

  def readDuplicateSets(filename : String) : Seq[Seq[String]] = {
    implicit val formats = DefaultFormats

    val duplicateText = Source.fromFile(filename).getLines.mkString
    val duplicateSets : Seq[Seq[String]] = parse(duplicateText).extract[Seq[Seq[String]]]

    duplicateSets
  }
}
