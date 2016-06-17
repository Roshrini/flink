/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.feature_extraction

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{LabeledVector, ParameterMap}
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.ml.pipeline.{TransformOperation, FitOperation, Transformer}
import org.apache.flink.util.Collector

class CountVectorizer extends Transformer[CountVectorizer] {

  import CountVectorizer._

  var dictionary: Option[DataSet[Map[String, Int]]] = None
  var start = 1
  var end = 1

  def setInputCol(inputCol: String): CountVectorizer =  {
    parameters.add(InputCol, inputCol)
    this
  }

  def setOutputCol(outputCol: String): CountVectorizer =  {
    parameters.add(OutputCol, outputCol)
    this
  }

  def setVocabSize(vocabSize: Int): CountVectorizer =  {
    parameters.add(VocabSize, vocabSize)
    this
  }

  def setMinDF(minDF: Double): CountVectorizer =  {
    parameters.add(MinDF, minDF)
    this
  }

  def setMinTF(minTF: Double): CountVectorizer =  {
    parameters.add(MinTF, minTF)
    this
  }

  def setStopWords(stopWords: List[String]): CountVectorizer =  {
    parameters.add(StopWords, stopWords)
    this
  }

  def setNgramRange(ngramRange: List[Int]): CountVectorizer =  {
    parameters.add(NgramRange, ngramRange)
    this.start = ngramRange(0)
    this.end = ngramRange(1)
    this
  }

  def get_feature_names() : DataSet[Map[String,Int]] = {
    this.dictionary match {
      case Some(dic) => dic
      case None => throw new RuntimeException("CountVectorizer was not trained.")
    }
  }
}

object CountVectorizer {

  import org.apache.flink.ml.common.Parameter
  // ========================================== Parameters =========================================

  case object InputCol extends Parameter[String] {
    val defaultValue: Option[String] = None
  }

  case object OutputCol extends Parameter[String] {
    val defaultValue: Option[String] = None
  }

  case object VocabSize extends Parameter[Int] {
    val defaultValue = Some(10)
  }
  case object MinDF extends Parameter[Double] {
    val defaultValue = Some(1.0)
  }
  case object MinTF extends Parameter[Double] {
    val defaultValue = Some(1.0)
  }
  case object StopWords extends Parameter[List[String]] {
    val defaultValue: Option[List[String]] = None
  }
  case object NgramRange extends Parameter[List[Int]] {
    val defaultValue: Option[List[Int]] = None
  }
  var start =1
  var end =1
  // ========================================== Factory methods ====================================

  def apply(): CountVectorizer = {
    new CountVectorizer
  }

  // ========================================== Operations =========================================


  implicit val fitDictionary = new FitOperation[CountVectorizer, String] {
    override def fit(
                      instance: CountVectorizer,
                      fitParameters: ParameterMap,
                      input: DataSet[String])
    : Unit = {

      val resultingParameters = instance.parameters ++ fitParameters
      val minDF = resultingParameters.get(MinDF) match {
        case Some(value) => value
        case None => input.getParallelism
      }
      val stopWords: List[String] = resultingParameters.get(StopWords) match {
        case Some(value) => value
        case None => List[String]()
      }

      val nGramRange: List[Int] = resultingParameters.get(NgramRange) match {
        case Some(value) => value
        case None => List[Int]()
      }

      val result = trainDictionary(input,minDF,stopWords, nGramRange)
      instance.dictionary = Some(result)
    }
  }

  implicit val fitDictionaryLabeledData = new FitOperation[CountVectorizer, (Double, String)] {
    override def fit(instance: CountVectorizer, fitParameters: ParameterMap, input: DataSet[
      (Double, String)]): Unit = {
      val resultingParameters = instance.parameters ++ fitParameters
      val minDF = resultingParameters.get(MinDF) match {
        case Some(value) => value
        case None => input.getParallelism
      }

      val stopWords: List[String] = resultingParameters.get(StopWords) match {
        case Some(value) => value
        case None => List[String]()
      }

      val nGramRange: List[Int] = resultingParameters.get(NgramRange) match {
        case Some(value) => value
        case None => List[Int]()
      }

      val strippedInput = input.map(x => x._2)
      instance.dictionary = Some(trainDictionary(strippedInput,minDF,stopWords,nGramRange ))

    }
  }


  private def concatenate(input : Seq[Tuple1[String]], n : Int) : String = {
    var str = ""
    for(i <- 0 to n-1)
    {
      str = str + input(i)._1+" "
    }
    val output = str.reverse.dropWhile(_ == ' ').reverse
    return output
  }

  private def trainDictionary(input: DataSet[String], minDF: Double, stopWords: List[String], nGramRange: List[Int]): DataSet[Map[String, Int]] = {
    val result = input.flatMap {
      text => {
        (for( i <- nGramRange(0) to nGramRange(1)) yield
        """\b\w+\b""".r.findAllIn(text).map(x => new Tuple1(x.toLowerCase)).filter(w => w._1.length > minDF)
          .filter(w => !stopWords.contains(w._1)).sliding(i).toList.map(x => concatenate(x,i))).flatMap(x => x)
          .map(x => new Tuple1(x))

//          """\b\w+\b""".r.findAllIn(text).map(x => new Tuple1(x.toLowerCase)).filter(w => w._1.length > minDF)
//          .filter(w => !stopWords.contains(w._1)).sliding(2).toList.map(x => x(0)._1+" "+x(1)._1).map(x => new Tuple1(x))
      }
    }.distinct(0)
      .reduceGroup{
        (words, coll: Collector[Map[String, Int]]) => {
          val set = scala.collection.mutable.HashSet[String]()
          words.foreach {
              word =>
               set += word._1
          }
          coll.collect(set.iterator.zipWithIndex.toMap)
        }
      }
    result
  }

  implicit val transformText = new TransformOperation[
    CountVectorizer,
    Map[String, Int],
    String,
    SparseVector]
  {/** Retrieves the model of the [[Transformer]] for which this operation has been defined.
    *
    * @param instance
    * @param transformParemters
    * @return
    */

  override def getModel(instance: CountVectorizer, transformParemters: ParameterMap):
  DataSet[Map[String, Int]] = {
  // println("r", instance.start )
    //  println(instance.end )
    start = instance.start
    end = instance.end

    instance.dictionary match {
      case Some(dic) => dic
      case None => throw new RuntimeException("CountVectorizer was not trained.")
    }
  }
    /** Transforms a single element with respect to the model associated with the respective
      * [[Transformer]]
      *
      * @param element
      * @param model
      * @return
      */

    override def transform(element: String, model: Map[String, Int]): SparseVector = {
      transformTextElement(element, model)
    }

  }

  implicit val transformLabeledText = new TransformOperation[
    CountVectorizer,
    Map[String, Int],
    (Double, String),
    LabeledVector]
  {/** Retrieves the model of the [[Transformer]] for which this operation has
    * been defined.
    *
    * @param instance
    * @param transformParemters
    * @return
    */
  override def getModel(instance: CountVectorizer, transformParemters: ParameterMap):
  DataSet[Map[String, Int]] = {
    instance.dictionary match {
      case Some(dic) => dic
      case None => throw new RuntimeException("CountVectorizer was not trained.")
    }
  }

    /** Transforms a single element with respect to the model associated with the respective
      * [[Transformer]]
      *
      * @param element
      * @param model
      * @return
      */
    override def transform(element: (Double, String), model: Map[String, Int]): LabeledVector = {
      LabeledVector(element._1, transformTextElement(element._2, model ))
    }
  }

  private def transformTextElement(text: String, model: Map[String, Int]): SparseVector = {
    val coo = (for( i <- start to end) yield """\b\w+\b""".r.findAllIn(text).map(x => new Tuple1(x.toLowerCase)).sliding(i)
      .toList.map(x => concatenate(x,i))).flatMap(x => x).flatMap{
      word => {
        model.get(word.toLowerCase) match {
          case Some(id) => Some(id, 1.0)
          case None => None
        }
      }
    }
    SparseVector.fromCOO(model.size, coo.toIterable)
  }
}
