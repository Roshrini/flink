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



package org.apache.flink.ml.feature
import scala.io.Source
import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}
import scala.collection.immutable.ListMap

class FeatureTestSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "The CountVectorizer"

  it should "vectorize text" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

//    val trainingData = Seq(
//
//      Source.fromFile("/Users/roshaninagmote/Downloads/data/austen-bronte/Austen_Pride.txt").getLines().mkString,
//      Source.fromFile("/Users/roshaninagmote/Downloads/data/austen-bronte/Austen_Sense.txt").getLines().mkString   )

    //
    //    val trainingData = Seq(
    //      "This This is the first and second  document document document.",
    //         "This is the second second document.",
    //         "And the third one.",
    //         "Is this the first document?" )

        val trainingData = Seq(
          "This This is the first and second  document document document"
        )
//
//        val trainingData = Seq(
//          Source.fromFile("/Users/roshaninagmote/sample.txt").getLines().mkString   )


    val trainingDataDS = env.fromCollection(trainingData)
    val cv = CountVectorizer().setNgramRange(List(1,2))
    cv.fit(trainingDataDS)

    // cv.transform(trainingDataDS)
    val result = cv.transform(trainingDataDS).collect()

    println(result.mkString("\n"))
//   val result = cv.transform(trainingDataDS).flatMap(x => x).collect().toList

   // println(result.unzip._2)

    val words = cv.get_feature_names()
    println(words.collect())
    //   ListMap(words.toSeq.sortBy(_._1):_*)
    val checktype = words.collect().toList
    val data = ListMap(checktype.head.toSeq.sortBy(_._2):_*)
    println(data.keys.toList)

    //    val cvModel = new CountVectorizer()
    //      .setInputCol("words")
    //      .setOutputCol("features")
    //      .setVocabSize(3)
    //      .setMinDF(2)
    //
    //    cv.fit(trainingDataDS)
    //
    // val result =  cv.transform(trainingDataDS).collect()
    //
    //    println(result.mkString("\n"))

    //    val result =  cv.get_feature_values(trainingDataDS).collect()
    //
    //    println(result.mkString("\n"))

  }
}
