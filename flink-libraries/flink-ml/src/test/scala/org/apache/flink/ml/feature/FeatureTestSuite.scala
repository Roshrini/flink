package org.apache.flink.ml.feature_extraction

/**
  * Created by roshaninagmote on 6/8/16.
  */

import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}
import scala.io.Source
import org.apache.flink.ml.math.SparseMatrix
import org.apache.flink.ml.classification.SVM

class FeatureTestSuite
  extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "The CountVectorizer"

  it should "vectorize text" in {
    import org.apache.flink.ml.pipeline.Transformer
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    val trainingData = Seq(
    //
    //      Source.fromFile("/Users/roshaninagmote/Downloads/data/austen-brontë/Austen_Pride.txt").getLines().mkString,
    //      Source.fromFile("/Users/roshaninagmote/Downloads/data/austen-brontë/Austen_Sense.txt").getLines().mkString   )
    //

    val testData = Seq(
      "hi this is roshani",
      "who are you",
      "I am priyanshu's document"
       )

//    val trainingData = Seq(
//      "This This is the first and second  document document document.",
//         "This is the second second document.",
//         "And the third one.",
//         "Is this the first document?" )

    val trainingData = Seq(
      "roshan the sky sun is very bright.", "child sky blue blue blue ros","is good is good girl"
    )
    val trainingDataDS = env.fromCollection(trainingData)
    val testDataDS = env.fromCollection(testData)
    val cv = CountVectorizer().setNgramRange(List(1,2))
    cv.fit(trainingDataDS)

    println(cv.get_feature_names().collect())

    // cv.transform(trainingDataDS)
    val result = cv.transform(trainingDataDS).collect()
    println(result.mkString("\n"))


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
