/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// * Preamble 

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.regression.GeneralizedLinearRegression
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating generalized linear regression.
 * Run with
 * {{{
 * bin/run-example ml.GeneralizedLinearRegressionExample
 * }}}
 */

object GeneralizedLinearRegressionExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("GeneralizedLinearRegressionExample")
      .getOrCreate()

    // $example on$

    // ** Load training data
    val sp = sys.env.get("SPARK_HOME")

    def fgen0 (b0: String)(f0: String) = { s"file:///$b0/$f0" } 

    val fgen1 = fgen0(sp.get)(_)

    val dataset = spark.read.format("libsvm").load(fgen1("data/mllib/sample_linear_regression_data.txt"))

    // ** Paste this because of the multi-line

    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)

    // Fit the model
    val model = glr.fit(dataset)

    // *** Note

    // **** Print the coefficients and intercept for generalized linear regression model
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")

    // *** Summarize the model over the training set and print out some metrics
    val summary = model.summary

    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals().show()
    // $example off$

    // ** Finish
    spark.stop()
  }
}
// scalastyle:on println

// * Postamble

// The following are the file variables.

// Local Variables:
// mode:scala
// scala-edit-mark-re: "^[ \\t]*// [\\*]+ "
// comment-column:50 
// comment-start: "// "  
// comment-end: "" 
// eval: (outline-minor-mode)
// outline-regexp: "// [*]+"
// eval: (auto-fill-mode)
// fill-column: 85 
// End: 
