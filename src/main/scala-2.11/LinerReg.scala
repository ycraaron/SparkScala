/**
  * Created by Aaron on 16/9/16.
  */
/**
  * Created by Aaron on 16/9/16.
  */

// General purpose library
import org.apache.spark
import org.apache.spark.ml.regression.LinearRegression

import scala.xml._

// Spark data manipulation libraries
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// Spark machine learning libraries
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.Pipeline
import org.apache.spark.{SparkContext,SparkConf}



object LinerReg {
  def main (args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("LinearRegressionWithElasticNetExample")
      .getOrCreate()

    val training = spark.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(training)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


  }
}
