/**
  * Created by Aaron on 16/9/16.
  */
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object Kmeans {
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark IDEA Test")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)
    val data = sc.textFile("data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    println(parsedData.collect())

    val numClusters = 2
    val numIterations = 20

    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val WSSSE = clusters.computeCost(parsedData)

    println("Within Set Sum of Squared Errors = " + WSSSE)
    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
  }
}
