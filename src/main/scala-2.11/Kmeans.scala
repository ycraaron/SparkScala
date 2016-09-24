/**
  * Created by Aaron on 16/9/16.
  */
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Kmeans {

  def main (args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark K-means HDFS Clustering")
      .set("spark.executor.memory", "3g")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)
    val numClusters = 5
    val numIterations = 20
    var clusterIndex: Int = 0

    val training_data = sc.textFile("hdfs://localhost:9000/user/aaron/training_set.csv")
    val parsedTrainingData = training_data.filter(!isColumnNameLine(_)).map(line => {
      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    }).cache()

    //print(data.collect())
    //data.foreach(println)
    //parsedTrainingData.foreach(println)
    //[2.0,3.0,12669.0,9656.0,7561.0,214.0,2674.0,1338.0]
    //[2.0,3.0,7057.0,9810.0,9568.0,1762.0,3293.0,1776.0]
    //[2.0,3.0,6353.0,8808.0,7684.0,2405.0,3516.0,7844.0]
    val clusters: KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })
    val WSSSE = clusters.computeCost(parsedTrainingData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    val test_data = sc.textFile("hdfs://localhost:9000/user/aaron/test_set.csv")
    val parsedTestData = test_data.filter(!isColumnNameLine(_)).map(line => {
      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    })
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex:
      Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " +
        predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.")

    val ks:Array[Int] = Array(3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
    ks.foreach(cluster => {
      val model:KMeansModel = KMeans.train(parsedTrainingData, cluster,30,1)
      val WSSE = model.computeCost(parsedTrainingData)
      println("sum of squared distances of points to their nearest center when k=" + cluster + " -> "+ WSSE)
    })

    //clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    //val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
  }

  private def isColumnNameLine(line:String):Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}
