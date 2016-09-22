/**
  * Created by Aaron on 16/9/23.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object HDFSTest {
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark IDEA Test")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("hdfs://localhost:9000/user/aaron/README.md")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.foreach(println)
  }


}
