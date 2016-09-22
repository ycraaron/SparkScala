/**
  * Created by Aaron on 16/9/16.
  */

import org.apache.spark.{SparkContext, SparkConf}

object WordCount {
  def main (args: Array[String]): Unit ={
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark IDEA Test")
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val lines = sc.parallelize(Seq("First mo jie line", "Second ying line", "Third yang cheng rui jie line"))
    val counts = lines.flatMap(line=>line.split(" "))
      .map(word=>(word,1))
      .reduceByKey(_+_)
    counts.foreach(println)
  }
}