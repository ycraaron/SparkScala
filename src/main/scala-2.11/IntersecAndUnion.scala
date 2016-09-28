import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Aaron on 16/9/28.
  */


object IntersecAndUnion {
  def main(args: Array[String]) {
    val spConf = new SparkConf().setMaster("local[2]").setAppName("ItersectionList")
    val sc = new SparkContext(spConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val a = Array(
      (1, List(1, 2, 3, 4, 5)),
      (2, List(1, 6, 7, 8, 9)),
      (3, List(2, 3, 4, 5, 6))
    )

    val t2 = List(2, 5)

    var t1 = sc.makeRDD(a).map(x => (x._1, x._2))
    t1.foreach(println)
//    t1 = t1.reduceByKey(_++_)
//    t1.foreach(println)
    //val t1 = sc.makeRDD(a).map(x => (x._1, (List(x._2)))).reduceByKey(_ ++ _)
    val t3intersect = t1.map(x => (x._1, (x._2.intersect(t2))))
    val t3union = t1.map(x => (x._1, (x._2.union(t2).distinct)))

    t3intersect.foreach(println)
    t3union.foreach(println)

  }

}
