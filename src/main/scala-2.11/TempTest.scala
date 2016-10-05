import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

/**
  * Created by Aaron on 16/10/4.
  */
object TempTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark HDFS Test")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val textFile = sc.textFile("hdfs://localhost:9000/user/aaron/README.md")
    val textFile2 = sc.textFile("hdfs://localhost:9000/user/aaron/README.md",150)


    val list:List[String] = List("this is a test","how are you","I love you!","Where are you?")

    val lines = sc.parallelize(list)
    //Is this actually building a JavaPairRDD?
    val map = lines.map(line=>(line.split(" "){0},line))


  }

}
