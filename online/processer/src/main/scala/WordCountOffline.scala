import org.apache.spark.rdd.RDD

/**
  * Created by lyen on 17-6-21.
  */
class WordCountOffline extends OfflineSkeleton {

  override def compute(rdd: RDD[String]) = {
    rdd.map(f => (f, 1)).reduceByKey(_ + _).foreach(println)
  }
}

object WordCountOffline extends App {

  val wordCountOffline = new WordCountOffline
  //  wordCountOffline.execute(Array[String]("-help","-dir", "hdfs://master:9000/uv", "-S", "20170621", "-E", "20170621", "-U", "day", "-master", "local[*]", "-name", "wordcount"))
  //  wordCountOffline.execute(Array[String]("-help", "-dir", "hdfs://master:9000/uv", "-U", "day", "-master", "local[*]", "-name", "wordcount"))

}
