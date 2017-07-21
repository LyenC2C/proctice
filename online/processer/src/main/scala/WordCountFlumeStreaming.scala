import config.StreamingConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import redisclient.DefaultJedisPoolClient
import serializer.KryoInputSerializer

/**
  * Created by lyen on 17-6-20.
  */
class WordCountFlumeStreaming extends FlumeStreamingSkeleton {
  override def compute(dstream: DStream[String]): Unit = {
    //    val wordcount = dstream.map { f => val temp = f.split("\001"); (temp(3), 1) }.reduceByKeyAndWindow(_ + _, Seconds(10))
    val wordcount = dstream.map { f => val temp = f.split("\001"); temp(2) }

    wordcount.foreachRDD { rdd => rdd.foreachPartition { data =>
      //      val conf = new StreamingConf()
      val pool = new DefaultJedisPoolClient("127.0.0.1" /*conf.getStringKey("redis.name", null)*/ ,
        6379 /*conf.getIntegerKey("redis.port", 6379)*/ ,
        100, 2, true, new KryoInputSerializer())
      data.foreach { age =>
        pool.hincrBy("age_count", age, 1)
      }
    }
    }
  }

}

object WordCountFlumeStreaming extends App {
  val wc = new WordCountFlumeStreaming()
  //  wc.execute(Array[String]("-config", "hdfs://master:9000/conf.properties", "-help", "-master", "local[*]", "-name", "flumeStreaming", "-I", "10"))

  wc.execute(Array[String]("-config", "hdfs://master:9000/conf.properties", "-help", "-master", "local[*]", "-name", "streaming_wordcount"))

  /**
    * 可考虑全部用配置文件实现
    *wc.execute(Array[String]("-config", "hdfs://master:9000/conf.properties"))
    */
}
