import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import java.util
import scala.collection.mutable.Map

import org.elasticsearch.spark.sql._


object SparkEs2 extends App {
  val spark = SparkSession
    .builder()
    .config("es.nodes", "localhost")
    .config("es.port", "9200")
    .config("es.index.auto.create", "false")
    .appName("spark-es")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val data = spark.sparkContext.textFile("hdfs://master:9000/data/weibo.json")
  val records = data.map { d =>
    val ob = JSON.parseObject(d)
    val uid = ob.getString("uid")
    val orders = ob.getJSONArray("orders")
    for (i <- 0 until orders.size()) {
      val order: Map[String, Any] = orders.getJSONObject(i).asInstanceOf[util.Map[String, Any]].asScala
      val ofields = Array("postFee", "actualFee")
      stdMoney(order, ofields: _*)
      val goodsInfo = order.get("goodsInfo").get
      val goodsInfoJSONArray = JSON.parseArray(goodsInfo.toString)
      val gfields = Array("originalPrice", "promotionPrice")
      for (j <- 0 until goodsInfoJSONArray.size()) {
        val good: Map[String, Any] = goodsInfoJSONArray.getJSONObject(j).asInstanceOf[util.Map[String, Any]].asScala
        stdMoney(good, gfields: _*)
      }
      order.update("goodsInfo", goodsInfoJSONArray)
    }
    val map = Map("orders" -> orders, "uid" -> uid)
        map
    /*
    val map = new java.util.HashMap[String, AnyRef]
    map.put("orders", orders)
    map.put("uid", uid)
    val jsonStr = new JSONObject(map).toJSONString
    jsonStr
  */
  }.coalesce(15)

  val s = Map("es.index.auto.create" -> "true")
  records.saveToEs /*saveJsonToEs*/ ("r/g", s)


  def stdMoney(m: Map[String, Any], fields: String*) = {
    fields.map { field =>
      m.get(field) match {
        case Some(v) =>
          val sv = v.toString
          m.update(field, if (sv.contains("￥")) sv.replace("￥", "").toDouble else 0.0d)
        case None =>
      }
    }

  }

  val input = spark.sparkContext.parallelize(List("""{"name": "jiajieshi yagao", "desc": "youxiao fangzhu", "date":"2017-7-14",price": 25, "producer": "jiajieshi producer", "tags":["fangzhu"] }"""))
  val cfg = Map("date" -> "")
  val emc = spark.read.json(input).saveToEs("ecommerce_cus/product_cus")

}