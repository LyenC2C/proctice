package parse

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import java.util
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

/**
  * Created by lyen on 17-6-27.
  */
class JsonUtils {


}

object JsonUtils extends App {
  val file = "/home/lyen/data/weibo.json_1"
  val conf = new SparkConf().setAppName("parseJson").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val data = sc.textFile(file)
  data.flatMap { js => parseJson(js) }.foreach(println)


  def parseJson(js: String) = {
    val ob = json2Map(js)
    val uid = ob.getOrElse("uid", None)
    val orders: JSONArray = ob.getOrElse("orders", None).asInstanceOf[JSONArray]
    jsonArray2OrdersInfo(orders).map(ab => uid + "\001" + ab.asInstanceOf[ArrayBuffer[AnyRef]].mkString("\001"))
  }

  def json2Map(json: String): Map[String, AnyRef] = {
    JSON.parseObject(json).asInstanceOf[util.Map[String, AnyRef]].asScala
  }

  def jsonArray2OrdersInfo(orders: JSONArray, exceptStr: String = "goodsInfo"): ArrayBuffer[AnyRef] = {
    val records = ArrayBuffer[AnyRef]()
    for (i <- 0 until orders.size()) {
      //订单公共数据(orders中除goodsInfo以外的数据)
      val orderInfo = ArrayBuffer[AnyRef]()
      val map = orders.getJSONObject(i).asInstanceOf[util.Map[String, AnyRef]].asScala
      for (k <- map.keys if k != exceptStr) orderInfo += map.getOrElse(k, None)
      //订单商品数据(goodsInfo)
      val goodsInfo = map.getOrElse(exceptStr, None).asInstanceOf[JSONArray]
      jsonArray2GoodsInfo(goodsInfo, orderInfo, records)
    }
    records
  }

  def jsonArray2GoodsInfo(goodInfo: JSONArray, orderInfo: ArrayBuffer[AnyRef], record: ArrayBuffer[AnyRef]) = {
    for (i <- 0 until goodInfo.size()) {
      val goodsInfo = ArrayBuffer[AnyRef]()
      val map = goodInfo.getJSONObject(i).asInstanceOf[util.Map[String, AnyRef]].asScala
      for (k <- map.keys) goodsInfo += map.getOrElse(k, None)
      goodsInfo ++= orderInfo
      record += goodsInfo
    }
  }

  /*
  def jsonArray2OrdersInfo(jsArray: JSONArray, exceptStr: String = "goodsInfo"): mutable.ArrayBuffer[AnyRef] = {
    val doubleList = mutable.ArrayBuffer[AnyRef]()
    for (i <- 0 until jsArray.size()) {
      //订单公共数据(orders中除goodsInfo以外的数据)
      val list = mutable.ArrayBuffer[AnyRef]()
      val map = jsArray.getJSONObject(i).asInstanceOf[util.Map[String, AnyRef]].asScala
      for (k <- map.keys if k != exceptStr) list += map.getOrElse(k, None)
      //订单商品数据(goodsInfo)
      val goodsInfo = map.getOrElse(exceptStr, None).asInstanceOf[JSONArray]
      val goods = jsonArray2GoodsInfo(goodsInfo)
      goods.foreach { good => good ++= list; doubleList += good }
    }
    doubleList
  }

  def jsonArray2GoodsInfo(jsArray: JSONArray): mutable.ArrayBuffer[mutable.ArrayBuffer[AnyRef]] = {
    val finalList = mutable.ArrayBuffer[mutable.ArrayBuffer[AnyRef]]()
    for (i <- 0 until jsArray.size()) {
      val list = mutable.ArrayBuffer[AnyRef]()
      val map = jsArray.getJSONObject(i).asInstanceOf[util.Map[String, AnyRef]].asScala
      for (k <- map.keys) list += map.getOrElse(k, None)
      finalList += list
    }
    finalList
  }
  */

}
