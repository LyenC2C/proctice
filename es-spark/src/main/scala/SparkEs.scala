import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

case class Record(uid: String, goodsSize: String, goodsName: String,
                  promotionPrice: String, originalPrice: String, goodsNum: String,
                  actualFee: String, shopName: String, postFee: String, shopNick: String, shopId: String, orderTime: String,
                  zfbNum: String, orderStatus: String,
                  orderTimeout: String, quantity: String)

object SparkEs extends App {
  val spark = SparkSession
    .builder()
    .appName("spark-es")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val data = spark.sparkContext.textFile("hdfs://master:9000/data/taobao").map { f =>
    val p = f.split("\001")
    Record(p(0), p(1), p(2),
           p(3),p(4), p(5),
           p(6), p(7), p(8),
           p(9), p(10), p(11),
           p(12), p(13), p(14), p(15))
  }.coalesce(12).toDS
  data.createTempView("record")
//  spark.sql("select *,case when promotionPrice <> '' then regexp_replace(promotionPrice,'￥','') else 0.0 end as promotionPrice_1 from record ").drop("promotionPrice").saveToEs("record_1/good_1")

  val options = Map(
    "pushdown" -> "true",
    "es.nodes" -> "localhost", "es.port" -> "9200")
//  spark.esDF("record","q=",options).show()

}