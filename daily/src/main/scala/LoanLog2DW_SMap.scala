import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

/**
  * Created by lyen on 17-7-19.
  */
object LoanLog2DW_SMap {
  val FRAUD_FOCUS_LIST = "欺诈关注清单"
  val FRAUD_APPLY_SCORE = "欺诈申请分"
  val FRAUD_APPLY_VERIFY = "欺诈申请验证"
  val ZM_CREDIT_SCORE = "芝麻信用分"
  val BUSINESS_FOCUS_LIST = "行业关注名单"

  val loanPath = "hdfs://master:9000/data/wolong/loan"
  val spark = SparkSession
    .builder()
    .appName("Loan")
    .master("spark://master:7077")
    .config("hive.metastore.warehouse.dir", "hdfs://master:9000/user/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val originalData = spark.sparkContext.textFile(loanPath)
    val records = originalData.map { record =>
      val fields = record.split("\t")
      val idCard = fields(0)
      val name = fields(1)
      val phone = fields(2)
      val reqTime = fields(4)
      val `type` = fields(5)
      val result = fields(3)
      val ob = JSON.parseObject(result).getJSONObject("data")
      var data: Any = null
      if (`type` == FRAUD_FOCUS_LIST) {
        if (ob.get("hit") == "no") {
          data = null
        } else {
          data = ob.getOrDefault("risk_code", null).asInstanceOf[JSONArray]
        }
      }
      else if (`type` == FRAUD_APPLY_SCORE) {
        data = ob.getOrDefault("score", null)
      }
      else if (`type` == FRAUD_APPLY_VERIFY) {
        data = ob.getOrDefault("verify_code", null).asInstanceOf[JSONArray]
      }
      else if (`type` == ZM_CREDIT_SCORE) {
        /**
          * 创建新的JsonObject(如果采取new JsonObject(Map.asInstanceOf[Map[String, Object]].asJava)
          * 可能会出现序列化问题,或者出现转换出错,如：struct<empty: boolean, traversableAgain: boolean>)
          * 尽量避免JsonObject与Map的嵌套
          * val state = ob.getOrDefault("state", null) match {
          * case null => null
          * case b => b.asInstanceOf[JSONObject]
          * }
          * val zm_score = ob.getOrDefault("zm_score", null)
          * val jb = new JSONObject()
          * jb.put("state",state)
          * jb.put("zm_score",zm_score)
          * data =jb
          */
        val state = ob.getOrDefault("state", null) match {
          case null => null
          case b => b.asInstanceOf[JSONObject]
        }
        val zm_score = ob.getOrDefault("zm_score", null)
        val jb = new JSONObject()
        jb.put("state", state)
        jb.put("zm_score", zm_score)
        data = jb
      }
      else if (`type` == BUSINESS_FOCUS_LIST) {
        if (ob.getOrDefault("is_matched", null) == false) {
          data = null
        } else {
          val details = ob.getJSONArray("details")
          for (i <- 0 until details.size()) {
            val detail = details.getJSONObject(i)
            detail.remove("level")
            detail.remove("settlement")
          }
          data = details
        }
      }
      /** 以下代码
        * spark-shell执行无误，
        * 但在spark-submit的时候会报 scala.MatchError: 芝麻信用分 (of class java.lang.String),有可能是中文的原因
        * 暂修改为if else语法
        *
        * val typeTransformed = `type` match {
        * case FRAUD_FOCUS_LIST => "fraud_focus_list"
        * case FRAUD_APPLY_SCORE => "fraud_apply_score"
        * case FRAUD_APPLY_VERIFY => "fraud_apply_verify"
        * case ZM_CREDIT_SCORE => "zm_credit_score"
        * case BUSINESS_FOCUS_LIST => "business_focus_list"
        * }
        */
      val typeTransformed =
      if (`type` == FRAUD_FOCUS_LIST) "fraud_focus_list"
      else if (`type` == FRAUD_APPLY_SCORE) "fraud_apply_score"
      else if (`type` == FRAUD_APPLY_VERIFY) "fraud_apply_verify"
      else if (`type` == ZM_CREDIT_SCORE) "zm_credit_score"
      else "business_focus_list"
      (Map("idCard" -> idCard, "name" -> name, "phone" -> phone, "reqTime" -> reqTime), Map(typeTransformed -> data))
    }.reduceByKey((a, b) => a ++ b)
    val recordsJsonRDD = records.map { f =>
      val tmp = f._1 ++ f._2
      new JSONObject(tmp.asInstanceOf[Map[String, Object]].asJava).toJSONString
    }
    val recordsDF = spark.read.json(recordsJsonRDD)
    recordsDF.coalesce(1).write.mode("overwrite").saveAsTable(args(0).toString) /*默认保存文件为nappy压缩的parquet文件,用Hive分析需注意复杂类型字段*/
  }


}
