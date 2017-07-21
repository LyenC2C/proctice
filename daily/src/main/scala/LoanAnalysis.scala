import com.alibaba.fastjson.{JSON, JSONObject, JSONArray}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._


import scala.collection.JavaConverters._


/**
  * Created by lyen on 17-7-17.
  */


object LoanAnalysis extends App {


  val loanPath = "hdfs://master:9000/data/wolong/loan"
  val mapDetailsPath = "hdfs://master:9000/data/wolong/fraud"
  val multiPlatformPath = "hdfs://master:9000/data/wolong/wolong_jiedai.json"


  val spark = SparkSession
    .builder()
    .appName("Loan_cacsi")
    .master("local[*]")
    .getOrCreate()

  val mapInfo = spark.sparkContext.textFile(mapDetailsPath).map { f =>
    val tmp = f.split("\t")
    (tmp(0), tmp(1))
  }.collectAsMap()
  val broadcast = spark.sparkContext.broadcast(mapInfo)
  val bv = broadcast.value

  spark.udf.register("mapFunction", (arr: Seq[String]) => arr match {
    case null => null
    case data => data.toArray.map(da => bv.get(da) match {
      case Some(v) => v
      case _ => None
    }).filter(f => f != None).mkString(":")
  })

  spark.sql("select phone,zm_credit_score,fraud_apply_score,mapFunction(fraud_apply_verify),mapFunction(fraud_focus_list),mapFunction(business_focus_list['code']) from loan").show


}
