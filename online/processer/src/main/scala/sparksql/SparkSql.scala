package sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by lyen on 17-7-6.
  */
object SparkSql extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()


  import spark.implicits._
  import spark.sql
  sql("select fid from wc_text t lateral view explode(split(id,' ')) a as fid").createTempView("wc")

  sql("select fid,count(1) from wc group by fid").show()

}
