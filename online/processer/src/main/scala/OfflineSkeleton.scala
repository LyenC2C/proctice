import java.io.IOException

import org.apache.commons.cli.{CommandLine, Options}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by lyen on 17-6-21.
  */
abstract class OfflineSkeleton extends SparkSkeleton {

  private val log = LoggerFactory.getLogger(classOf[OfflineSkeleton])

  def compute(rdd: RDD[String])

  override def addOptions(options: Options): Unit = {
    options.addOption("dir", true, "input data dir!") //读取数据目录
    options.addOption("S", true, "the date begin,format:yyyyMMddHH") //开始时间
    options.addOption("E", true, "the date end,format:yyyyMMddHH") //结束时间
    options.addOption("U", true, "compute unit：hour,day,week,month") //计算单位，按天统计，按照周统计
  }

  override def parseOwnCommand(commandLine: CommandLine, customCF: CustomCF): Unit = {
    if (commandLine.hasOption("dir")) customCF.set("runtime.dir", commandLine.getOptionValue("dir"))
    if (commandLine.hasOption("S")) customCF.set("runtime.start", commandLine.getOptionValue("S")) //默认是昨天的数据
    if (commandLine.hasOption("E")) customCF.set("runtime.end", commandLine.getOptionValue("E"))
    if (commandLine.hasOption("U")) customCF.set("runtime.unit", commandLine.getOptionValue("U"))
  }

  override def execute(args: Array[String]): Unit = {
    val customCF = super.parseCommand(args)
    val sc = new SparkContext(customCF)
    if (customCF.get("runtime.unit") == "hour") {
      if (StringUtils.isBlank(customCF.get("runtime.start")) && StringUtils.isBlank(customCF.get("runtime.end"))) {
        val current: DateTime = new DateTime().plusHours(-1)
        val paths = mutable.MutableList[String](URIUtils.urlJoin(customCF.get("runtime.dir"), current.toString("yyyy/MM/dd/HH")))
        runJob(paths, sc, current)
      }
      else if (StringUtils.isNoneBlank(customCF.get("runtime.start"))) {
        val start: DateTime = getTime(customCF.get("runtime.start"))
        var end: DateTime = new DateTime().plusHours(-1)
        if (customCF.contains("runtime.end")) end = getTime(customCF.get("runtime.end"))
        var current: DateTime = new DateTime(start)
        while (current.isBefore(end)) {
          val paths = mutable.MutableList[String](URIUtils.urlJoin(customCF.get("runtime.dir"), current.toString("yyyy/MM/dd/HH")))
          runJob(paths, sc, current)
          current = current.plusHours(1)
        }
      }
    }
    else if (customCF.get("runtime.unit") == "day") {
      if (StringUtils.isBlank(customCF.get("runtime.start")) && StringUtils.isBlank(customCF.get("runtime.end"))) {
        val current: DateTime = new DateTime().plusDays(-1)
        val dayPath: String = URIUtils.urlJoin(customCF.get("runtime.dir"), current.toString("yyyy/MM/dd"))
        //        val paths = FileUtils.getDirecotryOfDepth(dayPath, 0, customCF)
        val paths = FileUtils.readDirectoryRDD(dayPath, customCF)
        runJob(paths, sc, current)
      }
      else if (StringUtils.isNoneBlank(customCF.get("runtime.start"))) {
        var start: DateTime = getTime(customCF.get("runtime.start"))
        var end: DateTime = new DateTime().plusDays(1)
        if (customCF.contains("runtime.end")) end = getTime(customCF.get("runtime.end"))
        start = start.withTime(0, 0, 0, 0)
        end = end.withTime(23, 59, 59, 999)

        var current: DateTime = new DateTime(start)
        while (current.isBefore(end)) {
          val dayPath: String = URIUtils.urlJoin(customCF.get("runtime.dir"), current.toString("yyyy/MM/dd"))
          //val paths = FileUtils.getDirecotryOfDepth(dayPath, 0, customCF)
          val paths = FileUtils.readDirectoryRDD(dayPath, customCF)
          runJob(paths, sc, current)
          current = current.plusDays(1)
        }
      }
    }
  }


  private def runJob(path: mutable.MutableList[String], sc: SparkContext, dt: DateTime): Unit = {
    log.info("executing task:" + dt.toString())
    val existPaths = mutable.MutableList[String]()
    path.foreach { f =>
      try {
        FileUtils.isExsit(sc.getConf, f)
        existPaths += f
      }
      catch {
        case e: IOException =>
          log.info("add path wrongly", e)
      }
    }
    if (existPaths.size == 0) log.info("no data in this period:" + dt.toString())
    val rdd = FileUtils.getRddOfMultiFile(existPaths, sc)
    compute(rdd)
  }

  private def getTime(time: String): DateTime = {
    var time_ = time
    if (time_ == null) return null
    if (time_.length < 10) {
      //补齐8位
      var i: Int = time_.length
      while (i < 10) {
        time_ += "0"
        i += 1;
      }
    }
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHH")
    DateTime.parse(time_, formatter)
  }
}
