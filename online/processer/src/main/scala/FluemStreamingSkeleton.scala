import org.apache.commons.cli.{CommandLine, Options}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils
import org.slf4j.LoggerFactory

/**
  * Created by lyen on 17-6-20.
  */
abstract class FlumeStreamingSkeleton extends SparkSkeleton {
  private val log = LoggerFactory.getLogger(classOf[FlumeStreamingSkeleton])

  def compute(dstream: DStream[String])

  override def addOptions(options: Options): Unit = {

    options.addOption("I", true, "Compute interval!")

  }

  override def parseOwnCommand(commandLine: CommandLine, customCF: CustomCF): Unit = {
    if (commandLine.hasOption("I")) customCF.set("interval", commandLine.getOptionValue("I"))
  }

  override def execute(args: Array[String]) = {

    val conf = super.parseCommand(args)
    val interval = Seconds(conf.getInt("interval", 10))
    print(interval)
    val ssc = new StreamingContext(conf, interval)
    val flumeStream = FlumeUtils. /*createStream*/ createPollingStream(ssc, conf.get("flume.hostname", "127.0.0.1"), conf.getInt("flume.port", 41412))

    val strStream = flumeStream.map(e => new String(e.event.getBody.array))
    log.info("Ready to compute ...")
    compute(strStream)
    ssc.start
    ssc.awaitTermination
  }

}

