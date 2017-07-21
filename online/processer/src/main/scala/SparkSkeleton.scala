import org.apache.commons.cli._
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory


/**
  * Created by lyen on 17-6-19.
  */


abstract class SparkSkeleton extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[SparkSkeleton])
  val OPTION_HELP = "help"
  val OPTION_APPNAME = "name"
  val OPTION_MASTER = "master"
  val OPTION_CONFIG_FILE = "config"

  def execute(args: Array[String])

  def addOptions(options: Options)

  def parseOwnCommand(commandLine: CommandLine, customCF: CustomCF)

  def parseCommand(cmds: Array[String]): CustomCF = {
    val parser: CommandLineParser = new BasicParser
    val customCF: CustomCF = new CustomCF()
    val options: Options = new Options
    options.addOption(OPTION_HELP, false, "spark application help information!")
    options.addOption(OPTION_APPNAME, true, "spark application name!")
    options.addOption(OPTION_MASTER, true, "spark master url!")
    options.addOption(OPTION_CONFIG_FILE, true, "additional spark application properties to set!")
    addOptions(options)
    val cmd = parser.parse(options, cmds)

    if (cmd.hasOption("help")) printHelp(options)

    if (cmd.hasOption(OPTION_APPNAME)) {
      val appName: String = cmd.getOptionValue(OPTION_APPNAME)
      if (!StringUtils.isBlank(appName)) customCF.setAppName(appName)
      if (cmd.hasOption(OPTION_MASTER)) customCF.setMaster(cmd.getOptionValue(OPTION_MASTER))
      if (cmd.hasOption(OPTION_CONFIG_FILE)) {
        val filePath: String = cmd.getOptionValue(OPTION_CONFIG_FILE)
        try
          customCF.loadProperties(filePath)
        catch {
          case e: Exception => {
            logger.error("can not load configure file:" + filePath)
          }
        }
      }
      parseOwnCommand(cmd, customCF)
    }
    customCF
  }

  private def printHelp(options: Options) {
    val formatter: HelpFormatter = new HelpFormatter
    formatter.printHelp("[]", options)
  }

}

