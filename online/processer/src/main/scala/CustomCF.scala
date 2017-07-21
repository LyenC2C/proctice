import java.io.{IOException, Serializable}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

/**
  * Created by lyen on 17-6-19.
  */
class CustomCF extends SparkConf with Serializable {

  def loadProperties(file: String) = {
    try {
      val path = new Path(Statics.propPath)
      val hcf = new Configuration()
      val fs = FileSystem.get(path.toUri, hcf)
      val is = fs.open(new Path(file))
      try {
        val props = new Properties()
        props.load(is)
        val en = props.keys()
        while (en.hasMoreElements) {
          val name = en.nextElement().toString()
          this.set(name, props.getProperty(name))
        }
      }
      finally {
        try {
          is.close()
        }
        catch {
          case e: Exception =>
        }

      }
    }
    catch {
      case e: IOException => logInfo("加载配置文件出错", e)
        throw e
    }
  }


  override def get(key: String, defaultValue: String): String = {
    if (this.contains(key)) {
      super.get(key, defaultValue)
    }
    else {
      defaultValue
    }
  }

  override def get(key: String): String = {
    get(key, null)
  }

}

object CustomCF {
  def isLocal(conf: SparkConf): Boolean = conf.get("spark.master").toLowerCase.contains("local")
}
