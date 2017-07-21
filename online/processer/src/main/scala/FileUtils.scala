import java.io.IOException
import java.net.URISyntaxException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{RDD, UnionRDD}

import scala.collection.mutable.MutableList

/**
  * Created by lyen on 17-6-20.
  */
class FileUtils {

}

object FileUtils {
  @throws[IOException]
  @throws[URISyntaxException]
  def readDirectoryRDD(dir: String, conf: SparkConf): MutableList[String] = {
    val fs = FileSystem.get(new Path(URIUtils.getRoot(dir)).toUri(), getDefaultConfig(conf))
    val paths = MutableList[String]()
    readDirectory(fs, new Path(dir), paths)
    paths
  }

  private def getDefaultConfig(conf: SparkConf): Configuration = {
    val fsConf: Configuration = new Configuration
    //    if (CustomCF.isLocal(conf)) fsConf.set("job.runlocal", "true")
    fsConf
  }

  @throws[IOException]
  private def readDirectory(fs: FileSystem, path: Path, paths: MutableList[String]): Unit = {
    val fsStatus = fs.listStatus(path)
    fsStatus.foreach { ob =>
      ob match {
        case ob if ob.isFile => paths += ob.getPath.toString
        case ob if ob.isDirectory => readDirectory(fs, ob.getPath, paths)
      }
    }
  }


  /**
    * 读取相同深度的文件，需要指定深度到最后叶子文件一级，如果文件中有文件夹和文件混合则会出问题
    * 速度较readDirectoryRDD更快
    *
    * @param dir
    * @param leafDepth 叶子目录相对于dir的深度
    * @return
    * @throws URISyntaxException
    * @throws IOException
    */
  @throws[URISyntaxException]
  @throws[IOException]
  def getDirecotryOfDepth(dir: String, leafDepth: Int, ctx: SparkConf): MutableList[String] = {
    val path: Path = new Path(dir)
    val fs: FileSystem = FileSystem.get(path.toUri, getDefaultConfig(ctx))
    val paths = MutableList[String]()
    getOfSubDirectoy(fs, path, 0, leafDepth, paths)
    assert(paths.size != 0)
    paths
  }

  @throws[IOException]
  private def getOfSubDirectoy(fs: FileSystem, path: Path, currentDepth: Int, leafDepth: Int, paths: MutableList[String]) {
    if (currentDepth == leafDepth) {
      val fsList = fs.listStatus(path)

      for (f <- fsList if f.isDirectory) {
        paths += f.getPath.toString
      }
    }
    else {
      val fsList: Array[FileStatus] = fs.listStatus(path)
      for (f <- fsList if f.isDirectory) {
        getOfSubDirectoy(fs, f.getPath, currentDepth + 1, leafDepth, paths)
      }
    }
  }


  def getRddOfMultiFile(paths: MutableList[String], sc: SparkContext): RDD[String] = {
    val rdd = new UnionRDD[String](sc, paths.map(f => sc.textFile(f)))
    rdd
  }

  @throws[IOException]
  def isExsit(ctx: SparkConf, dir: String): Boolean = {
    val path: Path = new Path(dir)
    val fs: FileSystem = FileSystem.get(path.toUri, getDefaultConfig(ctx))
    fs.exists(path)
  }

  def main(args: Array[String]): Unit = {
    val list = FileUtils.readDirectoryRDD("hdfs://master:9000/", new CustomCF())
    list.foreach(println)

  }


}
