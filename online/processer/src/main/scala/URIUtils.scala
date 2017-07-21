import java.net.{URI, URISyntaxException}

/**
  * Created by lyen on 17-6-20.
  */
class URIUtils {

}

object URIUtils {
  @throws[URISyntaxException]
  def getRoot(path: String): String = {
    val uri: URI = new URI(path)
    uri.getScheme + "://" + uri.getAuthority
  }

  def urlJoin(first: String, parameters: String*): String = {
    var first_ = first
    if (parameters == null || parameters.length == 0) return first
    for (parameter <- parameters) {
      var parameter_ = parameter.replaceAll("[\\\\]", "/")
      if (!first_.endsWith("/") && !parameter_.startsWith("/")) first_ = first_ + "/" + parameter_
      else if (first_.endsWith("/") && parameter_.startsWith("/")) first_ = first_ + parameter_.substring(1)
      else first_ = first_ + parameter_
    }
    first_
  }
}
