package utils

object PathGenerators {

  def getPathResourcesMainFolder(fileName: String): String = {
    s"src/main/resources/data/$fileName"
  }

}
