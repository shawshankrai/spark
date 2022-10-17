package utils

object PathGenerators {

  def getPathResourcesMainFolderWithFile(fileName: String): String = {
    s"src/main/resources/data/$fileName"
  }

  def getPathResourcesMainFolder: String = {
    "src/main/resources/data/"
  }

}
