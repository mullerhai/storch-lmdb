package torch.lmdb.db

import java.util

object DataStructure {
  private var instance: DataStructure = null

  def getInstance: DataStructure = {
    if (instance == null) instance = new DataStructure
    instance
  }
}

class DataStructure {
  private val list: List[String] = List.empty
}