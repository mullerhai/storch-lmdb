package torch.lmdb.db

import torch.lmdb.db.{CursorIterable, Dbi, Env, Txn}

import java.io.File
import java.nio.ByteBuffer
import java.util
import scala.collection.mutable.ListBuffer

class DataBase(private var dbDirectory: File) {

  private val pathLmdb: String = null
  private var db: Dbi[ByteBuffer] = null
  private var env: Env[ByteBuffer] = null
  this.env = Env.create.setMaxDbs(0).open(this.dbDirectory)

  for (obj <- this.env.getDbiNames) {
    System.out.println(new String(obj))
  }
  private def FilterDbPath(fullPath: String) = new File(fullPath)

  def GetDbNames: String = {
    if (db == null) return ""
    "String.valueOf(.getName())"
  }

  def GetData: List[KeyValue] = {
    val rtx = env.txnRead
    val results = new ListBuffer[KeyValue]
    db = env.openDbi(null.asInstanceOf[Array[Byte]])
    val cursor = db.iterate(rtx)
//    import scala.collection.JavaConversions.*
    for (kv <- cursor) {
      // Storing in the data class
      results.append(new KeyValue(kv.key, kv.vals))
    }
    results.toList
  }

  def searchData(keyValue: String, valueSearch: Boolean): List[KeyValue] = {
    val rtx = env.txnRead
    // To byte
    val results = new ListBuffer[KeyValue]
    db = env.openDbi(null.asInstanceOf[Array[Byte]])
    val cursor = db.iterate(rtx)
//    import scala.collection.JavaConversions.*
    for (kv <- cursor) {
      // copy of key
      val size = kv.key.remaining
      val keyByte = new Array[Byte](size)
      kv.key.get(keyByte)
      val dbString = new String(keyByte)
      // copy of value
      val sizeValue = kv.vals.remaining
      val valueByte = new Array[Byte](sizeValue)
      kv.vals.get(valueByte)
      val dbStringValue = new String(valueByte)
      if (valueSearch && dbStringValue.contains(keyValue)) results.append(new KeyValue(ByteBuffer.wrap(dbString.getBytes), ByteBuffer.wrap(dbStringValue.getBytes)))
      else if (!valueSearch && dbString.contains(keyValue)) results.append(new KeyValue(ByteBuffer.wrap(dbString.getBytes), ByteBuffer.wrap(dbStringValue.getBytes)))
    }
    results.toList
  }
}