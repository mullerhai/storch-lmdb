package torch.lmdb.db

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class KeyValue(var keyName: ByteBuffer, var valueName: ByteBuffer) {
  setKeyName(keyName)
  setValueName(valueName)
//  private var keyName: String = null
//  private var valueName: String = null

  def getKeyName: String = StandardCharsets.UTF_8.decode(this.keyName).toString() 

  def setKeyName(keyName: ByteBuffer): Unit = {
    val keyByte = new Array[Byte](keyName.remaining)
    keyName.get(keyByte)
    this.keyName = ByteBuffer.wrap(keyByte)
  }

  def getValueName: String = StandardCharsets.UTF_8.decode(this.valueName).toString() 

  def setValueName(valueName: ByteBuffer): Unit = {
    val valueByte = new Array[Byte](valueName.remaining)
    valueName.get(valueByte)
    this.valueName = ByteBuffer.wrap(valueByte)
  }
}