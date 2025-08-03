/*
 * Copyright © 2016-2025 The torch.lmdb Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package torch
package lmdb.proxy

import io.netty.buffer.PooledByteBufAllocator.DEFAULT

import java.lang.Class.forName
import java.util.Objects.requireNonNull
import torch.lmdb.UnsafeAccess.UNSAFE
import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator

import java.lang.reflect.Field
import java.util.Comparator
import jnr.ffi.Pointer
import torch.lmdb.exceptions.LmdbException
import torch.lmdb.proxy.BufferProxy
import torch.lmdb.proxy.BufferProxy.{STRUCT_FIELD_OFFSET_DATA, STRUCT_FIELD_OFFSET_SIZE}

/**
 * A buffer proxy backed by Netty's {@link ByteBuf}.
 *
 * <p>This class requires {@link UnsafeAccess} and netty-buffer must be in the classpath.
 */
object ByteBufProxy {
  /**
   * A proxy for using Netty {@link ByteBuf}. Guaranteed to never be null, although a class
   * initialization exception will occur if an attempt is made to access this field when Netty is
   * unavailable.
   */
  val PROXY_NETTY = new ByteBufProxy
  private val BUFFER_RETRIES = 10
  private val FIELD_NAME_ADDRESS = "memoryAddress"
  private val FIELD_NAME_LENGTH = "length"
  private val NAME = "io.netty.buffer.PooledUnsafeDirectByteBuf"
  val comparator: Comparator[ByteBuf] = (o1: ByteBuf, o2: ByteBuf) => {
    requireNonNull(o1)
    requireNonNull(o2)
    o1.compareTo(o2)
  }
//  private val comparator = (o1: ByteBuf, o2: ByteBuf) => {
//    requireNonNull(o1)
//    requireNonNull(o2)
//    o1.compareTo(o2)
//  }

  private[torch] def findField(c: String, name: String): Field = {
    var clazz: Class[?] = null
    try {
      clazz = Class.forName(c)
    } catch {
      case e: ClassNotFoundException =>
        throw new LmdbException(c + " class unavailable", e)
    }
    while (clazz != null) {
      try {
        val field = clazz.getDeclaredField(name)
        field.setAccessible(true)
        return field
      } catch {
        case e: NoSuchFieldException =>
          clazz = clazz.getSuperclass
      }
    }
    throw new LmdbException(name + " not found")
  }
}

final class ByteBufProxy(private val nettyAllocator: PooledByteBufAllocator) extends BufferProxy[ByteBuf] {
  try {
    val initBuf = this.allocate
    initBuf.release
    val address = ByteBufProxy.findField(ByteBufProxy.NAME, ByteBufProxy.FIELD_NAME_ADDRESS)
    val length = ByteBufProxy.findField(ByteBufProxy.NAME, ByteBufProxy.FIELD_NAME_LENGTH)
    addressOffset = UNSAFE.objectFieldOffset(address)
    lengthOffset = UNSAFE.objectFieldOffset(length)
  } catch {
    case e: SecurityException =>
      throw new LmdbException("Field access error", e)
  }
  final private var lengthOffset = 0L
  final private var addressOffset = 0L

  def this() = {
    this(DEFAULT)
  }

  override  def allocate: ByteBuf = {
    for (i <- 0 until ByteBufProxy.BUFFER_RETRIES) {
      val bb = nettyAllocator.directBuffer
      if (ByteBufProxy.NAME == bb.getClass.getName) return bb
      else bb.release
    }
    throw new IllegalStateException("Netty buffer must be " + ByteBufProxy.NAME)
  }

  override  def getSignedComparator: Comparator[ByteBuf] = ByteBufProxy.comparator

  override  def getUnsignedComparator: Comparator[ByteBuf] = ByteBufProxy.comparator

  override  def deallocate(buff: ByteBuf): Unit = {
    buff.release
  }

  override  def getBytes(buffer: ByteBuf): Array[Byte] = {
    val dest = new Array[Byte](buffer.capacity)
    buffer.getBytes(0, dest)
    dest
  }

  override  def in(buffer: ByteBuf, ptr: Pointer, ptrAddr: Long): Unit = {
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, buffer.writerIndex - buffer.readerIndex)
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, buffer.memoryAddress + buffer.readerIndex)
  }

  override  def in(buffer: ByteBuf, size: Int, ptr: Pointer, ptrAddr: Long): Unit = {
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size)
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, buffer.memoryAddress + buffer.readerIndex)
  }

  override  def out(buffer: ByteBuf, ptr: Pointer, ptrAddr: Long): ByteBuf = {
    val addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA)
    val size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE)
    UNSAFE.putLong(buffer, addressOffset, addr)
    UNSAFE.putInt(buffer, lengthOffset, size.toInt)
    buffer.clear.writerIndex(size.toInt)
    buffer
  }
}