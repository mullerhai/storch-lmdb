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
package torch.lmdb

import java.lang.ThreadLocal.withInitial
import java.nio.ByteBuffer.allocateDirect
import java.nio.ByteOrder.BIG_ENDIAN
import java.util.Objects.requireNonNull
import torch.lmdb.UnsafeAccess.UNSAFE

import java.nio.ByteBuffer
import java.util
import java.util.Comparator
import jnr.ffi.Pointer
import torch.lmdb.proxy.BufferProxy
import org.agrona.DirectBuffer
import org.agrona.MutableDirectBuffer
import org.agrona.concurrent.UnsafeBuffer
import torch.lmdb.proxy.BufferProxy.{STRUCT_FIELD_OFFSET_DATA, STRUCT_FIELD_OFFSET_SIZE}

/**
 * A buffer proxy backed by Agrona's {@link DirectBuffer}.
 *
 * <p>This class requires {@link UnsafeAccess} and Agrona must be in the classpath.
 */
object DirectBufferProxy {
  private val signedComparator: Comparator[DirectBuffer] = (o1: DirectBuffer, o2: DirectBuffer) => {
    requireNonNull(o1)
    requireNonNull(o2)
    o1.compareTo(o2)
  }
  private val unsignedComparator: Comparator[DirectBuffer] = DirectBufferProxy.compareBuff
  /**
   * The {@link MutableDirectBuffer} proxy. Guaranteed to never be null, although a class
   * initialization exception will occur if an attempt is made to access this field when unsafe or
   * Agrona is unavailable.
   */
  val PROXY_DB = new DirectBufferProxy
  /**
   * A thread-safe pool for a given length. If the buffer found is valid (ie not of a negative
   * length) then that buffer is used. If no valid buffer is found, a new buffer is created.
   */
  private val BUFFERS = withInitial(() => new util.ArrayDeque[DirectBuffer](16))

  /**
   * Lexicographically compare two buffers.
   *
   * @param o1 left operand (required)
   * @param o2 right operand (required)
   * @return as specified by {@link Comparable} interface
   */
  def compareBuff(o1: DirectBuffer, o2: DirectBuffer): Int = {
    requireNonNull(o1)
    requireNonNull(o2)
    if (o1 == o2) return 0
    val minLength = Math.min(o1.capacity, o2.capacity)
    val minWords = minLength / java.lang.Long.BYTES
    var i = 0
    while (i < minWords * java.lang.Long.BYTES) {
      val lw = o1.getLong(i, BIG_ENDIAN)
      val rw = o2.getLong(i, BIG_ENDIAN)
      val diff = java.lang.Long.compareUnsigned(lw, rw)
      if (diff != 0) return diff
      i += java.lang.Long.BYTES
    }
    for (i <- minWords * java.lang.Long.BYTES until minLength) {
      val lw = java.lang.Byte.toUnsignedInt(o1.getByte(i))
      val rw = java.lang.Byte.toUnsignedInt(o2.getByte(i))
      val result = Integer.compareUnsigned(lw, rw)
      if (result != 0) return result
    }
    o1.capacity - o2.capacity
  }
}

final class DirectBufferProxy private extends BufferProxy[DirectBuffer] {

  override def allocate: DirectBuffer = {
    val q = DirectBufferProxy.BUFFERS.get
    val buffer = q.poll
    if (buffer != null && buffer.capacity >= 0) buffer
    else {
      val bb = allocateDirect(0)
      new UnsafeBuffer(bb)
    }
  }

  override  def getSignedComparator: Comparator[DirectBuffer] = DirectBufferProxy.signedComparator

  override  def getUnsignedComparator: Comparator[DirectBuffer] = DirectBufferProxy.unsignedComparator

  override  def deallocate(buff: DirectBuffer): Unit = {
    val q = DirectBufferProxy.BUFFERS.get
    q.offer(buff)
  }

  override  def getBytes(buffer: DirectBuffer): Array[Byte] = {
    val dest = new Array[Byte](buffer.capacity)
    buffer.getBytes(0, dest, 0, buffer.capacity)
    dest
  }

  override  def in(buffer: DirectBuffer, ptr: Pointer, ptrAddr: Long): Unit = {
    val addr = buffer.addressOffset
    val size = buffer.capacity
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr)
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size)
  }

  override  def in(buffer: DirectBuffer, size: Int, ptr: Pointer, ptrAddr: Long): Unit = {
    val addr = buffer.addressOffset
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, addr)
    UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size)
  }

  override  def out(buffer: DirectBuffer, ptr: Pointer, ptrAddr: Long): DirectBuffer = {
    val addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA)
    val size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE)
    buffer.wrap(addr, size.toInt)
    buffer
  }



}