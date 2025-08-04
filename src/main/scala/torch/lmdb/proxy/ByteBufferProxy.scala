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

import java.lang.Long.reverseBytes
import java.lang.ThreadLocal.withInitial
import java.nio.ByteBuffer.allocateDirect
import java.nio.ByteOrder.BIG_ENDIAN
import java.nio.ByteOrder.LITTLE_ENDIAN
import java.util.Objects.requireNonNull
import torch.lmdb.db.Env.SHOULD_CHECK
import torch.lmdb.UnsafeAccess.UNSAFE
import java.lang.reflect.Field
import java.nio.Buffer
import java.nio.ByteBuffer
import java.util
import java.util.Comparator
import jnr.ffi.Pointer
//import org.agrona.DirectBuffer
import torch.lmdb.exceptions.LmdbException
import torch.lmdb.proxy.BufferProxy.{STRUCT_FIELD_OFFSET_DATA, STRUCT_FIELD_OFFSET_SIZE}
import torch.lmdb.proxy.ByteBufferProxy.AbstractByteBufferProxy.{FIELD_NAME_ADDRESS, FIELD_NAME_CAPACITY}
import torch.lmdb.proxy.BufferProxy
import torch.lmdb.proxy.ByteBufferProxy.AbstractByteBufferProxy.findField

/**
 * {@link ByteBuffer}-based proxy.
 *
 * <p>There are two concrete {@link ByteBuffer} proxy implementations available:
 *
 * <ul>
 * <li>A "fast" implementation: {@link UnsafeProxy}
 * <li>A "safe" implementation: {@link ReflectiveProxy}
 * </ul>
 *
 * <p>Users nominate which implementation they prefer by referencing the {@link # PROXY_OPTIMAL} or
 * {@link # PROXY_SAFE} field when invoking {@link Env# create ( org.torch.lmdb.BufferProxy )}.
 */
object ByteBufferProxy {
  /**
   * The fastest {@link ByteBuffer} proxy that is available on this platform. This will always be
   * the same instance as {@link # PROXY_SAFE} if the {@link UnsafeAccess# DISABLE_UNSAFE_PROP} has
   * been set to <code>true</code> and/or {@link UnsafeAccess} is unavailable. Guaranteed to never
   * be null.
   */
  var PROXY_OPTIMAL: BufferProxy[ByteBuffer] = null
  /** The safe, reflective {@link ByteBuffer} proxy for this system. Guaranteed to never be null. */
  var PROXY_SAFE: BufferProxy[ByteBuffer] = null

  private def getProxyOptimal = try new ByteBufferProxy.UnsafeProxy
  catch {
    case e: RuntimeException =>
      PROXY_SAFE
  }

  /** The buffer must be a direct buffer (not heap allocated). */
  @SerialVersionUID(1L)
  final class BufferMustBeDirectException

  /** Creates a new instance. */
    extends LmdbException("The buffer must be a direct buffer (not heap allocated") {
  }

  /**
   * Provides {@link ByteBuffer} pooling and address resolution for concrete {@link BufferProxy}
   * implementations.
   */
  object AbstractByteBufferProxy {
    val FIELD_NAME_ADDRESS = "address"
    val FIELD_NAME_CAPACITY = "capacity"
    private val signedComparator: Comparator[ByteBuffer] = (o1: ByteBuffer, o2: ByteBuffer) => {
      requireNonNull(o1)
      requireNonNull(o2)
      o1.compareTo(o2)
    }
    private val unsignedComparator: Comparator[ByteBuffer] = AbstractByteBufferProxy.compareBuff
    /**
     * A thread-safe pool for a given length. If the buffer found is valid (ie not of a negative
     * length) then that buffer is used. If no valid buffer is found, a new buffer is created.
     */
    private val BUFFERS = withInitial(() => new util.ArrayDeque[ByteBuffer](16))

    /**
     * Lexicographically compare two buffers.
     *
     * @param o1 left operand (required)
     * @param o2 right operand (required)
     * @return as specified by {@link Comparable} interface
     */
    def compareBuff(o1: ByteBuffer, o2: ByteBuffer): Int = {
      requireNonNull(o1)
      requireNonNull(o2)
      if (o1 == o2) return 0
      val minLength = Math.min(o1.limit, o2.limit)
      val minWords = minLength / java.lang.Long.BYTES
      val reverse1 = o1.order eq LITTLE_ENDIAN
      val reverse2 = o2.order eq LITTLE_ENDIAN
      var i = 0
      while (i < minWords * java.lang.Long.BYTES) {
        val lw = if (reverse1) reverseBytes(o1.getLong(i))
        else o1.getLong(i)
        val rw = if (reverse2) reverseBytes(o2.getLong(i))
        else o2.getLong(i)
        val diff = java.lang.Long.compareUnsigned(lw, rw)
        if (diff != 0) return diff
        i += java.lang.Long.BYTES
      }
      for (i <- minWords * java.lang.Long.BYTES until minLength) {
        val lw = java.lang.Byte.toUnsignedInt(o1.get(i))
        val rw = java.lang.Byte.toUnsignedInt(o2.get(i))
        val result = Integer.compareUnsigned(lw, rw)
        if (result != 0) return result
      }
      o1.remaining - o2.remaining
    }

    def findField(c: Class[?], name: String): Field = {
      var clazz = c
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

//    def findField(c: Class[?], name: String): Field = {
//      var clazz = c
//      do try {
//        val field = clazz.getDeclaredField(name)
//        field.setAccessible(true)
//        return field
//      } catch {
//        case e: NoSuchFieldException =>
//          clazz = clazz.getSuperclass
//      } while (clazz != null)
//      throw new LmdbException(name + " not found")
//    }
  }

  abstract class AbstractByteBufferProxy extends BufferProxy[ByteBuffer] {
    final protected def address(buffer: ByteBuffer): Long = {
        if (SHOULD_CHECK && !buffer.isDirect) throw new ByteBufferProxy.BufferMustBeDirectException
        // The original code used `sun.nio.ch.DirectBuffer` which is not available. 
        // Please provide the appropriate replacement logic for getting the address.
        ???
      }

    override final  def allocate: ByteBuffer = {
      val queue = AbstractByteBufferProxy.BUFFERS.get
      val buffer = queue.poll
      if (buffer != null && buffer.capacity >= 0) buffer
      else allocateDirect(0)
    }

    override  def getSignedComparator: Comparator[ByteBuffer] = AbstractByteBufferProxy.signedComparator

    override  def getUnsignedComparator: Comparator[ByteBuffer] = AbstractByteBufferProxy.unsignedComparator

    override final  def deallocate(buff: ByteBuffer): Unit = {
      buff.order(BIG_ENDIAN)
      val queue = AbstractByteBufferProxy.BUFFERS.get
      queue.offer(buff)
    }

    override  def getBytes(buffer: ByteBuffer): Array[Byte] = {
      val dest = new Array[Byte](buffer.limit)
      buffer.get(dest, 0, buffer.limit)
      dest
    }
  }

  /**
   * A proxy that uses Java reflection to modify byte buffer fields, and official JNR-FFF methods to
   * manipulate native pointers.
   */
  private object ReflectiveProxy {
    private var ADDRESS_FIELD: Field = null
    private var CAPACITY_FIELD: Field = null
    try ADDRESS_FIELD = findField(classOf[Buffer], FIELD_NAME_ADDRESS)
    CAPACITY_FIELD = findField(classOf[Buffer], FIELD_NAME_CAPACITY)
  }

  final private class ReflectiveProxy extends ByteBufferProxy.AbstractByteBufferProxy {
    override  def in(buffer: ByteBuffer, ptr: Pointer, ptrAddr: Long): Unit = {
      ptr.putAddress(STRUCT_FIELD_OFFSET_DATA, address(buffer))
      ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, buffer.remaining)
    }

    override  def in(buffer: ByteBuffer, size: Int, ptr: Pointer, ptrAddr: Long): Unit = {
      ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, size)
      ptr.putAddress(STRUCT_FIELD_OFFSET_DATA, address(buffer))
    }

    override  def out(buffer: ByteBuffer, ptr: Pointer, ptrAddr: Long): ByteBuffer = {
      val addr = ptr.getAddress(STRUCT_FIELD_OFFSET_DATA)
      val size = ptr.getLong(STRUCT_FIELD_OFFSET_SIZE)
      try {
        ReflectiveProxy.ADDRESS_FIELD.set(buffer, addr)
        ReflectiveProxy.CAPACITY_FIELD.set(buffer, size.toInt)
      } catch {
        case e@(_: IllegalArgumentException | _: IllegalAccessException) =>
          throw new LmdbException("Cannot modify buffer", e)
      }
      buffer.clear
      buffer
    }
  }

  /**
   * A proxy that uses Java's "unsafe" class to directly manipulate byte buffer fields and JNR-FFF
   * allocated memory pointers.
   */
  private object UnsafeProxy {
    private var ADDRESS_OFFSET = 0L
    private var CAPACITY_OFFSET = 0L
    try try {
      val address = findField(classOf[Buffer], FIELD_NAME_ADDRESS)
      val capacity = findField(classOf[Buffer], FIELD_NAME_CAPACITY)
      ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address)
      CAPACITY_OFFSET = UNSAFE.objectFieldOffset(capacity)
    } catch {
      case e: SecurityException =>
        throw new LmdbException("Field access error", e)
    }
  }

  final private class UnsafeProxy extends ByteBufferProxy.AbstractByteBufferProxy {
    override  def in(buffer: ByteBuffer, ptr: Pointer, ptrAddr: Long): Unit = {
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, buffer.remaining)
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, address(buffer))
    }

    override  def in(buffer: ByteBuffer, size: Int, ptr: Pointer, ptrAddr: Long): Unit = {
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size)
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, address(buffer))
    }

    override  def out(buffer: ByteBuffer, ptr: Pointer, ptrAddr: Long): ByteBuffer = {
      val addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA)
      val size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE)
      UNSAFE.putLong(buffer, UnsafeProxy.ADDRESS_OFFSET, addr)
      UNSAFE.putInt(buffer, UnsafeProxy.CAPACITY_OFFSET, size.toInt)
      buffer.clear
      buffer
    }
  }

  try PROXY_SAFE = new ByteBufferProxy.ReflectiveProxy
  PROXY_OPTIMAL = getProxyOptimal
}

final class ByteBufferProxy private {
}