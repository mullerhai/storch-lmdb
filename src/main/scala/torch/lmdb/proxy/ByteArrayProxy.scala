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

import java.lang.Math.min
import java.util.Objects.requireNonNull
import torch.lmdb.db.Library.RUNTIME

import java.util
import java.util.Comparator
import jnr.ffi.Pointer
import jnr.ffi.provider.MemoryManager
import torch.lmdb.proxy.BufferProxy

/**
 * Byte array proxy.
 *
 * <p>{@link Env# create ( org.torch.lmdb.BufferProxy )}.
 */
object ByteArrayProxy {
  /** The byte array proxy. Guaranteed to never be null. */
  val PROXY_BA = new ByteArrayProxy
  private val MEM_MGR = RUNTIME.getMemoryManager
  private val signedComparator : Comparator[Array[Byte]]  = ByteArrayProxy.compareArraysSigned
  private val unsignedComparator: Comparator[Array[Byte]]  = ByteArrayProxy.compareArrays

  /**
   * Lexicographically compare two byte arrays.
   *
   * @param o1 left operand (required)
   * @param o2 right operand (required)
   * @return as specified by {@link Comparable} interface
   */
  def compareArrays(o1: Array[Byte], o2: Array[Byte]): Int = {
    requireNonNull(o1)
    requireNonNull(o2)
    if (o1 eq o2) return 0
    val minLength = min(o1.length, o2.length)
    for (i <- 0 until minLength) {
      val lw = java.lang.Byte.toUnsignedInt(o1(i))
      val rw = java.lang.Byte.toUnsignedInt(o2(i))
      val result = Integer.compareUnsigned(lw, rw)
      if (result != 0) return result
    }
    o1.length - o2.length
  }

  /**
   * Compare two byte arrays.
   *
   * @param b1 left operand (required)
   * @param b2 right operand (required)
   * @return as specified by {@link Comparable} interface
   */
  def compareArraysSigned(b1: Array[Byte], b2: Array[Byte]): Int = {
    requireNonNull(b1)
    requireNonNull(b2)
    if (b1 eq b2) return 0
    for (i <- 0 until min(b1.length, b2.length)) {
      if (b1(i) != b2(i)) return b1(i) - b2(i)
    }
    b1.length - b2.length
  }
}

final class ByteArrayProxy private extends BufferProxy[Array[Byte]] {
  override  def allocate = new Array[Byte](0)

  override  def deallocate(buff: Array[Byte]): Unit = {

    // byte arrays cannot be allocated
  }

  override  def getBytes(buffer: Array[Byte]): Array[Byte] = util.Arrays.copyOf(buffer, buffer.length)

  override  def getSignedComparator: Comparator[Array[Byte]] = ByteArrayProxy.signedComparator

  override  def getUnsignedComparator: Comparator[Array[Byte]] = ByteArrayProxy.unsignedComparator

  override  def in(buffer: Array[Byte], ptr: Pointer, ptrAddr: Long): Unit = {
    val pointer = ByteArrayProxy.MEM_MGR.allocateDirect(buffer.length)
    pointer.put(0, buffer, 0, buffer.length)
    ptr.putLong(BufferProxy.STRUCT_FIELD_OFFSET_SIZE, buffer.length)
    ptr.putAddress(BufferProxy.STRUCT_FIELD_OFFSET_DATA, pointer.address)
  }

  override  def in(buffer: Array[Byte], size: Int, ptr: Pointer, ptrAddr: Long): Unit = {

    // cannot reserve for byte arrays
  }

  override  def out(buffer: Array[Byte], ptr: Pointer, ptrAddr: Long): Array[Byte] = {
    val addr = ptr.getAddress(BufferProxy.STRUCT_FIELD_OFFSET_DATA)
    val size = ptr.getLong(BufferProxy.STRUCT_FIELD_OFFSET_SIZE).toInt
    val pointer = ByteArrayProxy.MEM_MGR.newPointer(addr, size)
    val bytes = new Array[Byte](size)
    pointer.get(0, bytes, 0, size)
    bytes
  }
}