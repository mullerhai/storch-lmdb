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
package torch.lmdb.db

import jnr.ffi.Pointer
import jnr.ffi.provider.MemoryManager
import torch.lmdb.db.KeyVal
import torch.lmdb.db.Library.RUNTIME
import torch.lmdb.proxy.BufferProxy
import torch.lmdb.proxy.BufferProxy.{MDB_VAL_STRUCT_SIZE, STRUCT_FIELD_OFFSET_SIZE}

import java.util.Objects.requireNonNull

/**
 * Represents off-heap memory holding a key and value pair.
 *
 * @param < T> buffer type
 */
object KeyVal {
  private val MEM_MGR = RUNTIME.getMemoryManager
}

final class KeyVal[T] (private var proxy: BufferProxy[T]) extends AutoCloseable {
  requireNonNull(proxy)
  private var closed = false
  private var k: T = proxy.allocate
  final private var ptrArray: Pointer = KeyVal.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE * 2, false)
  final private var ptrKey: Pointer = KeyVal.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false)
  final private var ptrKeyAddr = ptrKey.address
  final private var ptrVal: Pointer = ptrArray.slice(0, MDB_VAL_STRUCT_SIZE)
  final private var ptrValAddr = ptrVal.address
  private var v: T = proxy.allocate
//  this.k = proxy.allocate
//  this.v = proxy.allocate
//  ptrKey = KeyVal.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false)
//  ptrKeyAddr = ptrKey.address
//  ptrArray = KeyVal.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE * 2, false)
//  ptrVal = ptrArray.slice(0, MDB_VAL_STRUCT_SIZE)
//  ptrValAddr = ptrVal.address


  override def close(): Unit = {
    if (closed) return
    closed = true
    proxy.deallocate(k)
    proxy.deallocate(v)
  }

  def key = k

  def keyIn(key: T): Unit = {
    proxy.in(key, ptrKey, ptrKeyAddr)
  }

  def keyOut = {
    k = proxy.out(k, ptrKey, ptrKeyAddr)
    k
  }

  def pointerKey = ptrKey

  def pointerVal = ptrVal

  def vals = v

  def valIn(vals: T): Unit = {
    proxy.in(vals, ptrVal, ptrValAddr)
  }

  def valIn(size: Int): Unit = {
    proxy.in(v, size, ptrVal, ptrValAddr)
  }

  /**
   * Prepares an array suitable for presentation as the data argument to a <code>MDB_MULTIPLE</code>
   * put.
   *
   * <p>The returned array is equivalent of two <code>MDB_val</code>s as follows:
   *
   * <ul>
   * <li>ptrVal1.data = pointer to the data address of passed buffer
   * <li>ptrVal1.size = size of each individual data element
   * <li>ptrVal2.data = unused
   * <li>ptrVal2.size = number of data elements (as passed to this method)
   * </ul>
   *
   * @param val      a user-provided buffer with data elements (required)
   * @param elements number of data elements the user has provided
   * @return a properly-prepared pointer to an array for the operation
   */
  def valInMulti(vals: T, elements: Int) = {
    val ptrVal2SizeOff = MDB_VAL_STRUCT_SIZE + STRUCT_FIELD_OFFSET_SIZE
    ptrArray.putLong(ptrVal2SizeOff, elements) // ptrVal2.size
    proxy.in(vals, ptrVal, ptrValAddr) // ptrVal1.data
    val totalBufferSize = ptrVal.getLong(STRUCT_FIELD_OFFSET_SIZE)
    val elemSize = totalBufferSize / elements
    ptrVal.putLong(STRUCT_FIELD_OFFSET_SIZE, elemSize) // ptrVal1.size
    ptrArray
  }

  def valOut = {
    v = proxy.out(v, ptrVal, ptrValAddr)
    v
  }
}