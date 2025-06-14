/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
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

import java.util.Objects.requireNonNull
import torch.lmdb.BufferProxy.MDB_VAL_STRUCT_SIZE
import torch.lmdb.BufferProxy.STRUCT_FIELD_OFFSET_SIZE
import torch.lmdb.Library.RUNTIME
import jnr.ffi.Pointer
import jnr.ffi.provider.MemoryManager
import torch.lmdb.BufferProxy

/**
 * Represents off-heap memory holding a key and value pair.
 *
 * @param < T> buffer type
 */
object KeyVal {
  private val MEM_MGR = RUNTIME.getMemoryManager
}

final class KeyVal[T] private[torch](private val proxy: BufferProxy[T]) extends AutoCloseable {
  requireNonNull(proxy)
  this.k = proxy.allocate
  this.v = proxy.allocate
  ptrKey = KeyVal.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false)
  ptrKeyAddr = ptrKey.address
  ptrArray = KeyVal.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE * 2, false)
  ptrVal = ptrArray.slice(0, MDB_VAL_STRUCT_SIZE)
  ptrValAddr = ptrVal.address
  private var closed = false
  private var k: T = null
  final private var ptrArray: Pointer = null
  final private var ptrKey: Pointer = null
  final private var ptrKeyAddr = 0L
  final private var ptrVal: Pointer = null
  final private var ptrValAddr = 0L
  private var v: T = null

  override def close(): Unit = {
    if (closed) return
    closed = true
    proxy.deallocate(k)
    proxy.deallocate(v)
  }

  private[torch] def key = k

  private[torch] def keyIn(key: T): Unit = {
    proxy.in(key, ptrKey, ptrKeyAddr)
  }

  private[torch] def keyOut = {
    k = proxy.out(k, ptrKey, ptrKeyAddr)
    k
  }

  private[torch] def pointerKey = ptrKey

  private[torch] def pointerVal = ptrVal

  private[torch] def vals = v

  private[torch] def valIn(vals: T): Unit = {
    proxy.in(vals, ptrVal, ptrValAddr)
  }

  private[torch] def valIn(size: Int): Unit = {
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
  private[torch] def valInMulti(vals: T, elements: Int) = {
    val ptrVal2SizeOff = MDB_VAL_STRUCT_SIZE + STRUCT_FIELD_OFFSET_SIZE
    ptrArray.putLong(ptrVal2SizeOff, elements) // ptrVal2.size
    proxy.in(vals, ptrVal, ptrValAddr) // ptrVal1.data
    val totalBufferSize = ptrVal.getLong(STRUCT_FIELD_OFFSET_SIZE)
    val elemSize = totalBufferSize / elements
    ptrVal.putLong(STRUCT_FIELD_OFFSET_SIZE, elemSize) // ptrVal1.size
    ptrArray
  }

  private[torch] def valOut = {
    v = proxy.out(v, ptrVal, ptrValAddr)
    v
  }
}