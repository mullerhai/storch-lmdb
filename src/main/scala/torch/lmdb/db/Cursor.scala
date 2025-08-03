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

import java.util.Objects.requireNonNull
import torch.lmdb.db.Dbi.KeyExistsException.MDB_KEYEXIST
import torch.lmdb.db.Dbi.KeyNotFoundException.MDB_NOTFOUND
import torch.lmdb.db.Env.SHOULD_CHECK
import torch.lmdb.db.Library.LIB
import torch.lmdb.flags.MaskedFlag.isSet
import torch.lmdb.flags.MaskedFlag.mask
import torch.lmdb.flags.PutFlags.MDB_MULTIPLE
import torch.lmdb.flags.PutFlags.MDB_NODUPDATA
import torch.lmdb.flags.PutFlags.MDB_NOOVERWRITE
import torch.lmdb.flags.PutFlags.MDB_RESERVE
import torch.lmdb.ResultCodeMapper.checkRc
import torch.lmdb.enums.SeekOp.{MDB_FIRST, MDB_LAST, MDB_NEXT, MDB_PREV}
import jnr.ffi.Pointer
import jnr.ffi.byref.NativeLongByReference
import torch.lmdb.enums.GetOp.MDB_SET_RANGE
import torch.lmdb.enums.{GetOp, SeekOp}
import torch.lmdb.exceptions.{LmdbException, LmdbNativeException}
import torch.lmdb.flags.PutFlags
import torch.lmdb.utils.ReferenceUtil

/**
 * A cursor handle.
 *
 * @param < T> buffer type
 */
object Cursor {
  /** Cursor has already been closed. */
  @SerialVersionUID(1L)
  final class ClosedException

  /** Creates a new instance. */
    extends LmdbException("Cursor has already been closed") {
  }

  /** Cursor stack too deep - internal error. */
  @SerialVersionUID(1L)
  object FullException {
    private[torch] val MDB_CURSOR_FULL = -30_787
  }

  @SerialVersionUID(1L)
  final class FullException private[torch] extends LmdbNativeException(FullException.MDB_CURSOR_FULL, "Cursor stack too deep - internal error") {
  }
}

final class Cursor[T] private[torch](private val ptrCursor: Pointer, private var txn: Txn[T], private val env: Env[T]) extends AutoCloseable {
  requireNonNull(ptrCursor)
  requireNonNull(txn)
  this.kv = txn.newKeyVal
  private var closed = false
  final private var kv: KeyVal[T] = null

  /**
   * Close a cursor handle.
   *
   * <p>The cursor handle will be freed and must not be used again after this call. Its transaction
   * must still be live if it is a write-transaction.
   */
  override def close(): Unit = {
    if (closed) return
    kv.close()
    if (SHOULD_CHECK) {
      env.checkNotClosed()
      if (!txn.isReadOnly) txn.checkReady()
    }
    LIB.mdb_cursor_close(ptrCursor)
    closed = true
  }

  /**
   * Return count of duplicates for current key.
   *
   * <p>This call is only valid on databases that support sorted duplicate data items {@link 
 * DbiFlags#MDB_DUPSORT}.
   *
   * @return count of duplicates for current key
   */
  def count: Long = {
    if (SHOULD_CHECK) {
      env.checkNotClosed()
      checkNotClosed()
      txn.checkReady()
    }
    val longByReference = new NativeLongByReference
    checkRc(LIB.mdb_cursor_count(ptrCursor, longByReference))
    longByReference.longValue
  }

  /**
   * Delete current key/data pair.
   *
   * <p>This function deletes the key/data pair to which the cursor refers.
   *
   * @param f flags (either null or {@link PutFlags# MDB_NODUPDATA}
   */
  def delete(f: PutFlags*): Unit = {
    if (SHOULD_CHECK) {
      env.checkNotClosed()
      checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    val flags = mask(true, f*)
    checkRc(LIB.mdb_cursor_del(ptrCursor, flags))
  }

  /**
   * Position at first key/data item.
   *
   * @return false if requested position not found
   */
  def first: Boolean = seek(MDB_FIRST)

  /**
   * Reposition the key/value buffers based on the passed key and operation.
   *
   * @param key  to search for
   * @param data to search for
   * @param op   options for this operation
   * @return false if key not found
   */
  def get(key: T, data: T, op: SeekOp): Boolean = {
    if (SHOULD_CHECK) {
      requireNonNull(key)
      requireNonNull(op)
      env.checkNotClosed()
      checkNotClosed()
      txn.checkReady()
    }
    kv.keyIn(key)
    kv.valIn(data)
    val rc = LIB.mdb_cursor_get(ptrCursor, kv.pointerKey, kv.pointerVal, op.getCode)
    if (rc == MDB_NOTFOUND) return false
    checkRc(rc)
    kv.keyOut
    kv.valOut
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    true
  }

  /**
   * Reposition the key/value buffers based on the passed key and operation.
   *
   * @param key to search for
   * @param op  options for this operation
   * @return false if key not found
   */
  def get(key: T, op: GetOp): Boolean = {
    if (SHOULD_CHECK) {
      requireNonNull(key)
      requireNonNull(op)
      env.checkNotClosed()
      checkNotClosed()
      txn.checkReady()
    }
    kv.keyIn(key)
    val rc = LIB.mdb_cursor_get(ptrCursor, kv.pointerKey, kv.pointerVal, op.getCode)
    if (rc == MDB_NOTFOUND) return false
    checkRc(rc)
    kv.keyOut
    kv.valOut
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    true
  }

  /**
   * Obtain the key.
   *
   * @return the key that the cursor is located at.
   */
  def key: T = kv.key

  /**
   * Position at last key/data item.
   *
   * @return false if requested position not found
   */
  def last: Boolean = seek(MDB_LAST)

  /**
   * Position at next data item.
   *
   * @return false if requested position not found
   */
  def next: Boolean = seek(MDB_NEXT)

  /**
   * Position at previous data item.
   *
   * @return false if requested position not found
   */
  def prev: Boolean = seek(MDB_PREV)

  /**
   * Store by cursor.
   *
   * <p>This function stores key/data pairs into the database.
   *
   * @param key key to store
   * @param val data to store
   * @param op  options for this operation
   * @return true if the value was put, false if MDB_NOOVERWRITE or MDB_NODUPDATA were set and the
   *         key/value existed already.
   */
  def put(key: T, vals: T, op: PutFlags*): Boolean = {
    if (SHOULD_CHECK) {
      requireNonNull(key)
      requireNonNull(vals)
      env.checkNotClosed()
      checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    kv.keyIn(key)
    kv.valIn(vals)
    val maskFlag = mask(true, op*)
    val rc = LIB.mdb_cursor_put(ptrCursor, kv.pointerKey, kv.pointerVal, maskFlag)
    if (rc == MDB_KEYEXIST) {
      if (isSet(maskFlag, MDB_NOOVERWRITE)) kv.valOut // marked as in,out in LMDB C docs
      else if (!isSet(maskFlag, MDB_NODUPDATA)) checkRc(rc)
      return false
    }
    checkRc(rc)
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    ReferenceUtil.reachabilityFence0(vals.asInstanceOf[AnyRef])
    true
  }

  /**
   * Put multiple values into the database in one <code>MDB_MULTIPLE</code> operation.
   *
   * <p>The database must have been opened with {@link DbiFlags# MDB_DUPFIXED}. The buffer must
   * contain fixed-sized values to be inserted. The size of each element is calculated from the
   * buffer's size divided by the given element count. For example, to populate 10 X 4 byte integers
   * at once, present a buffer of 40 bytes and specify the element as 10.
   *
   * @param key      key to store in the database (not null)
   * @param val      value to store in the database (not null)
   * @param elements number of elements contained in the passed value buffer
   * @param op       options for operation (must set <code>MDB_MULTIPLE</code>)
   */
  def putMultiple(key: T, vals: T, elements: Int, op: PutFlags*): Unit = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      requireNonNull(key)
      requireNonNull(vals)
      env.checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    val maskFlags = mask(true, op*)
    if (SHOULD_CHECK && !isSet(maskFlags, MDB_MULTIPLE)) throw new IllegalArgumentException("Must set " + MDB_MULTIPLE + " flag")
    txn.kv.keyIn(key)
    val dataPtr = txn.kv.valInMulti(vals, elements)
    val rc = LIB.mdb_cursor_put(ptrCursor, txn.kv.pointerKey, dataPtr, maskFlags)
    checkRc(rc)
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    ReferenceUtil.reachabilityFence0(vals.asInstanceOf[AnyRef])
  }

  /**
   * Renew a cursor handle.
   *
   * <p>A cursor is associated with a specific transaction and database. Cursors that are only used
   * in read-only transactions may be re-used, to avoid unnecessary malloc/free overhead. The cursor
   * may be associated with a new read-only transaction, and referencing the same database handle as
   * it was created with. This may be done whether the previous transaction is live or dead.
   *
   * @param newTxn transaction handle
   */
  def renew(newTxn: Txn[T]): Unit = {
    if (SHOULD_CHECK) {
      requireNonNull(newTxn)
      env.checkNotClosed()
      checkNotClosed()
      this.txn.checkReadOnly() // existing
      newTxn.checkReadOnly()
      newTxn.checkReady()
    }
    checkRc(LIB.mdb_cursor_renew(newTxn.pointer, ptrCursor))
    this.txn = newTxn
  }

  /**
   * Reserve space for data of the given size, but don't copy the given val. Instead, return a
   * pointer to the reserved space, which the caller can fill in later - before the next update
   * operation or the transaction ends. This saves an extra memcpy if the data is being generated
   * later. LMDB does nothing else with this memory, the caller is expected to modify all of the
   * space requested.
   *
   * <p>This flag must not be specified if the database was opened with MDB_DUPSORT
   *
   * @param key  key to store in the database (not null)
   * @param size size of the value to be stored in the database (not null)
   * @param op   options for this operation
   * @return a buffer that can be used to modify the value
   */
  def reserve(key: T, size: Int, op: PutFlags*): T = {
    if (SHOULD_CHECK) {
      requireNonNull(key)
      env.checkNotClosed()
      checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    kv.keyIn(key)
    kv.valIn(size)
    val flags = mask(true, op*) | MDB_RESERVE.getMask
    checkRc(LIB.mdb_cursor_put(ptrCursor, kv.pointerKey, kv.pointerVal, flags))
    kv.valOut
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    vals
  }

  /**
   * Reposition the key/value buffers based on the passed operation.
   *
   * @param op options for this operation
   * @return false if requested position not found
   */
  def seek(op: SeekOp): Boolean = {
    if (SHOULD_CHECK) {
      requireNonNull(op)
      env.checkNotClosed()
      checkNotClosed()
      txn.checkReady()
    }
    val rc = LIB.mdb_cursor_get(ptrCursor, kv.pointerKey, kv.pointerVal, op.getCode)
    if (rc == MDB_NOTFOUND) return false
    checkRc(rc)
    kv.keyOut
    kv.valOut
    true
  }

  /**
   * Obtain the value.
   *
   * @return the value that the cursor is located at.
   */
  def vals: T = kv.vals

  private def checkNotClosed(): Unit = {
    if (closed) throw new Cursor.ClosedException
  }
}