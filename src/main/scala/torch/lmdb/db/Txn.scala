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

import jnr.ffi.Memory.allocateDirect
import jnr.ffi.NativeType.ADDRESS
import jnr.ffi.Pointer
import torch.lmdb.db.Env.SHOULD_CHECK
import torch.lmdb.db.Library.{LIB, RUNTIME}
import torch.lmdb.ResultCodeMapper.checkRc
import torch.lmdb.db.Txn.State.{DONE, READY, RELEASED, RESET}
import torch.lmdb.exceptions.{LmdbException, LmdbNativeException}
import torch.lmdb.flags.MaskedFlag.{isSet, mask}
import torch.lmdb.flags.TxnFlags
import torch.lmdb.flags.TxnFlags.MDB_RDONLY_TXN
import torch.lmdb.proxy.BufferProxy

/**
 * LMDB transaction.
 *
 * @param < T> buffer type
 */
object Txn {
  /** Transaction must abort, has a child, or is invalid. */
  @SerialVersionUID(1L)
  object BadException {
    val MDB_BAD_TXN = -30_782
  }

  @SerialVersionUID(1L)
  final class BadException extends LmdbNativeException(BadException.MDB_BAD_TXN, "Transaction must abort, has a child, or is invalid") {
  }

  /** Invalid reuse of reader locktable slot. */
  @SerialVersionUID(1L)
  object BadReaderLockException {
    val MDB_BAD_RSLOT = -30_783
  }

  @SerialVersionUID(1L)
  final class BadReaderLockException extends LmdbNativeException(BadReaderLockException.MDB_BAD_RSLOT, "Invalid reuse of reader locktable slot") {
  }

  /** The proposed R-W transaction is incompatible with a R-O Env. */
  @SerialVersionUID(1L)
  class EnvIsReadOnly extends LmdbException("Read-write Txn incompatible with read-only Env") {
  }

  /** The proposed transaction is incompatible with its parent transaction. */
  @SerialVersionUID(1L)
  class IncompatibleParent extends LmdbException("Transaction incompatible with its parent transaction") {
  }

  /** Transaction is not in a READY state. */
  @SerialVersionUID(1L)
  final class NotReadyException extends LmdbException("Transaction is not in ready state") {
  }

  /** The current transaction has not been reset. */
  @SerialVersionUID(1L)
  class NotResetException

  /** Creates a new instance. */
    extends LmdbException("Transaction has not been reset") {
  }

  /** The current transaction is not a read-only transaction. */
  @SerialVersionUID(1L)
  class ReadOnlyRequiredException

  /** Creates a new instance. */
    extends LmdbException("Not a read-only transaction") {
  }

  /** The current transaction is not a read-write transaction. */
  @SerialVersionUID(1L)
  class ReadWriteRequiredException

  /** Creates a new instance. */
    extends LmdbException("Not a read-write transaction") {
  }

  /** The current transaction has already been reset. */
  @SerialVersionUID(1L)
  class ResetException

  /** Creates a new instance. */
    extends LmdbException("Transaction has already been reset") {
  }

  /** Transaction has too many dirty pages. */
  @SerialVersionUID(1L)
  object TxFullException {
    val MDB_TXN_FULL = -30_788
  }

  @SerialVersionUID(1L)
  final class TxFullException extends LmdbNativeException(TxFullException.MDB_TXN_FULL, "Transaction has too many dirty pages") {
  }

  /** Transaction states. */
  enum State:
    case READY, DONE, RESET, RELEASED

}

final class Txn[T] (private val env: Env[T], private val parent: Txn[T], private val proxy: BufferProxy[T], flags: TxnFlags*) extends AutoCloseable {
  final private var keyVal: KeyVal[T] = proxy.keyVal
  final private var ptr: Pointer = null
  final private var readOnly = false
  private var state: Txn.State = READY
//  this.keyVal = proxy.keyVal
  val flagsMask: Int = mask(true, flags*)
  this.readOnly = isSet(flagsMask, MDB_RDONLY_TXN)
  if (env.isReadOnly && !this.readOnly) throw new Txn.EnvIsReadOnly
  if (parent != null && parent.isReadOnly != this.readOnly) throw new Txn.IncompatibleParent
  val txnPtr: Pointer = allocateDirect(RUNTIME, ADDRESS)
  val txnParentPtr: Pointer = if (parent == null) null
  else parent.ptrCursor
  checkRc(LIB.mdb_txn_begin(env.pointer, txnParentPtr, flagsMask, txnPtr))
  val ptrCursor = txnPtr.getPointer(0)
//  state = READY


  /** Aborts this transaction. */
  def abort(): Unit = {
    if (SHOULD_CHECK) env.checkNotClosed()
    checkReady()
    state = DONE
    LIB.mdb_txn_abort(ptr)
  }

  /**
   * Closes this transaction by aborting if not already committed.
   *
   * <p>Closing the transaction will invoke {@link BufferProxy# deallocate ( java.lang.Object )} for
   * each read-only buffer (ie the key and value).
   */
  override def close(): Unit = {
    if (SHOULD_CHECK) env.checkNotClosed()
    if (state eq RELEASED) return
    if (state eq READY) LIB.mdb_txn_abort(ptr)
    keyVal.close()
    state = RELEASED
  }

  /** Commits this transaction. */
  def commit(): Unit = {
    if (SHOULD_CHECK) env.checkNotClosed()
    checkReady()
    state = DONE
    checkRc(LIB.mdb_txn_commit(ptr))
  }

  /**
   * Return the transaction's ID.
   *
   * @return A transaction ID, valid if input is an active transaction
   */
  def getId: Long = {
    if (SHOULD_CHECK) env.checkNotClosed()
    LIB.mdb_txn_id(ptr)
  }

  /**
   * Obtains this transaction's parent.
   *
   * @return the parent transaction (may be null)
   */
  def getParent: Txn[T] = parent

  /**
   * Whether this transaction is read-only.
   *
   * @return if read-only
   */
  def isReadOnly: Boolean = readOnly

  /**
   * Fetch the buffer which holds a read-only view of the LMDI allocated memory. Any use of this
   * buffer must comply with the standard LMDB C "mdb_get" contract (ie do not modify, do not
   * attempt to release the memory, do not use once the transaction or cursor closes, do not use
   * after a write etc).
   *
   * @return the key buffer (never null)
   */
  def key: T = keyVal.key

  /** Renews a read-only transaction previously released by {@link # reset ( )}. */
  def renew(): Unit = {
    if (SHOULD_CHECK) env.checkNotClosed()
    if (state ne RESET) throw new Txn.NotResetException
    state = DONE
    checkRc(LIB.mdb_txn_renew(ptr))
    state = READY
  }

  /**
   * Aborts this read-only transaction and resets the transaction handle so it can be reused upon
   * calling {@link # renew ( )}.
   */
  def reset(): Unit = {
    if (SHOULD_CHECK) env.checkNotClosed()
    checkReadOnly()
    if ((state ne READY) && (state ne DONE)) throw new Txn.ResetException
    state = RESET
    LIB.mdb_txn_reset(ptr)
  }

  /**
   * Fetch the buffer which holds a read-only view of the LMDI allocated memory. Any use of this
   * buffer must comply with the standard LMDB C "mdb_get" contract (ie do not modify, do not
   * attempt to release the memory, do not use once the transaction or cursor closes, do not use
   * after a write etc).
   *
   * @return the value buffer (never null)
   */
  def vals: T = keyVal.vals

  def checkReadOnly(): Unit = {
    if (!readOnly) throw new Txn.ReadOnlyRequiredException
  }

  def checkReady(): Unit = {
    if (state ne READY) throw new Txn.NotReadyException
  }

  def checkWritesAllowed(): Unit = {
    if (readOnly) throw new Txn.ReadWriteRequiredException
  }

  /**
   * Return the state of the transaction.
   *
   * @return the state
   */
  def getState = state

  def kv = keyVal

  def newKeyVal = proxy.keyVal

  def pointer = ptr
}