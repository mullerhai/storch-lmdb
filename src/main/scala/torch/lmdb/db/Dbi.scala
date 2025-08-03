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
import jnr.ffi.Memory.allocateDirect
import jnr.ffi.NativeType.ADDRESS
import torch.lmdb.db.Dbi.KeyExistsException.MDB_KEYEXIST
import torch.lmdb.db.Dbi.KeyNotFoundException.MDB_NOTFOUND
import torch.lmdb.db.Env.SHOULD_CHECK
import KeyRange.all
import torch.lmdb.db.Library.LIB
import torch.lmdb.db.Library.RUNTIME
import torch.lmdb.flags.MaskedFlag.isSet
import torch.lmdb.flags.MaskedFlag.mask
import torch.lmdb.flags.PutFlags.MDB_NODUPDATA
import torch.lmdb.flags.PutFlags.MDB_NOOVERWRITE
import torch.lmdb.flags.PutFlags.MDB_RESERVE
import torch.lmdb.ResultCodeMapper.checkRc

import java.util
import java.util.Comparator
import jnr.ffi.Pointer
import jnr.ffi.byref.IntByReference
import jnr.ffi.byref.PointerByReference
import torch.lmdb.db.Library.ComparatorCallback
import torch.lmdb.db.Library.MDB_stat
import torch.lmdb.exceptions.LmdbNativeException
import torch.lmdb.flags.{DbiFlags, PutFlags}
import torch.lmdb.proxy.BufferProxy
import torch.lmdb.utils.ReferenceUtil

/**
 * LMDB Database.
 *
 * @param < T> buffer type
 */
object Dbi {
  /** The specified DBI was changed unexpectedly. */
  @SerialVersionUID(1L)
  object BadDbiException {
    private[torch] val MDB_BAD_DBI = -30_780
  }

  @SerialVersionUID(1L)
  final class BadDbiException private[torch] extends LmdbNativeException(BadDbiException.MDB_BAD_DBI, "The specified DBI was changed unexpectedly") {
  }

  /** Unsupported size of key/DB name/data, or wrong DUPFIXED size. */
  @SerialVersionUID(1L)
  object BadValueSizeException {
    private[torch] val MDB_BAD_VALSIZE = -30_781
  }

  @SerialVersionUID(1L)
  final class BadValueSizeException private[torch] extends LmdbNativeException(BadValueSizeException.MDB_BAD_VALSIZE, "Unsupported size of key/DB name/data, or wrong DUPFIXED size") {
  }

  /** Environment maxdbs reached. */
  @SerialVersionUID(1L)
  object DbFullException {
    private[torch] val MDB_DBS_FULL = -30_791
  }

  @SerialVersionUID(1L)
  final class DbFullException private[torch] extends LmdbNativeException(DbFullException.MDB_DBS_FULL, "Environment maxdbs reached") {
  }

  /**
   * Operation and DB incompatible, or DB type changed.
   *
   * <p>This can mean:
   *
   * <ul>
   * <li>The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.
   * <li>Opening a named DB when the unnamed DB has MDB_DUPSORT / MDB_INTEGERKEY.
   * <li>Accessing a data record as a database, or vice versa.
   * <li>The database was dropped and recreated with different flags.
   * </ul>
   */
  @SerialVersionUID(1L)
  object IncompatibleException {
    private[torch] val MDB_INCOMPATIBLE = -30_784
  }

  @SerialVersionUID(1L)
  final class IncompatibleException private[torch] extends LmdbNativeException(IncompatibleException.MDB_INCOMPATIBLE, "Operation and DB incompatible, or DB type changed") {
  }

  /** Key/data pair already exists. */
  @SerialVersionUID(1L)
  object KeyExistsException {
    private[torch] val MDB_KEYEXIST = -30_799
  }

  @SerialVersionUID(1L)
  final class KeyExistsException private[torch] extends LmdbNativeException(KeyExistsException.MDB_KEYEXIST, "key/data pair already exists") {
  }

  /** Key/data pair not found (EOF). */
  @SerialVersionUID(1L)
  object KeyNotFoundException {
    private[torch] val MDB_NOTFOUND = -30_798
  }

  @SerialVersionUID(1L)
  final class KeyNotFoundException private[torch] extends LmdbNativeException(KeyNotFoundException.MDB_NOTFOUND, "key/data pair not found (EOF)") {
  }

  /** Database contents grew beyond environment mapsize. */
  @SerialVersionUID(1L)
  object MapResizedException {
    private[torch] val MDB_MAP_RESIZED = -30_785
  }

  @SerialVersionUID(1L)
  final class MapResizedException private[torch] extends LmdbNativeException(MapResizedException.MDB_MAP_RESIZED, "Database contents grew beyond environment mapsize") {
  }
}

final class Dbi[T] private[torch](private val env: Env[T],var txn: Txn[T],var name: Array[Byte],var comparator: Comparator[T],var nativeCb: Boolean,var proxy: BufferProxy[T], flags: DbiFlags*) {
  if (SHOULD_CHECK) {
    requireNonNull(txn)
    txn.checkReady()
  }
  final private var ccb: Library.ComparatorCallback = null
  private var cleaned = false
//  final private var comparator: Comparator[T] = null
  name = if (name == null) then null else util.Arrays.copyOf(name, name.length)
  final private var ptr: Pointer = null
  if (comparator == null) this.comparator = proxy.getComparator(flags*)
  else this.comparator = comparator
  val flagsMask: Int = mask(true, flags*)
  val dbiPtr: Pointer = allocateDirect(RUNTIME, ADDRESS)
  checkRc(LIB.mdb_dbi_open(txn.pointer, name, flagsMask, dbiPtr))
  val ptrCursor = dbiPtr.getPointer(0)
  if (nativeCb) {
    this.ccb = (keyA: Pointer, keyB: Pointer) => {
      val compKeyA = proxy.allocate
      val compKeyB = proxy.allocate
      proxy.out(compKeyA, keyA, keyA.address)
      proxy.out(compKeyB, keyB, keyB.address)
      val result = this.comparator.compare(compKeyA, compKeyB)
      proxy.deallocate(compKeyA)
      proxy.deallocate(compKeyB)
      result
    }
    LIB.mdb_set_compare(txn.pointer, ptrCursor, ccb)
  }
  else ccb = null


  /**
   * Close the database handle (normally unnecessary; use with caution).
   *
   * <p>It is very rare that closing a database handle is useful. There are also many
   * warnings/restrictions if closing a database handle (refer to the LMDB C documentation). As such
   * this is non-routine usage and this class does not track the open/closed state of the {@link 
 * Dbi}. Advanced users are expected to have specific reasons for using this method and will
   * manage their own state accordingly.
   */
  def close(): Unit = {
    clean()
    if (SHOULD_CHECK) env.checkNotClosed()
    LIB.mdb_dbi_close(env.pointer, ptr)
  }

  /**
   * Starts a new read-write transaction and deletes the key.
   *
   * @param key key to delete from the database (not null)
   * @return true if the key/data pair was found, false otherwise
   * @see #delete(org.torch.lmdb.Txn, java.lang.Object, java.lang.Object)
   */
  def delete(key: T): Boolean = try {
    val txn = env.txnWrite
    try {
      val ret = delete(txn, key)
      txn.commit()
      ret
    } finally if (txn != null) txn.close()
  }

  /**
   * Deletes the key using the passed transaction.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @return true if the key/data pair was found, false otherwise
   * @see #delete(org.torch.lmdb.Txn, java.lang.Object, java.lang.Object)
   */
  def delete(txn: Txn[T], key: T): Boolean = delete(txn, key, null)

  /**
   * Removes key/data pairs from the database.
   *
   * <p>If the database does not support sorted duplicate data items ({@link DbiFlags# MDB_DUPSORT})
   * the value parameter is ignored. If the database supports sorted duplicates and the value
   * parameter is null, all of the duplicate data items for the key will be deleted. Otherwise, if
   * the data parameter is non-null only the matching data item will be deleted.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @param val value to delete from the database (null permitted)
   * @return true if the key/data pair was found, false otherwise
   */
  def delete(txn: Txn[T], key: T, vals: Option[T]): Boolean = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      requireNonNull(key)
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    txn.kv.keyIn(key)
    var data: Pointer = null
    if (vals != null) {
      if vals.isDefined then txn.kv.valIn(vals.get)
      data = txn.kv.pointerVal
    }
    val rc = LIB.mdb_del(txn.pointer, ptr, txn.kv.pointerKey, data)
    if (rc == MDB_NOTFOUND) return false
    checkRc(rc)
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    ReferenceUtil.reachabilityFence0(vals.asInstanceOf[AnyRef])
    true
  }

  /**
   * Drops the data in this database, leaving the database open for further use.
   *
   * <p>This method slightly differs from the LMDB C API in that it does not provide support for
   * also closing the DB handle. If closing the DB handle is required, please see {@link # close ( )}.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   */
  def drop(txn: Txn[T]): Unit = {
    drop(txn, false)
  }

  /**
   * Drops the database. If delete is set to true, the database will be deleted and handle will be
   * closed. See {@link # close ( )} for implication of handle close. Otherwise, only the data in this
   * database will be dropped.
   *
   * @param txn    transaction handle (not null; not committed; must be R-W)
   * @param delete whether database should be deleted.
   */
  def drop(txn: Txn[T], delete: Boolean): Unit = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      env.checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    if (delete) clean()
    val del = if (delete) 1
    else 0
    checkRc(LIB.mdb_drop(txn.pointer, ptr, del))
  }

  /**
   * Get items from a database, moving the {@link Txn# val ( )} to the value.
   *
   * <p>This function retrieves key/data pairs from the database. The address and length of the data
   * associated with the specified \b key are returned in the structure to which \b data refers. If
   * the database supports duplicate keys ({@link org.torch.lmdb.DbiFlags# MDB_DUPSORT}) then the first
   * data item for the key will be returned. Retrieval of other items requires the use of
   * #mdb_cursor_get().
   *
   * @param txn transaction handle (not null; not committed)
   * @param key key to search for in the database (not null)
   * @return the data or null if not found
   */
  def get(txn: Txn[T], key: T): Option[T] = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      requireNonNull(key)
      env.checkNotClosed()
      txn.checkReady()
    }
    txn.kv.keyIn(key)
    val rc = LIB.mdb_get(txn.pointer, ptr, txn.kv.pointerKey, txn.kv.pointerVal)
    if (rc == MDB_NOTFOUND) return None
    checkRc(rc)
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    Some(txn.kv.valOut) // marked as out in LMDB C docs
  }

  /**
   * Obtains the name of this database.
   *
   * @return the name (may be null)
   */
  def getName: Array[Byte] = if (name == null) null
  else util.Arrays.copyOf(name, name.length)

  /**
   * Iterate the database from the first item and forwards.
   *
   * @param txn transaction handle (not null; not committed)
   * @return iterator
   */
  def iterate(txn: Txn[T]): CursorIterable[T] = iterate(txn, all)

  /**
   * Iterate the database in accordance with the provided {@link KeyRange}.
   *
   * @param txn   transaction handle (not null; not committed)
   * @param range range of acceptable keys (not null)
   * @return iterator (never null)
   */
  def iterate(txn: Txn[T], range: KeyRange[T]): CursorIterable[T] = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      requireNonNull(range)
      env.checkNotClosed()
      txn.checkReady()
    }
    new CursorIterable[T](txn, this, range, comparator)
  }

  /**
   * Return DbiFlags for this Dbi.
   *
   * @param txn transaction handle (not null; not committed)
   * @return the list of flags this Dbi was created with
   */
  def listFlags(txn: Txn[T]): util.List[DbiFlags] = {
    if (SHOULD_CHECK) env.checkNotClosed()
    val resultPtr = new IntByReference
    checkRc(LIB.mdb_dbi_flags(txn.pointer, ptr, resultPtr))
    val flags = resultPtr.intValue
    val result = new util.ArrayList[DbiFlags]
    for (flag <- DbiFlags.values) {
      if (isSet(flags, flag)) result.add(flag)
    }
    result
  }

  /**
   * Create a cursor handle.
   *
   * <p>A cursor is associated with a specific transaction and database. A cursor cannot be used
   * when its database handle is closed. Nor when its transaction has ended, except with {@link 
 * Cursor#renew(org.torch.lmdb.Txn)}. It can be discarded with {@link Cursor# close ( )}. A cursor in a
   * write-transaction can be closed before its transaction ends, and will otherwise be closed when
   * its transaction ends. A cursor in a read-only transaction must be closed explicitly, before or
   * after its transaction ends. It can be reused with {@link Cursor# renew ( org.torch.lmdb.Txn )} before
   * finally closing it.
   *
   * @param txn transaction handle (not null; not committed)
   * @return cursor handle
   */
  def openCursor(txn: Txn[T]): Cursor[T] = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      env.checkNotClosed()
      txn.checkReady()
    }
    val cursorPtr = new PointerByReference
    checkRc(LIB.mdb_cursor_open(txn.pointer, ptr, cursorPtr))
    new Cursor[T](cursorPtr.getValue, txn, env)
  }

  /**
   * Starts a new read-write transaction and puts the key/data pair.
   *
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @see #put(org.torch.lmdb.Txn, java.lang.Object, java.lang.Object, org.torch.lmdb.PutFlags...)
   */
  def put(key: T, vals: T): Unit = {
    try {
      val txn = env.txnWrite
      try {
        put(txn, key, vals)
        txn.commit()
      } finally if (txn != null) txn.close()
    }
  }

  /**
   * Store a key/value pair in the database.
   *
   * <p>This function stores key/data pairs in the database. The default behavior is to enter the
   * new key/data pair, replacing any previously existing key if duplicates are disallowed, or
   * adding a duplicate data item if duplicates are allowed ({@link DbiFlags# MDB_DUPSORT}).
   *
   * @param txn   transaction handle (not null; not committed; must be R-W)
   * @param key   key to store in the database (not null)
   * @param val   value to store in the database (not null)
   * @param flags Special options for this operation
   * @return true if the value was put, false if MDB_NOOVERWRITE or MDB_NODUPDATA were set and the
   *         key/value existed already.
   */
  def put(txn: Txn[T], key: T, vals: T, flags: PutFlags*): Boolean = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      requireNonNull(key)
      requireNonNull(vals)
      env.checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    txn.kv.keyIn(key)
    txn.kv.valIn(vals)
    val maskFlag = mask(true, flags*)
    val rc = LIB.mdb_put(txn.pointer, ptr, txn.kv.pointerKey, txn.kv.pointerVal, maskFlag)
    if (rc == MDB_KEYEXIST) {
      if (isSet(maskFlag, MDB_NOOVERWRITE)) txn.kv.valOut // marked as in,out in LMDB C docs
      else if (!isSet(maskFlag, MDB_NODUPDATA)) checkRc(rc)
      return false
    }
    checkRc(rc)
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    ReferenceUtil.reachabilityFence0(vals.asInstanceOf[AnyRef])
    true
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
   * @param txn  transaction handle (not null; not committed; must be R-W)
   * @param key  key to store in the database (not null)
   * @param size size of the value to be stored in the database
   * @param op   options for this operation
   * @return a buffer that can be used to modify the value
   */
  def reserve(txn: Txn[T], key: T, size: Int, op: PutFlags*): T = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      requireNonNull(key)
      env.checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    txn.kv.keyIn(key)
    txn.kv.valIn(size)
    val flags = mask(true, op*) | MDB_RESERVE.getMask
    checkRc(LIB.mdb_put(txn.pointer, ptr, txn.kv.pointerKey, txn.kv.pointerVal, flags))
    txn.kv.valOut // marked as in,out in LMDB C docs
    ReferenceUtil.reachabilityFence0(key.asInstanceOf[AnyRef])
    txn.vals
  }

  /**
   * Return statistics about this database.
   *
   * @param txn transaction handle (not null; not committed)
   * @return an immutable statistics object.
   */
  def stat(txn: Txn[T]): Stat = {
    if (SHOULD_CHECK) {
      requireNonNull(txn)
      env.checkNotClosed()
      txn.checkReady()
    }
    val stat = new Library.MDB_stat(RUNTIME)
    checkRc(LIB.mdb_stat(txn.pointer, ptr, stat))
    new Stat(stat.f0_ms_psize.intValue, stat.f1_ms_depth.intValue, stat.f2_ms_branch_pages.longValue, stat.f3_ms_leaf_pages.longValue, stat.f4_ms_overflow_pages.longValue, stat.f5_ms_entries.longValue)
  }

  private def clean(): Unit = {
    if (cleaned) return
    cleaned = true
  }
}