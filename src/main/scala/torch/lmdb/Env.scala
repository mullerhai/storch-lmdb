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

import java.lang.Boolean.getBoolean
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Objects.requireNonNull
import torch.lmdb.ByteBufferProxy.PROXY_OPTIMAL
import torch.lmdb.EnvFlags.MDB_NOSUBDIR
import torch.lmdb.EnvFlags.MDB_RDONLY_ENV
import torch.lmdb.Library.LIB
import torch.lmdb.Library.RUNTIME
import torch.lmdb.MaskedFlag.isSet
import torch.lmdb.MaskedFlag.mask
import torch.lmdb.ResultCodeMapper.checkRc
import torch.lmdb.TxnFlags.MDB_RDONLY_TXN
import java.io.File
import java.nio.ByteBuffer
import java.util
import java.util.Collections
import java.util.Comparator
import jnr.ffi.Pointer
import jnr.ffi.byref.IntByReference
import jnr.ffi.byref.PointerByReference
import torch.lmdb.Library.MDB_envinfo
import torch.lmdb.Library.MDB_stat

/**
 * LMDB environment.
 *
 * @param < T> buffer type
 */
object Env {
  /** Java system property name that can be set to disable optional checks. */
  val DISABLE_CHECKS_PROP = "lmdbjava.disable.checks"
  /**
   * Indicates whether optional checks should be applied in LmdbJava. Optional checks are only
   * disabled in critical paths (see package-level JavaDocs). Non-critical paths have optional
   * checks performed at all times, regardless of this property.
   */
  val SHOULD_CHECK: Boolean = !getBoolean(DISABLE_CHECKS_PROP)

  /**
   * Create an {@link Env} using the {@link ByteBufferProxy# PROXY_OPTIMAL}.
   *
   * @return the environment (never null)
   */
  def create = new Env.Builder[ByteBuffer](PROXY_OPTIMAL)

  /**
   * Create an {@link Env} using the passed {@link BufferProxy}.
   *
   * @param <     T> buffer type
   * @param proxy the proxy to use (required)
   * @return the environment (never null)
   */
  def create[T](proxy: BufferProxy[T]) = new Env.Builder[T](proxy)

  /**
   * Opens an environment with a single default database in 0664 mode using the {@link 
 * ByteBufferProxy#PROXY_OPTIMAL}.
   *
   * @param path  file system destination
   * @param size  size in megabytes
   * @param flags the flags for this new environment
   * @return env the environment (never null)
   */
  def open(path: File, size: Int, flags: EnvFlags*): Env[ByteBuffer] = new Env.Builder[ByteBuffer](PROXY_OPTIMAL).setMapSize(size * 1_024L * 1_024L).open(path, flags)

  /** Object has already been closed and the operation is therefore prohibited. */
  @SerialVersionUID(1L)
  final class AlreadyClosedException

  /** Creates a new instance. */
    extends LmdbException("Environment has already been closed") {
  }

  /** Object has already been opened and the operation is therefore prohibited. */
  @SerialVersionUID(1L)
  final class AlreadyOpenException

  /** Creates a new instance. */
    extends LmdbException("Environment has already been opened") {
  }

  /**
   * Builder for configuring and opening Env.
   *
   * @param < T> buffer type
   */
  object Builder {
    private[torch] val MAX_READERS_DEFAULT = 126
  }

  final class Builder[T] private[torch](private val proxy: BufferProxy[T]) {
    requireNonNull(proxy)
    private var mapSize = 1_024 * 1_024
    private var maxDbs = 1
    private var maxReaders = Builder.MAX_READERS_DEFAULT
    private var opened = false

    /**
     * Opens the environment.
     *
     * @param path  file system destination
     * @param mode  Unix permissions to set on created files and semaphores
     * @param flags the flags for this new environment
     * @return an environment ready for use
     */
    def open(path: File, mode: Int, flags: EnvFlags*): Env[T] = {
      requireNonNull(path)
      if (opened) throw new Env.AlreadyOpenException
      opened = true
      val envPtr = new PointerByReference
      checkRc(LIB.mdb_env_create(envPtr))
      val ptr = envPtr.getValue
      try {
        checkRc(LIB.mdb_env_set_mapsize(ptr, mapSize))
        checkRc(LIB.mdb_env_set_maxdbs(ptr, maxDbs))
        checkRc(LIB.mdb_env_set_maxreaders(ptr, maxReaders))
        val flagsMask = mask(true, flags)
        val readOnly = isSet(flagsMask, MDB_RDONLY_ENV)
        val noSubDir = isSet(flagsMask, MDB_NOSUBDIR)
        checkRc(LIB.mdb_env_open(ptr, path.getAbsolutePath, flagsMask, mode))
        new Env[T](proxy, ptr, readOnly, noSubDir)
      } catch {
        case e: LmdbNativeException =>
          LIB.mdb_env_close(ptr)
          throw e
      }
    }

    /**
     * Opens the environment with 0664 mode.
     *
     * @param path  file system destination
     * @param flags the flags for this new environment
     * @return an environment ready for use
     */
    def open(path: File, flags: EnvFlags*): Env[T] = open(path, 0664, flags)

    /**
     * Sets the map size.
     *
     * @param mapSize new limit in bytes
     * @return the builder
     */
    def setMapSize(mapSize: Long): Env.Builder[T] = {
      if (opened) throw new Env.AlreadyOpenException
      if (mapSize < 0) throw new IllegalArgumentException("Negative value; overflow?")
      this.mapSize = mapSize
      this
    }

    /**
     * Sets the maximum number of databases (ie {@link Dbi}s permitted.
     *
     * @param dbs new limit
     * @return the builder
     */
    def setMaxDbs(dbs: Int): Env.Builder[T] = {
      if (opened) throw new Env.AlreadyOpenException
      this.maxDbs = dbs
      this
    }

    /**
     * Sets the maximum number of databases permitted.
     *
     * @param readers new limit
     * @return the builder
     */
    def setMaxReaders(readers: Int): Env.Builder[T] = {
      if (opened) throw new Env.AlreadyOpenException
      this.maxReaders = readers
      this
    }
  }

  /** File is not a valid LMDB file. */
  @SerialVersionUID(1L)
  object FileInvalidException {
    private[torch] val MDB_INVALID = -30_793
  }

  @SerialVersionUID(1L)
  final class FileInvalidException private[torch] extends LmdbNativeException(FileInvalidException.MDB_INVALID, "File is not a valid LMDB file") {
  }

  /** The specified copy destination is invalid. */
  @SerialVersionUID(1L)
  final class InvalidCopyDestination(message: String)

  /**
   * Creates a new instance.
   *
   * @param message the reason
   */
    extends LmdbException(message) {
  }

  /** Environment mapsize reached. */
  @SerialVersionUID(1L)
  object MapFullException {
    private[torch] val MDB_MAP_FULL = -30_792
  }

  @SerialVersionUID(1L)
  final class MapFullException private[torch] extends LmdbNativeException(MapFullException.MDB_MAP_FULL, "Environment mapsize reached") {
  }

  /** Environment maxreaders reached. */
  @SerialVersionUID(1L)
  object ReadersFullException {
    private[torch] val MDB_READERS_FULL = -30_790
  }

  @SerialVersionUID(1L)
  final class ReadersFullException private[torch] extends LmdbNativeException(ReadersFullException.MDB_READERS_FULL, "Environment maxreaders reached") {
  }

  /** Environment version mismatch. */
  @SerialVersionUID(1L)
  object VersionMismatchException {
    private[torch] val MDB_VERSION_MISMATCH = -30_794
  }

  @SerialVersionUID(1L)
  final class VersionMismatchException private[torch] extends LmdbNativeException(VersionMismatchException.MDB_VERSION_MISMATCH, "Environment version mismatch") {
  }
}

final class Env[T] private(private val proxy: BufferProxy[T], private val ptr: Pointer, private val readOnly: Boolean, private val noSubDir: Boolean) extends AutoCloseable { // cache max key size to avoid further JNI calls
  this.maxKeySize = LIB.mdb_env_get_maxkeysize(ptrCursor)
  private var closed = false
  final private var maxKeySize = 0

  /**
   * Close the handle.
   *
   * <p>Will silently return if already closed or never opened.
   */
  override def close(): Unit = {
    if (closed) return
    closed = true
    LIB.mdb_env_close(ptr)
  }

  /**
   * Copies an LMDB environment to the specified destination path.
   *
   * <p>This function may be used to make a backup of an existing environment. No lockfile is
   * created, since it gets recreated at need.
   *
   * <p>If this environment was created using {@link EnvFlags# MDB_NOSUBDIR}, the destination path
   * must be a directory that exists but contains no files. If {@link EnvFlags# MDB_NOSUBDIR} was
   * used, the destination path must not exist, but it must be possible to create a file at the
   * provided path.
   *
   * <p>Note: This call can trigger significant file size growth if run in parallel with write
   * transactions, because it employs a read-only transaction. See long-lived transactions under
   * "Caveats" in the LMDB native documentation.
   *
   * @param path  writable destination path as described above
   * @param flags special options for this copy
   */
  def copy(path: File, flags: CopyFlags*): Unit = {
    requireNonNull(path)
    validatePath(path)
    val flagsMask = mask(true, flags)
    checkRc(LIB.mdb_env_copy2(ptr, path.getAbsolutePath, flagsMask))
  }

  /**
   * Obtain the DBI names.
   *
   * <p>This method is only compatible with {@link Env}s that use named databases. If an unnamed
   * {@link Dbi} is being used to store data, this method will attempt to return all such keys from
   * the unnamed database.
   *
   * <p>This method must not be called from concurrent threads.
   *
   * @return a list of DBI names (never null)
   */
  def getDbiNames: util.List[Array[Byte]] = {
    val result = new util.ArrayList[Array[Byte]]
    val names = openDbi(null.asInstanceOf[Array[Byte]])
    try {
      val txn = txnRead
      val cursor = names.openCursor(txn)
      try {
        if (!cursor.first) return Collections.emptyList
        do {
          val name = proxy.getBytes(cursor.key)
          result.add(name)
        } while (cursor.next)
      } finally {
        if (txn != null) txn.close()
        if (cursor != null) cursor.close()
      }
    }
    result
  }

  /**
   * Set the size of the data memory map.
   *
   * @param mapSize the new size, in bytes
   */
  def setMapSize(mapSize: Long): Unit = {
    checkRc(LIB.mdb_env_set_mapsize(ptr, mapSize))
  }

  /**
   * Get the maximum size of keys and MDB_DUPSORT data we can write.
   *
   * @return the maximum size of keys.
   */
  def getMaxKeySize: Int = maxKeySize

  /**
   * Return information about this environment.
   *
   * @return an immutable information object.
   */
  def info: EnvInfo = {
    if (closed) throw new Env.AlreadyClosedException
    val info = new Library.MDB_envinfo(RUNTIME)
    checkRc(LIB.mdb_env_info(ptr, info))
    var mapAddress = 0L
    if (info.f0_me_mapaddr.get == null) mapAddress = 0
    else mapAddress = info.f0_me_mapaddr.get.address
    new EnvInfo(mapAddress, info.f1_me_mapsize.longValue, info.f2_me_last_pgno.longValue, info.f3_me_last_txnid.longValue, info.f4_me_maxreaders.intValue, info.f5_me_numreaders.intValue)
  }

  /**
   * Indicates whether this environment has been closed.
   *
   * @return true if closed
   */
  def isClosed: Boolean = closed

  /**
   * Indicates if this environment was opened with {@link EnvFlags# MDB_RDONLY_ENV}.
   *
   * @return true if read-only
   */
  def isReadOnly: Boolean = readOnly

  /**
   * Convenience method that opens a {@link Dbi} with a UTF-8 database name and default {@link 
 * Comparator} that is not invoked from native code.
   *
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: String, flags: DbiFlags*): Dbi[T] = {
    val nameBytes = if (name == null) null
    else name.getBytes(UTF_8)
    openDbi(nameBytes, null, false, flags)
  }

  /**
   * Convenience method that opens a {@link Dbi} with a UTF-8 database name and associated {@link 
 * Comparator} that is not invoked from native code.
   *
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use default)
   * @param flags      to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: String, comparator: Comparator[T], flags: DbiFlags*): Dbi[T] = {
    val nameBytes = if (name == null) null
    else name.getBytes(UTF_8)
    openDbi(nameBytes, comparator, false, flags)
  }

  /**
   * Convenience method that opens a {@link Dbi} with a UTF-8 database name and associated {@link 
 * Comparator} that may be invoked from native code if specified.
   *
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use default)
   * @param nativeCb   whether native code calls back to the Java comparator
   * @param flags      to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: String, comparator: Comparator[T], nativeCb: Boolean, flags: DbiFlags*): Dbi[T] = {
    val nameBytes = if (name == null) null
    else name.getBytes(UTF_8)
    openDbi(nameBytes, comparator, nativeCb, flags)
  }

  /**
   * Convenience method that opens a {@link Dbi} with a default {@link Comparator} that is not
   * invoked from native code.
   *
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: Array[Byte], flags: DbiFlags*): Dbi[T] = openDbi(name, null, false, flags)

  /**
   * Convenience method that opens a {@link Dbi} with an associated {@link Comparator} that is not
   * invoked from native code.
   *
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param flags      to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: Array[Byte], comparator: Comparator[T], flags: DbiFlags*): Dbi[T] = openDbi(name, comparator, false, flags)

  /**
   * Convenience method that opens a {@link Dbi} with an associated {@link Comparator} that may be
   * invoked from native code if specified.
   *
   * <p>This method will automatically commit the private transaction before returning. This ensures
   * the <code>Dbi</code> is available in the <code>Env</code>.
   *
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param nativeCb   whether native code calls back to the Java comparator
   * @param flags      to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: Array[Byte], comparator: Comparator[T], nativeCb: Boolean, flags: DbiFlags*): Dbi[T] = try {
    val txn = if (readOnly) txnRead
    else txnWrite
    try {
      val dbi = openDbi(txn, name, comparator, nativeCb, flags)
      txn.commit() // even RO Txns require a commit to retain Dbi in Env
      dbi
    } finally if (txn != null) txn.close()
  }

  /**
   * Open the {@link Dbi} using the passed {@link Txn}.
   *
   * <p>The caller must commit the transaction after this method returns in order to retain the
   * <code>Dbi</code> in the <code>Env</code>.
   *
   * <p>A {@link Comparator} may be provided when calling this method. Such comparator is primarily
   * used by {@link CursorIterable} instances. A secondary (but uncommon) use of the comparator is
   * to act as a callback from the native library if <code>nativeCb</code> is <code>true</code>.
   * This is usually avoided due to the overhead of native code calling back into Java. It is
   * instead highly recommended to set the correct {@link DbiFlags} to allow the native library to
   * correctly order the intended keys.
   *
   * <p>A default comparator will be provided if <code>null</code> is passed as the comparator. If a
   * custom comparator is provided, it must strictly match the lexicographical order of keys in the
   * native LMDB database.
   *
   * <p>This method (and its overloaded convenience variants) must not be called from concurrent
   * threads.
   *
   * @param txn        transaction to use (required; not closed)
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param nativeCb   whether native code should call back to the comparator
   * @param flags      to open the database with
   * @return a database that is ready to use
   */
  def openDbi(txn: Txn[T], name: Array[Byte], comparator: Comparator[T], nativeCb: Boolean, flags: DbiFlags*) = new Dbi[T](this, txn, name, comparator, nativeCb, proxy, flags)

  /**
   * Return statistics about this environment.
   *
   * @return an immutable statistics object.
   */
  def stat: Stat = {
    if (closed) throw new Env.AlreadyClosedException
    val stat = new Library.MDB_stat(RUNTIME)
    checkRc(LIB.mdb_env_stat(ptr, stat))
    new Stat(stat.f0_ms_psize.intValue, stat.f1_ms_depth.intValue, stat.f2_ms_branch_pages.longValue, stat.f3_ms_leaf_pages.longValue, stat.f4_ms_overflow_pages.longValue, stat.f5_ms_entries.longValue)
  }

  /**
   * Flushes the data buffers to disk.
   *
   * @param force force a synchronous flush (otherwise if the environment has the MDB_NOSYNC flag
   *              set the flushes will be omitted, and with MDB_MAPASYNC they will be asynchronous)
   */
  def sync(force: Boolean): Unit = {
    if (closed) throw new Env.AlreadyClosedException
    val f = if (force) 1
    else 0
    checkRc(LIB.mdb_env_sync(ptr, f))
  }

  /**
   * Obtain a transaction with the requested parent and flags.
   *
   * @param parent parent transaction (may be null if no parent)
   * @param flags  applicable flags (eg for a reusable, read-only transaction)
   * @return a transaction (never null)
   */
  def txn(parent: Txn[T], flags: TxnFlags*): Txn[T] = {
    if (closed) throw new Env.AlreadyClosedException
    new Txn[T](this, parent, proxy, flags)
  }

  /**
   * Obtain a read-only transaction.
   *
   * @return a read-only transaction
   */
  def txnRead: Txn[T] = txn(null, MDB_RDONLY_TXN)

  /**
   * Obtain a read-write transaction.
   *
   * @return a read-write transaction
   */
  def txnWrite: Txn[T] = txn(null)

  private[torch] def pointer = ptr

  private[torch] def checkNotClosed(): Unit = {
    if (closed) throw new Env.AlreadyClosedException
  }

  private def validateDirectoryEmpty(path: File): Unit = {
    if (!path.exists) throw new Env.InvalidCopyDestination("Path does not exist")
    if (!path.isDirectory) throw new Env.InvalidCopyDestination("Path must be a directory")
    val files = path.list
    if (files != null && files.length > 0) throw new Env.InvalidCopyDestination("Path must contain no files")
  }

  private def validatePath(path: File): Unit = {
    if (noSubDir) {
      if (path.exists) throw new Env.InvalidCopyDestination("Path must not exist for MDB_NOSUBDIR")
      return
    }
    validateDirectoryEmpty(path)
  }

  /**
   * Check for stale entries in the reader lock table.
   *
   * @return 0 on success, non-zero on failure
   */
  def readerCheck: Int = {
    val resultPtr = new IntByReference
    checkRc(LIB.mdb_reader_check(ptr, resultPtr))
    resultPtr.intValue
  }
}