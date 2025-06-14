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

import java.io.File.createTempFile
import java.lang.System.getProperty
import java.lang.Thread.currentThread
import java.util.Objects.requireNonNull
import jnr.ffi.LibraryLoader.create
import jnr.ffi.Runtime.getRuntime
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import jnr.ffi.Pointer
import jnr.ffi.Struct
import jnr.ffi.annotations.Delegate
import jnr.ffi.annotations.In
import jnr.ffi.annotations.Out
import jnr.ffi.byref.IntByReference
import jnr.ffi.byref.NativeLongByReference
import jnr.ffi.byref.PointerByReference
import jnr.ffi.types.size_t

/**
 * JNR-FFI interface to LMDB.
 *
 * <p>For performance reasons pointers are used rather than structs.
 */
object Library {
  /**
   * Java system property name that can be set to the path of an existing directory into which the
   * LMDB system library will be extracted from the LmdbJava JAR. If unspecified the LMDB system
   * library is extracted to the <code>java.io.tmpdir</code>. Ignored if the LMDB system library is
   * not being extracted from the LmdbJava JAR (as would be the case if other system properties
   * defined in <code>TargetName</code> have been set).
   */
  val LMDB_EXTRACT_DIR_PROP = "lmdbjava.extract.dir"
  /** Indicates the directory where the LMDB system library will be extracted. */
  private[torch] val EXTRACT_DIR = getProperty(LMDB_EXTRACT_DIR_PROP, getProperty("java.io.tmpdir"))
  private[torch] var LIB: Library.Lmdb = null
  private[torch] var RUNTIME: Runtime = null

  private def extract(name: String) = {
    val suffix = name.substring(name.lastIndexOf('.'))
    val file: File = null
    try {
      val dir = new File(EXTRACT_DIR)
      if (!dir.exists || !dir.isDirectory) throw new IllegalStateException("Invalid extraction directory " + dir)
      file = createTempFile("lmdbjava-native-library-", suffix, dir)
      file.deleteOnExit()
      val cl = currentThread.getContextClassLoader
      try {
        val in = cl.getResourceAsStream(name)
        val out = Files.newOutputStream(file.toPath)
        try {
          requireNonNull(in, "Classpath resource not found")
          var bytes = 0
          val buffer = new Array[Byte](4_096)
          while (-1 != (bytes = in.read(buffer))) out.write(buffer, 0, bytes)
        } finally {
          if (in != null) in.close()
          if (out != null) out.close()
        }
      }
      file.getAbsolutePath
    } catch {
      case e: IOException =>
        throw new LmdbException("Failed to extract " + name, e)
    }
  }

  /** Structure to wrap a native <code>MDB_envinfo</code>. Not for external use. */
  final class MDB_envinfo private[torch](runtime: Runtime) extends Struct(runtime) {
    this.f0_me_mapaddr = new Struct#Pointer
    this.f1_me_mapsize = new Struct#size_t
    this.f2_me_last_pgno = new Struct#size_t
    this.f3_me_last_txnid = new Struct#size_t
    this.f4_me_maxreaders = new Struct#u_int32_t
    this.f5_me_numreaders = new Struct#u_int32_t
    final var f0_me_mapaddr: Struct#Pointer = null
    final var f1_me_mapsize: Struct#size_t = null
    final var f2_me_last_pgno: Struct#size_t = null
    final var f3_me_last_txnid: Struct#size_t = null
    final var f4_me_maxreaders: Struct#u_int32_t = null
    final var f5_me_numreaders: Struct#u_int32_t = null
  }

  /** Structure to wrap a native <code>MDB_stat</code>. Not for external use. */
  final class MDB_stat private[torch](runtime: Runtime) extends Struct(runtime) {
    this.f0_ms_psize = new Struct#u_int32_t
    this.f1_ms_depth = new Struct#u_int32_t
    this.f2_ms_branch_pages = new Struct#size_t
    this.f3_ms_leaf_pages = new Struct#size_t
    this.f4_ms_overflow_pages = new Struct#size_t
    this.f5_ms_entries = new Struct#size_t
    final var f0_ms_psize: Struct#u_int32_t = null
    final var f1_ms_depth: Struct#u_int32_t = null
    final var f2_ms_branch_pages: Struct#size_t = null
    final var f3_ms_leaf_pages: Struct#size_t = null
    final var f4_ms_overflow_pages: Struct#size_t = null
    final var f5_ms_entries: Struct#size_t = null
  }

  /** Custom comparator callback used by <code>mdb_set_compare</code>. */
  trait ComparatorCallback {
    @Delegate def compare(@In keyA: Pointer, @In keyB: Pointer): Int
  }

  /** JNR API for MDB-defined C functions. Not for external use. */
  trait Lmdb {
    def mdb_cursor_close(@In cursor: Pointer): Unit

    def mdb_cursor_count(@In cursor: Pointer, countp: NativeLongByReference): Int

    def mdb_cursor_del(@In cursor: Pointer, flags: Int): Int

    def mdb_cursor_get(@In cursor: Pointer, k: Pointer, @Out v: Pointer, cursorOp: Int): Int

    def mdb_cursor_open(@In txn: Pointer, @In dbi: Pointer, cursorPtr: PointerByReference): Int

    def mdb_cursor_put(@In cursor: Pointer, @In key: Pointer, @In data: Pointer, flags: Int): Int

    def mdb_cursor_renew(@In txn: Pointer, @In cursor: Pointer): Int

    def mdb_dbi_close(@In env: Pointer, @In dbi: Pointer): Unit

    def mdb_dbi_flags(@In txn: Pointer, @In dbi: Pointer, @Out flags: IntByReference): Int

    def mdb_dbi_open(@In txn: Pointer, @In name: Array[Byte], flags: Int, @In dbiPtr: Pointer): Int

    def mdb_del(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @In data: Pointer): Int

    def mdb_drop(@In txn: Pointer, @In dbi: Pointer, del: Int): Int

    def mdb_env_close(@In env: Pointer): Unit

    def mdb_env_copy2(@In env: Pointer, @In path: String, flags: Int): Int

    def mdb_env_create(envPtr: PointerByReference): Int

    def mdb_env_get_fd(@In env: Pointer, @In fd: Pointer): Int

    def mdb_env_get_flags(@In env: Pointer, flags: Int): Int

    def mdb_env_get_maxkeysize(@In env: Pointer): Int

    def mdb_env_get_maxreaders(@In env: Pointer, readers: Int): Int

    def mdb_env_get_path(@In env: Pointer, path: String): Int

    def mdb_env_info(@In env: Pointer, @Out info: Library.MDB_envinfo): Int

    def mdb_env_open(@In env: Pointer, @In path: String, flags: Int, mode: Int): Int

    def mdb_env_set_flags(@In env: Pointer, flags: Int, onoff: Int): Int

    def mdb_env_set_mapsize(@In env: Pointer, @size_t size: Long): Int

    def mdb_env_set_maxdbs(@In env: Pointer, dbs: Int): Int

    def mdb_env_set_maxreaders(@In env: Pointer, readers: Int): Int

    def mdb_env_stat(@In env: Pointer, @Out stat: Library.MDB_stat): Int

    def mdb_env_sync(@In env: Pointer, f: Int): Int

    def mdb_get(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @Out data: Pointer): Int

    def mdb_put(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @In data: Pointer, flags: Int): Int

    def mdb_reader_check(@In env: Pointer, @Out dead: IntByReference): Int

    def mdb_set_compare(@In txn: Pointer, @In dbi: Pointer, cb: Library.ComparatorCallback): Int

    def mdb_stat(@In txn: Pointer, @In dbi: Pointer, @Out stat: Library.MDB_stat): Int

    def mdb_strerror(rc: Int): String

    def mdb_txn_abort(@In txn: Pointer): Unit

    def mdb_txn_begin(@In env: Pointer, @In parentTx: Pointer, flags: Int, txPtr: Pointer): Int

    def mdb_txn_commit(@In txn: Pointer): Int

    def mdb_txn_env(@In txn: Pointer): Pointer

    def mdb_txn_id(@In txn: Pointer): Long

    def mdb_txn_renew(@In txn: Pointer): Int

    def mdb_txn_reset(@In txn: Pointer): Unit

    def mdb_version(major: IntByReference, minor: IntByReference, patch: IntByReference): Pointer
  }

  try 
  var libToLoad: String = null
  if (TargetName.IS_EXTERNAL) libToLoad = TargetName.RESOLVED_FILENAME
  else libToLoad = extract(TargetName.RESOLVED_FILENAME)
  LIB = create(classOf[Library.Lmdb]).load(libToLoad)
  RUNTIME = getRuntime(LIB)
}

final class Library private {
}