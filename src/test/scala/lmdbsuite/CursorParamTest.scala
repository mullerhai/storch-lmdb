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
package lmdbsuite

import com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES

import java.lang.Long.BYTES
import java.nio.charset.StandardCharsets.UTF_8
import org.hamcrest.CoreMatchers.is
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.collection.IsEmptyCollection.empty
import torch.lmdb.DirectBufferProxy.PROXY_DB
import torch.lmdb.proxy.ByteArrayProxy.PROXY_BA
import torch.lmdb.proxy.ByteBufProxy.PROXY_NETTY
import torch.lmdb.proxy.ByteBufferProxy.PROXY_OPTIMAL
import torch.lmdb.proxy.ByteBufferProxy.PROXY_SAFE
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.flags.DbiFlags.MDB_DUPSORT
//import torch.lmdb.proxy.DirectBufferProxy.PROXY_DB
import torch.lmdb.db.Env.create
import torch.lmdb.flags.EnvFlags.MDB_NOSUBDIR
import torch.lmdb.enums.GetOp.MDB_SET_KEY
import torch.lmdb.enums.GetOp.MDB_SET_RANGE
import torch.lmdb.flags.PutFlags.MDB_NOOVERWRITE
import torch.lmdb.enums.SeekOp.MDB_FIRST
import torch.lmdb.enums.SeekOp.MDB_LAST
import torch.lmdb.enums.SeekOp.MDB_NEXT
import torch.lmdb.enums.SeekOp.MDB_PREV
import lmdbsuite.TestUtils.DB_1
import lmdbsuite.TestUtils.POSIX_MODE
import lmdbsuite.TestUtils.bb
import lmdbsuite.TestUtils.mdb
import lmdbsuite.TestUtils.nb
import io.netty.buffer.ByteBuf

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import org.agrona.DirectBuffer
import org.agrona.MutableDirectBuffer
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameter
import org.junit.runners.Parameterized.Parameters
import torch.lmdb.exceptions.LmdbException
import torch.lmdb.proxy.BufferProxy

/** Test {@link Cursor} with different buffer implementations. */
@RunWith(classOf[Parameterized]) object CursorParamTest {
  @Parameters(name = "{index}: buffer adapter: {0}") def data: Array[AnyRef] = {
    val bb1 = new CursorParamTest.ByteBufferRunner(PROXY_OPTIMAL)
    val bb2 = new CursorParamTest.ByteBufferRunner(PROXY_SAFE)
    val ba = new CursorParamTest.ByteArrayRunner(PROXY_BA)
    val db = new CursorParamTest.DirectBufferRunner
    val netty = new CursorParamTest.NettyBufferRunner
    Array[AnyRef](bb1, bb2, ba, db, netty)
  }

  /**
   * Abstract implementation of {@link BufferRunner}.
   *
   * @param < T> buffer type
   */
  abstract private class AbstractBufferRunner[T] protected(val proxy: BufferProxy[T]) extends CursorParamTest.BufferRunner[T] {
    override final def execute(tmp: TemporaryFolder): Unit = {
      try {
        val env = envDef(tmp)
        try {
//          assertThat(env.getDbiNames, empty)
          val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
          assertThat(env.getDbiNames(0), is(DB_1.getBytes(UTF_8)))
          try {
            val txn = env.txnWrite
            val c = db.openCursor(txn)
            try {
              // populate data
              c.put(set(1), set(2), MDB_NOOVERWRITE)
              c.put(set(3), set(4))
              c.put(set(5), set(6))
              // we cannot set the value for ByteArrayProxy
              // but the key is still valid.
              val valForKey7 = c.reserve(set(7), BYTES)
              set(valForKey7, 8)
              // check MDB_SET operations
              val key3 = set(3)
              assertThat(c.get(key3, MDB_SET_KEY), is(true))
              assertThat(get(c.key), is(3))
              assertThat(get(c.vals), is(4))
              val key6 = set(6)
              assertThat(c.get(key6, MDB_SET_RANGE), is(true))
              assertThat(get(c.key), is(7))
              if (!this.isInstanceOf[CursorParamTest.ByteArrayRunner]) assertThat(get(c.vals), is(8))
              val key999 = set(999)
              assertThat(c.get(key999, MDB_SET_KEY), is(false))
              // check MDB navigation operations
              assertThat(c.seek(MDB_LAST), is(true))
              val mdb1 = get(c.key)
              val mdb2 = get(c.vals)
              assertThat(c.seek(MDB_PREV), is(true))
              val mdb3 = get(c.key)
              val mdb4 = get(c.vals)
              assertThat(c.seek(MDB_NEXT), is(true))
              val mdb5 = get(c.key)
              val mdb6 = get(c.vals)
              assertThat(c.seek(MDB_FIRST), is(true))
              val mdb7 = get(c.key)
              val mdb8 = get(c.vals)
              // assert afterwards to ensure memory address from LMDB
              // are valid within same txn and across cursor movement
              // MDB_LAST
              assertThat(mdb1, is(7))
              if (!this.isInstanceOf[CursorParamTest.ByteArrayRunner]) assertThat(mdb2, is(8))
              // MDB_PREV
              assertThat(mdb3, is(5))
              assertThat(mdb4, is(6))
              // MDB_NEXT
              assertThat(mdb5, is(7))
              if (!this.isInstanceOf[CursorParamTest.ByteArrayRunner]) assertThat(mdb6, is(8))
              // MDB_FIRST
              assertThat(mdb7, is(1))
              assertThat(mdb8, is(2))
            } finally {
              if (txn != null) txn.close()
              if (c != null) c.close()
            }
          }
        } finally if (env != null) env.close()
      }
    }

    private def envDef(tmp: TemporaryFolder) = try {
      val path = tmp.newFile
      create(proxy).setMapSize(KIBIBYTES.toBytes(1_024)).setMaxReaders(1).setMaxDbs(1).open(path, POSIX_MODE, MDB_NOSUBDIR)
    } catch {
      case e: IOException =>
        throw new LmdbException("IO failure", e)
    }
  }

  /** {@link BufferRunner} for Java byte buffers. */
  private class ByteArrayRunner(proxy: BufferProxy[Array[Byte]]) extends CursorParamTest.AbstractBufferRunner[Array[Byte]](proxy) {
    override def get(buff: Array[Byte]): Int = (buff(0) & 0xFF) << 24 | (buff(1) & 0xFF) << 16 | (buff(2) & 0xFF) << 8 | (buff(3) & 0xFF)

    override def set(`val`: Int): Array[Byte] = {
      val buff = new Array[Byte](4)
      buff(0) = (`val` >>> 24).toByte
      buff(1) = (`val` >>> 16).toByte
      buff(2) = (`val` >>> 8).toByte
      buff(3) = `val`.toByte
      buff
    }

    override def set(buff: Array[Byte], `val`: Int): Unit = {
      buff(0) = (`val` >>> 24).toByte
      buff(1) = (`val` >>> 16).toByte
      buff(2) = (`val` >>> 8).toByte
      buff(3) = `val`.toByte
    }
  }

  /** {@link BufferRunner} for Java byte buffers. */
  private class ByteBufferRunner (proxy: BufferProxy[ByteBuffer]) extends CursorParamTest.AbstractBufferRunner[ByteBuffer](proxy) {
    override def get(buff: ByteBuffer): Int = buff.getInt(0)

    override def set(`val`: Int): ByteBuffer = bb(`val`)

    override def set(buff: ByteBuffer, `val`: Int): Unit = {
      buff.putInt(`val`)
    }
  }

  /** {@link BufferRunner} for Agrona direct buffer. */
  private class DirectBufferRunner extends CursorParamTest.AbstractBufferRunner[DirectBuffer](PROXY_DB) {
    override def get(buff: DirectBuffer): Int = buff.getInt(0)

    override def set(vals: Int): DirectBuffer = mdb(vals)

    override def set(buff: DirectBuffer, vals: Int): Unit = {
      buff.asInstanceOf[MutableDirectBuffer].putInt(0, vals)
    }
  }

  /** {@link BufferRunner} for Netty byte buf. */
  private class NettyBufferRunner extends CursorParamTest.AbstractBufferRunner[ByteBuf](PROXY_NETTY) {
    override def get(buff: ByteBuf): Int = buff.getInt(0)

    override def set(vals: Int): ByteBuf = nb(vals)

    override def set(buff: ByteBuf, vals: Int): Unit = {
      buff.setInt(0, vals)
    }
  }

  /**
   * Adapter to allow different buffers to be tested with this class.
   *
   * @param < T> buffer type
   */
  trait BufferRunner[T] {
    def execute(tmp: TemporaryFolder): Unit

    def set(vals: Int): T

    def set(buff: T, vals: Int): Unit

    def get(buff: T): Int
  }
}

@RunWith(classOf[Parameterized]) final class CursorParamTest {
  /** Injected by {@link # data ( )} with appropriate runner. */
  @Parameter var runner: CursorParamTest.BufferRunner[?] = null
  @Rule final def tmpDef = new TemporaryFolder
  val tmp = tmpDef

  @Test def execute(): Unit = {
    runner.execute(tmp)
  }
}