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
import java.lang.Long.MIN_VALUE
import java.nio.ByteBuffer.allocateDirect
import org.hamcrest.CoreMatchers.is
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import torch.lmdb.proxy.ByteBufferProxy.PROXY_OPTIMAL
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.flags.DbiFlags.MDB_DUPFIXED
import torch.lmdb.flags.DbiFlags.MDB_DUPSORT
import torch.lmdb.db.Env.create
import torch.lmdb.flags.EnvFlags.MDB_NOSUBDIR
import torch.lmdb.flags.PutFlags.MDB_APPENDDUP
import torch.lmdb.flags.PutFlags.MDB_MULTIPLE
import torch.lmdb.flags.PutFlags.MDB_NODUPDATA
import torch.lmdb.flags.PutFlags.MDB_NOOVERWRITE
import torch.lmdb.enums.SeekOp.MDB_FIRST
import torch.lmdb.enums.SeekOp.MDB_GET_BOTH
import torch.lmdb.enums.SeekOp.MDB_LAST
import torch.lmdb.enums.SeekOp.MDB_NEXT
import lmdbsuite.TestUtils.DB_1
import lmdbsuite.TestUtils.POSIX_MODE
import lmdbsuite.TestUtils.bb

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util.function.Consumer
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import torch.lmdb.db.{Cursor, Env, Txn}
import torch.lmdb.db.Cursor.ClosedException
import torch.lmdb.db.Txn.NotReadyException
import torch.lmdb.db.Txn.ReadOnlyRequiredException
import torch.lmdb.exceptions.LmdbException
import torch.lmdb.flags.PutFlags

/** Test {@link Cursor}. */
final class CursorTest {
  @Rule final def tmpDef = new TemporaryFolder
  val tmp = tmpDef
  private var env: Env[ByteBuffer] = null

  @After def after(): Unit = {
    env.close()
  }

  @Before
  @throws[IOException]
  def before(): Unit = {
    try {
      val path = tmp.newFile
      env = create(PROXY_OPTIMAL).setMapSize(KIBIBYTES.toBytes(1_024)).setMaxReaders(1).setMaxDbs(1).open(path, POSIX_MODE, MDB_NOSUBDIR)
    } catch {
      case e: IOException =>
        throw new LmdbException("IO failure", e)
    }
  }

  @Test(expected = classOf[Cursor.ClosedException]) def closedCursorRejectsSubsequentGets(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        val c = db.openCursor(txn)
        c.close()
        c.seek(MDB_FIRST)
      } finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsSeekFirstCall(): Unit = {
    doEnvClosedTest(null, (c: Cursor[ByteBuffer]) => c.seek(MDB_FIRST))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsSeekLastCall(): Unit = {
    doEnvClosedTest(null, (c: Cursor[ByteBuffer]) => c.seek(MDB_LAST))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsSeekNextCall(): Unit = {
    doEnvClosedTest(null, (c: Cursor[ByteBuffer]) => c.seek(MDB_NEXT))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsCloseCall(): Unit = {
    doEnvClosedTest(null, (c: Cursor[ByteBuffer]) => c.close())
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsFirstCall(): Unit = {
    doEnvClosedTest(null, (c: Cursor[ByteBuffer]) => c.first)
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsLastCall(): Unit = {
    doEnvClosedTest(null, (c: Cursor[ByteBuffer]) => c.last)
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsPrevCall(): Unit = {
    doEnvClosedTest((c: Cursor[ByteBuffer]) => {
      c.first
      assertThat(c.key.getInt, is(1))
      assertThat(c.vals.getInt, is(10))
      c.next
    }, (c: Cursor[ByteBuffer]) => c.prev)
  }

//  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsDeleteCall(): Unit = {
//    doEnvClosedTest((c: Cursor[ByteBuffer]) => {
//      c.first
//      assertThat(c.key.getInt, is(1))
//      assertThat(c.vals.getInt, is(10))
//    }, (c: Cursor[ByteBuffer], f: PutFlags) => c.delete(f*))
//  }

  @Test def count(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      val c = db.openCursor(txn)
      try {
        c.put(bb(1), bb(2), MDB_APPENDDUP)
        assertThat(c.count, is(1L))
        c.put(bb(1), bb(4), MDB_APPENDDUP)
        c.put(bb(1), bb(6), MDB_APPENDDUP)
        assertThat(c.count, is(3L))
        c.put(bb(2), bb(1), MDB_APPENDDUP)
        c.put(bb(2), bb(2), MDB_APPENDDUP)
        assertThat(c.count, is(2L))
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test(expected = classOf[Txn.NotReadyException]) def cursorCannotCloseIfTransactionCommitted(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      try try {
        val c = db.openCursor(txn)
        try {
          c.put(bb(1), bb(2), MDB_APPENDDUP)
          assertThat(c.count, is(1L))
          c.put(bb(1), bb(4), MDB_APPENDDUP)
          assertThat(c.count, is(2L))
          txn.commit()
        } finally if (c != null) c.close()
      }
      finally if (txn != null) txn.close()
    }
  }

  @Test def cursorFirstLastNextPrev(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      val c = db.openCursor(txn)
      try {
        c.put(bb(1), bb(2), MDB_NOOVERWRITE)
        c.put(bb(3), bb(4))
        c.put(bb(5), bb(6))
        c.put(bb(7), bb(8))
        assertThat(c.first, is(true))
        assertThat(c.key.getInt(0), is(1))
        assertThat(c.vals.getInt(0), is(2))
        assertThat(c.last, is(true))
        assertThat(c.key.getInt(0), is(7))
        assertThat(c.vals.getInt(0), is(8))
        assertThat(c.prev, is(true))
        assertThat(c.key.getInt(0), is(5))
        assertThat(c.vals.getInt(0), is(6))
        assertThat(c.first, is(true))
        assertThat(c.next, is(true))
        assertThat(c.key.getInt(0), is(3))
        assertThat(c.vals.getInt(0), is(4))
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def delete(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      val c = db.openCursor(txn)
      try {
        c.put(bb(1), bb(2), MDB_NOOVERWRITE)
        c.put(bb(3), bb(4))
        assertThat(c.seek(MDB_FIRST), is(true))
        assertThat(c.key.getInt, is(1))
        assertThat(c.vals.getInt, is(2))
        c.delete()
        assertThat(c.seek(MDB_FIRST), is(true))
        assertThat(c.key.getInt, is(3))
        assertThat(c.vals.getInt, is(4))
        c.delete()
        assertThat(c.seek(MDB_FIRST), is(false))
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def getKeyVal(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      val c = db.openCursor(txn)
      try {
        c.put(bb(1), bb(2), MDB_APPENDDUP)
        c.put(bb(1), bb(4), MDB_APPENDDUP)
        c.put(bb(1), bb(6), MDB_APPENDDUP)
        c.put(bb(2), bb(1), MDB_APPENDDUP)
        c.put(bb(2), bb(2), MDB_APPENDDUP)
        c.put(bb(2), bb(3), MDB_APPENDDUP)
        c.put(bb(2), bb(4), MDB_APPENDDUP)
        assertThat(c.get(bb(1), bb(2), MDB_GET_BOTH), is(true))
        assertThat(c.count, is(3L))
        assertThat(c.get(bb(1), bb(3), MDB_GET_BOTH), is(false))
        assertThat(c.get(bb(2), bb(1), MDB_GET_BOTH), is(true))
        assertThat(c.count, is(4L))
        assertThat(c.get(bb(2), bb(0), MDB_GET_BOTH), is(false))
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def putMultiple(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT, MDB_DUPFIXED)
    val elemCount = 20
    val values = allocateDirect(Integer.BYTES * elemCount)
    for (i <- 1 to elemCount) {
      values.putInt(i)
    }
    values.flip
    val key = 100
    val k = bb(key)
    try {
      val txn = env.txnWrite
      val c = db.openCursor(txn)
      try {
        c.putMultiple(k, values, elemCount, MDB_MULTIPLE)
        assertThat(c.count, is(elemCount.toLong))
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test(expected = classOf[IllegalArgumentException]) def putMultipleWithoutMdbMultipleFlag(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      val c = db.openCursor(txn)
      try c.putMultiple(bb(100), bb(1), 1)
      finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def renewTxRo(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    var c: Cursor[ByteBuffer] = null
    try {
      val txn = env.txnRead
      try {
        c = db.openCursor(txn)
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    try {
      val txn = env.txnRead
      try {
        c.renew(txn)
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    c.close()
  }

  @Test(expected = classOf[Txn.ReadOnlyRequiredException]) def renewTxRw(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        assertThat(txn.isReadOnly, is(false))
        try {
          val c = db.openCursor(txn)
          try c.renew(txn)
          finally if (c != null) c.close()
        }
      } finally if (txn != null) txn.close()
    }
  }

  @Test def repeatedCloseCausesNotError(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      try {
        val c = db.openCursor(txn)
        c.close()
        c.close()
      } finally if (txn != null) txn.close()
    }
  }

  @Test def reserve(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    val key = bb(5)
    try {
      val txn = env.txnWrite
      try {
        assertNull(db.get(txn, key))
        try {
          val c = db.openCursor(txn)
          try {
            val `val` = c.reserve(key, BYTES * 2)
            assertNotNull(db.get(txn, key))
            `val`.putLong(MIN_VALUE).flip
          } finally if (c != null) c.close()
        }
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    try {
      val txn = env.txnWrite
      try {
        val `val` = db.get(txn, key).get
        assertThat(`val`.capacity, is(BYTES * 2))
        assertThat(`val`.getLong, is(MIN_VALUE))
      } finally if (txn != null) txn.close()
    }
  }

  @Test def returnValueForNoDupData(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      val c = db.openCursor(txn)
      try {
        // ok
        assertThat(c.put(bb(5), bb(6), MDB_NODUPDATA), is(true))
        assertThat(c.put(bb(5), bb(7), MDB_NODUPDATA), is(true))
        assertThat(c.put(bb(5), bb(6), MDB_NODUPDATA), is(false))
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def returnValueForNoOverwrite(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      val c = db.openCursor(txn)
      try {
        // ok
        assertThat(c.put(bb(5), bb(6), MDB_NOOVERWRITE), is(true))
        // fails, but gets exist val
        assertThat(c.put(bb(5), bb(8), MDB_NOOVERWRITE), is(false))
        assertThat(c.vals.getInt(0), is(6))
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def testCursorByteBufferDuplicate(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        try {
          val c = db.openCursor(txn)
          try {
            c.put(bb(1), bb(2))
            c.put(bb(3), bb(4))
          } finally if (c != null) c.close()
        }
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    try {
      val txn = env.txnRead
      try try {
        val c = db.openCursor(txn)
        try {
          c.first
          val key1 = c.key.duplicate
          val val1 = c.vals.duplicate
          c.last
          val key2 = c.key.duplicate
          val val2 = c.vals.duplicate
          assertThat(key1.getInt(0), is(1))
          assertThat(val1.getInt(0), is(2))
          assertThat(key2.getInt(0), is(3))
          assertThat(val2.getInt(0), is(4))
        } finally if (c != null) c.close()
      }
      finally if (txn != null) txn.close()
    }
  }

  private def doEnvClosedTest(workBeforeEnvClosed: Consumer[Cursor[ByteBuffer]], workAfterEnvClose: Consumer[Cursor[ByteBuffer]]): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    db.put(bb(1), bb(10))
    db.put(bb(2), bb(20))
    db.put(bb(2), bb(30))
    db.put(bb(4), bb(40))
    try {
      val txn = env.txnWrite
      try try {
        val c = db.openCursor(txn)
        try {
          if (workBeforeEnvClosed != null) workBeforeEnvClosed.accept(c)
          env.close()
          if (workAfterEnvClose != null) workAfterEnvClose.accept(c)
        } finally if (c != null) c.close()
      }
      finally if (txn != null) txn.close()
    }
  }
}