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

import com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES

import java.lang.Long.MAX_VALUE
import java.lang.System.getProperty
import java.nio.ByteBuffer.allocateDirect
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Collections.nCopies
import java.util.concurrent.TimeUnit.SECONDS
import java.util.stream.Collectors.toList
import java.util.stream.IntStream.range
import org.hamcrest.CoreMatchers.is
import org.hamcrest.CoreMatchers.not
import org.hamcrest.CoreMatchers.notNullValue
import org.hamcrest.CoreMatchers.nullValue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.hasSize
import org.hamcrest.collection.IsEmptyCollection.empty
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import torch.lmdb.proxy.ByteArrayProxy.PROXY_BA
import torch.lmdb.proxy.ByteBufferProxy.PROXY_OPTIMAL
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.flags.DbiFlags.MDB_DUPSORT
import torch.lmdb.flags.DbiFlags.MDB_INTEGERKEY
import torch.lmdb.flags.DbiFlags.MDB_REVERSEKEY
import torch.lmdb.db.Env.create
import torch.lmdb.flags.EnvFlags.MDB_NOSUBDIR
import torch.lmdb.enums.GetOp.MDB_SET_KEY
import torch.lmdb.db.KeyRange.atMost
import torch.lmdb.flags.PutFlags.MDB_NODUPDATA
import torch.lmdb.flags.PutFlags.MDB_NOOVERWRITE
import lmdbsuite.TestUtils.DB_1
import lmdbsuite.TestUtils.ba
import lmdbsuite.TestUtils.bb

import scala.jdk.CollectionConverters.*
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util
import java.util.Comparator
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiConsumer
import org.agrona.concurrent.UnsafeBuffer
import org.hamcrest.Matchers
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import torch.lmdb.db.CursorIterable.KeyVal
import torch.lmdb.db.Dbi.DbFullException
import torch.lmdb.db.{Dbi, Env, KeyRange, Stat, Txn}
import torch.lmdb.db.Env.AlreadyClosedException
import torch.lmdb.db.Env.MapFullException
import torch.lmdb.exceptions.LmdbNativeException
import torch.lmdb.exceptions.LmdbNativeException.ConstantDerivedException
import torch.lmdb.flags.DbiFlags

/** Test {@link Dbi}. */
final class DbiTest {
  @Rule final def tmpDef = new TemporaryFolder
  private var tmp = tmpDef
  private var env: Env[ByteBuffer] = null

  @After def after(): Unit = {
    env.close()
  }

  @Before
  @throws[IOException]
  def before(): Unit = {
    val path = new File("./tmp") // tmp.newFile
    env = create.setMapSize(MEBIBYTES.toBytes(64)).setMaxReaders(2).setMaxDbs(2).open(path, MDB_NOSUBDIR)
  }

  @Test(expected = classOf[LmdbNativeException.ConstantDerivedException]) 
  def close(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    db.put(bb(1), bb(42))
    db.close()
    db.put(bb(2), bb(42)) // error
  }

  @Test def customComparator(): Unit = {
    val reverseOrder: Comparator[ByteBuffer] = (o1: ByteBuffer, o2: ByteBuffer) => {
      def foo(o1: ByteBuffer, o2: ByteBuffer): Int = {
        val lexical = PROXY_OPTIMAL.getComparator().compare(o1, o2)
        if (lexical == 0) return 0
        lexical * -1
      }

      foo(o1, o2)
    }
    val db = env.openDbi(DB_1, reverseOrder, true, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        assertThat(db.put(txn, bb(2), bb(3)), is(true))
        assertThat(db.put(txn, bb(4), bb(6)), is(true))
        assertThat(db.put(txn, bb(6), bb(7)), is(true))
        assertThat(db.put(txn, bb(8), bb(7)), is(true))
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    try {
      val txn = env.txnRead
      val ci = db.iterate(txn, atMost(bb(4)))
      try {
        val iter = ci.iterator
        assertThat(iter.next.key.getInt, is(8))
        assertThat(iter.next.key.getInt, is(6))
        assertThat(iter.next.key.getInt, is(4))
      } finally {
        if (txn != null) txn.close()
        if (ci != null) ci.close()
      }
    }
  }

  @Test(expected = classOf[Dbi.DbFullException]) def dbOpenMaxDatabases(): Unit = {
    env.openDbi("db1 is OK", MDB_CREATE)
    env.openDbi("db2 is OK", MDB_CREATE)
    env.openDbi("db3 fails", MDB_CREATE)
  }

  @Test def dbiWithComparatorThreadSafety(): Unit = {
    val flags = Array[DbiFlags](MDB_CREATE, MDB_INTEGERKEY)
    val c = PROXY_OPTIMAL.getComparator(flags*)
    val db = env.openDbi(DB_1, c, true, flags*)
    
    val keys = range(0, 1_000).boxed.collect(toList)
    val pool = Executors.newCachedThreadPool
    val proceed = new AtomicBoolean(true)
    val reader = pool.submit(new Runnable {
      override def run(): Unit = {
        while (proceed.get) try {
          val txn = env.txnRead
          try db.get(txn, bb(50))
          finally if (txn != null) txn.close()
        }
      }}
      )
//    val reader = pool.submit(() => {
//      while (proceed.get) try {
//        val txn = env.txnRead
//        try db.get(txn, bb(50))
//        finally if (txn != null) txn.close()
//      }
//    })
//    import scala.collection.JavaConversions._
    for (key <- keys.asScala) {
      try {
        val txn = env.txnWrite
        try {
          db.put(txn, bb(key), bb(3))
          txn.commit()
        } finally if (txn != null) txn.close()
      }
    }
    try {
      val txn = env.txnRead
      val ci = db.iterate(txn)
      try {
        val iter = ci.iterator
        val result = new util.ArrayList[Integer]
        while (iter.hasNext) result.add(iter.next.key.getInt)
        assertThat(result, Matchers.contains(keys.toArray(new Array[Integer](0))))
      } finally {
        if (txn != null) txn.close()
        if (ci != null) ci.close()
      }
    }
    proceed.set(false)
    try {
      reader.get(1, SECONDS)
      pool.shutdown()
      pool.awaitTermination(1, SECONDS)
    } catch {
      case e@(_: ExecutionException | _: InterruptedException | _: TimeoutException) =>
        throw new IllegalStateException(e)
    }
  }

  @Test def drop(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        db.put(txn, bb(1), bb(42))
        db.put(txn, bb(2), bb(42))
        assertThat(db.get(txn, bb(1)), not(nullValue))
        assertThat(db.get(txn, bb(2)), not(nullValue))
        db.drop(txn)
        assertThat(db.get(txn, bb(1)), is(nullValue)) // data gone
        assertThat(db.get(txn, bb(2)), is(nullValue))
        db.put(txn, bb(1), bb(42)) // ensure DB still works
        db.put(txn, bb(2), bb(42))
        assertThat(db.get(txn, bb(1)), not(nullValue))
        assertThat(db.get(txn, bb(2)), not(nullValue))
      } finally if (txn != null) txn.close()
    }
  }

  @Test def dropAndDelete(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    val nameDb = env.openDbi(null.asInstanceOf[Array[Byte]])
    val dbNameBytes = DB_1.getBytes(UTF_8)
    val dbNameBuffer = allocateDirect(dbNameBytes.length)
    dbNameBuffer.put(dbNameBytes).flip
    try {
      val txn = env.txnWrite
      try {
        assertThat(nameDb.get(txn, dbNameBuffer), not(nullValue))
        db.drop(txn, true)
        assertThat(nameDb.get(txn, dbNameBuffer), is(nullValue))
        txn.commit()
      } finally if (txn != null) txn.close()
    }
  }

  @Test def dropAndDeleteAnonymousDb(): Unit = {
    env.openDbi(DB_1, MDB_CREATE)
    val nameDb = env.openDbi(null.asInstanceOf[Array[Byte]])
    val dbNameBytes = DB_1.getBytes(UTF_8)
    val dbNameBuffer = allocateDirect(dbNameBytes.length)
    dbNameBuffer.put(dbNameBytes).flip
    try {
      val txn = env.txnWrite
      try {
        assertThat(nameDb.get(txn, dbNameBuffer), not(nullValue))
        nameDb.drop(txn, true)
        assertThat(nameDb.get(txn, dbNameBuffer), is(nullValue))
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    nameDb.close() // explicit close after drop is OK
  }

  @Test def getName(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    assertThat(db.getName, is(DB_1.getBytes(UTF_8)))
  }

  @Test def getNamesWhenDbisPresent(): Unit = {
    val dbHello = Array[Byte]('h', 'e', 'l', 'l', 'o')
    val dbWorld = Array[Byte]('w', 'o', 'r', 'l', 'd')
    env.openDbi(dbHello, MDB_CREATE)
    env.openDbi(dbWorld, MDB_CREATE)
    val dbiNames = env.getDbiNames
//    assertThat(dbiNames, hasSize(2))
    assertThat(dbiNames(0), is(dbHello))
    assertThat(dbiNames(1), is(dbWorld))
  }

  @Test def getNamesWhenEmpty(): Unit = {
    val dbiNames = env.getDbiNames
//    assertThat(dbiNames, empty)
  }

  @Test def listsFlags(): Unit = {
    val dbi = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT, MDB_REVERSEKEY)
    try {
      val txn = env.txnRead
      try {
        val flags = dbi.listFlags(txn)
        assertThat(flags, containsInAnyOrder(MDB_DUPSORT, MDB_REVERSEKEY))
      } finally if (txn != null) txn.close()
    }
  }

  @Test def putAbortGet(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        db.put(txn, bb(5), bb(5))
        txn.abort()
      } finally if (txn != null) txn.close()
    }
    try {
      val txn = env.txnWrite
      try assertNull(db.get(txn, bb(5)))
      finally if (txn != null) txn.close()
    }
  }

  @Test def putAndGetAndDeleteWithInternalTx(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    db.put(bb(5), bb(5))
    try {
      val txn = env.txnRead
      try {
        val found = db.get(txn, bb(5))
        assertNotNull(found)
        assertThat(txn.vals.getInt, is(5))
      } finally if (txn != null) txn.close()
    }
    assertThat(db.delete(bb(5)), is(true))
    assertThat(db.delete(bb(5)), is(false))
    try {
      val txn = env.txnRead
      try assertNull(db.get(txn, bb(5)))
      finally if (txn != null) txn.close()
    }
  }

  @Test def putCommitGet(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        db.put(txn, bb(5), bb(5))
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    try {
      val txn = env.txnWrite
      try {
        val found = db.get(txn, bb(5))
        assertNotNull(found)
        assertThat(txn.vals.getInt, is(5))
      } finally if (txn != null) txn.close()
    }
  }

  @Test
  @throws[IOException]
  def putCommitGetByteArray(): Unit = {
    val path = tmp.newFile
    try {
      val envBa = create(PROXY_BA).setMapSize(MEBIBYTES.toBytes(64)).setMaxReaders(1).setMaxDbs(2).open(path, MDB_NOSUBDIR)
      try {
        val db = envBa.openDbi(DB_1, MDB_CREATE)
        try {
          val txn = envBa.txnWrite
          try {
            db.put(txn, ba(5), ba(5))
            txn.commit()
          } finally if (txn != null) txn.close()
        }
        try {
          val txn = envBa.txnWrite
          try {
            val found = db.get(txn, ba(5))
            assertNotNull(found)
            assertThat(new UnsafeBuffer(txn.vals).getInt(0), is(5))
          } finally if (txn != null) txn.close()
        }
      } finally if (envBa != null) envBa.close()
    }
  }

  @Test def putDelete(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        db.put(txn, bb(5), bb(5))
        assertThat(db.delete(txn, bb(5)), is(true))
        assertNull(db.get(txn, bb(5)))
        txn.abort()
      } finally if (txn != null) txn.close()
    }
  }

  @Test def putDuplicateDelete(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      try {
        db.put(txn, bb(5), bb(5))
        db.put(txn, bb(5), bb(6))
        db.put(txn, bb(5), bb(7))
        assertThat(db.delete(txn, bb(5), Some(bb(6))), is(true))
        assertThat(db.delete(txn, bb(5), Some(bb(6))), is(false))
        assertThat(db.delete(txn, bb(5), Some(bb(5))), is(true))
        assertThat(db.delete(txn, bb(5), Some(bb(5))), is(false))
        try {
          val cursor = db.openCursor(txn)
          try {
            val key = bb(5)
            cursor.get(key, MDB_SET_KEY)
            assertThat(cursor.count, is(1L))
          } finally if (cursor != null) cursor.close()
        }
        txn.abort()
      } finally if (txn != null) txn.close()
    }
  }

  @Test def putReserve(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    val key = bb(5)
    try {
      val txn = env.txnWrite
      try {
        assertNull(db.get(txn, key))
        val `val` = db.reserve(txn, key, 32, MDB_NOOVERWRITE)
        `val`.putLong(MAX_VALUE)
        assertNotNull(db.get(txn, key))
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    try {
      val txn = env.txnWrite
      try {
        val `val` = db.get(txn, key).get
        assertThat(`val`.capacity, is(32))
        assertThat(`val`.getLong, is(MAX_VALUE))
        assertThat(`val`.getLong(8), is(0L))
      } finally if (txn != null) txn.close()
    }
  }

  @Test def putZeroByteValueForNonMdbDupSortDatabase(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        val `val` = allocateDirect(0)
        db.put(txn, bb(5), `val`)
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    try {
      val txn = env.txnRead
      try {
        val found = db.get(txn, bb(5))
        assertNotNull(found)
        assertThat(txn.vals.capacity, is(0))
      } finally if (txn != null) txn.close()
    }
  }

  @Test def returnValueForNoDupData(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT)
    try {
      val txn = env.txnWrite
      try {
        // ok
        assertThat(db.put(txn, bb(5), bb(6), MDB_NODUPDATA), is(true))
        assertThat(db.put(txn, bb(5), bb(7), MDB_NODUPDATA), is(true))
        assertThat(db.put(txn, bb(5), bb(6), MDB_NODUPDATA), is(false))
      } finally if (txn != null) txn.close()
    }
  }

  @Test def returnValueForNoOverwrite(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        // ok
        assertThat(db.put(txn, bb(5), bb(6), MDB_NOOVERWRITE), is(true))
        // fails, but gets exist val
        assertThat(db.put(txn, bb(5), bb(8), MDB_NOOVERWRITE), is(false))
        assertThat(txn.vals.getInt(0), is(6))
      } finally if (txn != null) txn.close()
    }
  }

  @Test def stats(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    db.put(bb(1), bb(42))
    db.put(bb(2), bb(42))
    db.put(bb(3), bb(42))
    var stat: Stat = null
    try {
      val txn = env.txnRead
      try stat = db.stat(txn)
      finally if (txn != null) txn.close()
    }
    assertThat(stat, is(notNullValue))
    assertThat(stat.branchPages, is(0L))
    assertThat(stat.depth, is(1))
    assertThat(stat.entries, is(3L))
    assertThat(stat.leafPages, is(1L))
    assertThat(stat.overflowPages, is(0L))
    assertThat(stat.pageSize % 4_096, is(0))
  }

  @Test(expected = classOf[Env.MapFullException]) def testMapFullException(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    try {
      val txn = env.txnWrite
      try {
        var v: ByteBuffer = null
        try v = allocateDirect(1_024 * 1_024 * 1_024)
        catch {
          case e: OutOfMemoryError =>

            // Travis CI OS X build cannot allocate this much memory, so assume OK
            throw new Env.MapFullException
        }
        db.put(txn, bb(1), v)
      } finally if (txn != null) txn.close()
    }
  }

  @Test def testParallelWritesStress(): Unit = {
    if (getProperty("os.name").startsWith("Windows")) return // Windows VMs run this test too slowly
    val db = env.openDbi(DB_1, MDB_CREATE)
    // Travis CI has 1.5 cores for legacy builds
    nCopies(2, null).parallelStream.forEach((ignored: AnyRef) => {
      for (i <- 0 until 15_000) {
        db.put(bb(i), bb(i))
      }
    })
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsOpenCall(): Unit = {
    env.close()
    env.openDbi(DB_1, MDB_CREATE)
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsCloseCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.close())
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsGetCall(): Unit = {
    doEnvClosedTest((db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => {
      val valBuf = db.get(txn, bb(1))
      assertThat(valBuf.get.getInt, is(10))
    }, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.get(txn, bb(2)))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsPutCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.put(bb(5), bb(50)))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsPutWithTxnCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => {
      db.put(txn, bb(5), bb(50))
    })
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsIterateCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.iterate(txn, KeyRange.all))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsDropCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.drop(txn))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsDropAndDeleteCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.drop(txn, true))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsOpenCursorCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.openCursor(txn))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsReserveCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.reserve(txn, bb(1), 32, MDB_NOOVERWRITE))
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def closedEnvRejectsStatCall(): Unit = {
    doEnvClosedTest(null, (db: Dbi[ByteBuffer], txn: Txn[ByteBuffer]) => db.stat(txn))
  }

  private def doEnvClosedTest(workBeforeEnvClosed: BiConsumer[Dbi[ByteBuffer], Txn[ByteBuffer]], workAfterEnvClose: BiConsumer[Dbi[ByteBuffer], Txn[ByteBuffer]]): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    db.put(bb(1), bb(10))
    db.put(bb(2), bb(20))
    db.put(bb(2), bb(30))
    db.put(bb(4), bb(40))
    try {
      val txn = env.txnWrite
      try {
        if (workBeforeEnvClosed != null) workBeforeEnvClosed.accept(db, txn)
        env.close()
        if (workAfterEnvClose != null) workAfterEnvClose.accept(db, txn)
      } finally if (txn != null) txn.close()
    }
  }
}