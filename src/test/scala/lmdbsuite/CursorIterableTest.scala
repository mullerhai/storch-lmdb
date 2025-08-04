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

import java.util.Arrays.asList
import org.hamcrest.CoreMatchers.is
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasSize
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.db.Env.create
import torch.lmdb.flags.EnvFlags.MDB_NOSUBDIR
import torch.lmdb.db.KeyRange.all
import torch.lmdb.db.KeyRange.allBackward
import torch.lmdb.db.KeyRange.atLeast
import torch.lmdb.db.KeyRange.atLeastBackward
import torch.lmdb.db.KeyRange.atMost
import torch.lmdb.db.KeyRange.atMostBackward
import torch.lmdb.db.KeyRange.closed
import torch.lmdb.db.KeyRange.closedBackward
import torch.lmdb.db.KeyRange.closedOpen
import torch.lmdb.db.KeyRange.closedOpenBackward
import torch.lmdb.db.KeyRange.greaterThan
import torch.lmdb.db.KeyRange.greaterThanBackward
import torch.lmdb.db.KeyRange.lessThan
import torch.lmdb.db.KeyRange.lessThanBackward
import torch.lmdb.db.KeyRange.open
import torch.lmdb.db.KeyRange.openBackward
import torch.lmdb.db.KeyRange.openClosed
import torch.lmdb.db.KeyRange.openClosedBackward
import torch.lmdb.flags.PutFlags.MDB_NOOVERWRITE
import lmdbsuite.TestUtils.DB_1
import lmdbsuite.TestUtils.POSIX_MODE
import lmdbsuite.TestUtils.bb
import com.google.common.primitives.UnsignedBytes

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util
import java.util.Comparator
import java.util.NoSuchElementException
import org.hamcrest.Matchers
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import torch.lmdb.db.CursorIterable.KeyVal
import torch.lmdb.db.{CursorIterable, Dbi, Env, KeyRange}

/** Test {@link CursorIterable}. */
final class CursorIterableTest {
  @Rule final def tmpDef = new TemporaryFolder
  val tmp = tmpDef

  private var db: Dbi[ByteBuffer] = null
  private var env: Env[ByteBuffer] = null
  private var list: util.Deque[Integer] = null

  @After def after(): Unit = {
    env.close()
  }

  @Test def allBackwardTest(): Unit = {
    verify(allBackward, 8, 6, 4, 2)
  }

  @Test def allTest(): Unit = {
    verify(all, 2, 4, 6, 8)
  }

  @Test def atLeastBackwardTest(): Unit = {
    verify(atLeastBackward(bb(5)), 4, 2)
    verify(atLeastBackward(bb(6)), 6, 4, 2)
    verify(atLeastBackward(bb(9)), 8, 6, 4, 2)
  }

  @Test def atLeastTest(): Unit = {
    verify(atLeast(bb(5)), 6, 8)
    verify(atLeast(bb(6)), 6, 8)
  }

  @Test def atMostBackwardTest(): Unit = {
    verify(atMostBackward(bb(5)), 8, 6)
    verify(atMostBackward(bb(6)), 8, 6)
  }

  @Test def atMostTest(): Unit = {
    verify(atMost(bb(5)), 2, 4)
    verify(atMost(bb(6)), 2, 4, 6)
  }

  @Before
  @throws[IOException]
  def before(): Unit = {
    val path = tmp.newFile
    env = create.setMapSize(KIBIBYTES.toBytes(256)).setMaxReaders(1).setMaxDbs(1).open(path, POSIX_MODE, MDB_NOSUBDIR)
    db = env.openDbi(DB_1, MDB_CREATE)
    populateDatabase(db)
  }

  private def populateDatabase(dbi: Dbi[ByteBuffer]): Unit = {
    list = new util.LinkedList[Integer]
    list.addAll(asList(2, 3, 4, 5, 6, 7, 8, 9))
    try {
      val txn = env.txnWrite
      try {
        val c = dbi.openCursor(txn)
        c.put(bb(2), bb(3), MDB_NOOVERWRITE)
        c.put(bb(4), bb(5))
        c.put(bb(6), bb(7))
        c.put(bb(8), bb(9))
        txn.commit()
      } finally if (txn != null) txn.close()
    }
  }

  @Test def closedBackwardTest(): Unit = {
    verify(closedBackward(bb(7), bb(3)), 6, 4)
    verify(closedBackward(bb(6), bb(2)), 6, 4, 2)
    verify(closedBackward(bb(9), bb(3)), 8, 6, 4)
  }

  @Test def closedOpenBackwardTest(): Unit = {
    verify(closedOpenBackward(bb(8), bb(3)), 8, 6, 4)
    verify(closedOpenBackward(bb(7), bb(2)), 6, 4)
    verify(closedOpenBackward(bb(9), bb(3)), 8, 6, 4)
  }

  @Test def closedOpenTest(): Unit = {
    verify(closedOpen(bb(3), bb(8)), 4, 6)
    verify(closedOpen(bb(2), bb(6)), 2, 4)
  }

  @Test def closedTest(): Unit = {
    verify(closed(bb(3), bb(7)), 4, 6)
    verify(closed(bb(2), bb(6)), 2, 4, 6)
    verify(closed(bb(1), bb(7)), 2, 4, 6)
  }

  @Test def greaterThanBackwardTest(): Unit = {
    verify(greaterThanBackward(bb(6)), 4, 2)
    verify(greaterThanBackward(bb(7)), 6, 4, 2)
    verify(greaterThanBackward(bb(9)), 8, 6, 4, 2)
  }

  @Test def greaterThanTest(): Unit = {
    verify(greaterThan(bb(4)), 6, 8)
    verify(greaterThan(bb(3)), 4, 6, 8)
  }

  @Test(expected = classOf[IllegalStateException]) def iterableOnlyReturnedOnce(): Unit = {
    try {
      val txn = env.txnRead
      val c = db.iterate(txn)
      try {
        c.iterator // ok
        c.iterator // fails
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def iterate(): Unit = {
    try {
      val txn = env.txnRead
      val c = db.iterate(txn)
      try{
        for (kv <- c) {
          assertThat(kv.key.getInt, is(list.pollFirst))
          assertThat(kv.vals.getInt, is(list.pollFirst))
        }
      }
      finally
      {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test(expected = classOf[IllegalStateException]) def iteratorOnlyReturnedOnce(): Unit = {
    try {
      val txn = env.txnRead
      val c = db.iterate(txn)
      try {
        c.iterator // ok
        c.iterator // fails
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def lessThanBackwardTest(): Unit = {
    verify(lessThanBackward(bb(5)), 8, 6)
    verify(lessThanBackward(bb(2)), 8, 6, 4)
  }

  @Test def lessThanTest(): Unit = {
    verify(lessThan(bb(5)), 2, 4)
    verify(lessThan(bb(8)), 2, 4, 6)
  }

  @Test(expected = classOf[NoSuchElementException]) def nextThrowsNoSuchElementExceptionIfNoMoreElements(): Unit = {
    try {
      val txn = env.txnRead
      val c = db.iterate(txn)
      try {
        val i = c.iterator
        while (i.hasNext) {
          val kv = i.next
          assertThat(kv.key.getInt, is(list.pollFirst))
          assertThat(kv.vals.getInt, is(list.pollFirst))
        }
        assertThat(i.hasNext, is(false))
        i.next
      } finally {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
  }

  @Test def openBackwardTest(): Unit = {
    verify(openBackward(bb(7), bb(2)), 6, 4)
    verify(openBackward(bb(8), bb(1)), 6, 4, 2)
    verify(openBackward(bb(9), bb(4)), 8, 6)
  }

  @Test def openClosedBackwardTest(): Unit = {
    verify(openClosedBackward(bb(7), bb(2)), 6, 4, 2)
    verify(openClosedBackward(bb(8), bb(4)), 6, 4)
    verify(openClosedBackward(bb(9), bb(4)), 8, 6, 4)
  }

  @Test def openClosedBackwardTestWithGuava(): Unit = {
    val guava = UnsignedBytes.lexicographicalComparator
    val comparator:Comparator[ByteBuffer] = (bb1: ByteBuffer, bb2: ByteBuffer) => {
      val array1 = new Array[Byte](bb1.remaining)
      val array2 = new Array[Byte](bb2.remaining)
      bb1.mark
      bb2.mark
      bb1.get(array1)
      bb2.get(array2)
      bb1.reset
      bb2.reset
      guava.compare(array1, array2)
    }
    val guavaDbi = env.openDbi(DB_1, comparator, MDB_CREATE)
    populateDatabase(guavaDbi)
    verify(openClosedBackward(bb(7), bb(2)), guavaDbi, 6, 4, 2)
    verify(openClosedBackward(bb(8), bb(4)), guavaDbi, 6, 4)
  }

  @Test def openClosedTest(): Unit = {
    verify(openClosed(bb(3), bb(8)), 4, 6, 8)
    verify(openClosed(bb(2), bb(6)), 4, 6)
  }

  @Test def openTest(): Unit = {
    verify(open(bb(3), bb(7)), 4, 6)
    verify(open(bb(2), bb(8)), 4, 6)
  }

  @Test def removeOddElements(): Unit = {
    verify(all, 2, 4, 6, 8)
    var idx = -1
    try {
      val txn = env.txnWrite
      try {
        try {
          val ci = db.iterate(txn)
          try {
            val c = ci.iterator
            while (c.hasNext) {
              c.next
              idx += 1
              if (idx % 2 == 0) ci.remove()
            }
          } finally if (ci != null) ci.close()
        }
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    verify(all, 4, 8)
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def nextWithClosedEnvTest(): Unit = {
    try {
      val txn = env.txnRead
      try try {
        val ci = db.iterate(txn, KeyRange.all)
        try {
          val c = ci.iterator
          env.close()
          c.next
        } finally if (ci != null) ci.close()
      }
      finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def removeWithClosedEnvTest(): Unit = {
    try {
      val txn = env.txnWrite
      try try {
        val ci = db.iterate(txn, KeyRange.all)
        try {
          val c = ci.iterator
          val keyVal = c.next
          assertThat(keyVal, Matchers.notNullValue)
          env.close()
          ci.remove()
        } finally if (ci != null) ci.close()
      }
      finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def hasNextWithClosedEnvTest(): Unit = {
    try {
      val txn = env.txnRead
      try try {
        val ci = db.iterate(txn, KeyRange.all)
        try {
          val c = ci.iterator
          env.close()
          c.hasNext
        } finally if (ci != null) ci.close()
      }
      finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def forEachRemainingWithClosedEnvTest(): Unit = {
    try {
      val txn = env.txnRead
      try try {
        val ci = db.iterate(txn, KeyRange.all)
        try {
          val c = ci.iterator
          env.close()

//          c.forEachRemaining((keyVal: CursorIterable.KeyVal[ByteBuffer]) => {
//          })
        } finally if (ci != null) ci.close()
      }
      finally if (txn != null) txn.close()
    }
  }

  private def verify(range: KeyRange[ByteBuffer], expected: Int*): Unit = {
    verify(range, db, expected*)
  }

  private def verify(range: KeyRange[ByteBuffer], dbi: Dbi[ByteBuffer], expected: Int*): Unit = {
    val results = new util.ArrayList[Integer]
    try {
      val txn = env.txnRead
      val c = dbi.iterate(txn, range)
      try{
        for (kv <- c) {
          val key = kv.key.getInt
          val `val` = kv.vals.getInt
          results.add(key)
          assertThat(`val`, is(key + 1))
        }
      }
      finally
      {
        if (txn != null) txn.close()
        if (c != null) c.close()
      }
    }
    assertThat(results, hasSize(expected.length))
    for (idx <- 0 until results.size) {
      assertThat(results.get(idx), is(expected(idx)))
    }
  }
}