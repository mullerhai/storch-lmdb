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

import java.nio.ByteBuffer.allocateDirect
import java.nio.charset.StandardCharsets.UTF_8
import org.hamcrest.CoreMatchers.is
import org.hamcrest.CoreMatchers.not
import org.hamcrest.CoreMatchers.notNullValue
import org.hamcrest.CoreMatchers.nullValue
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.assertEquals
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.db.Env.create
import torch.lmdb.flags.EnvFlags.MDB_NOSUBDIR
import torch.lmdb.flags.EnvFlags.MDB_RDONLY_ENV
import torch.lmdb.db.KeyRange.closed
import lmdbsuite.TestUtils.DB_1
import lmdbsuite.TestUtils.POSIX_MODE
import lmdbsuite.TestUtils.bb
import torch.lmdb.db.Txn.State.DONE
import torch.lmdb.db.Txn.State.READY
import torch.lmdb.db.Txn.State.RELEASED
import torch.lmdb.db.Txn.State.RESET
import torch.lmdb.flags.TxnFlags.MDB_RDONLY_TXN

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicLong
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import torch.lmdb.db.Dbi.BadValueSizeException
import torch.lmdb.db.Env.AlreadyClosedException
import torch.lmdb.db.{Dbi, Env, Txn}
import torch.lmdb.db.Txn.EnvIsReadOnly
import torch.lmdb.db.Txn.IncompatibleParent
import torch.lmdb.db.Txn.NotReadyException
import torch.lmdb.db.Txn.NotResetException
import torch.lmdb.db.Txn.ReadOnlyRequiredException
import torch.lmdb.db.Txn.ReadWriteRequiredException
import torch.lmdb.db.Txn.ResetException

/** Test {@link Txn}. */
final class TxnTest {
  @Rule final def tmpDef = new TemporaryFolder
  val tmp = tmpDef
  private var env: Env[ByteBuffer] = null
  private var path: File = null

  @After def after(): Unit = {
    env.close()
  }

  @Before
  @throws[IOException]
  def before(): Unit = {
    path = new File("./tmp") // tmp.newFile
    env = create.setMapSize(KIBIBYTES.toBytes(256)).setMaxReaders(1).setMaxDbs(2).open(path, POSIX_MODE, MDB_NOSUBDIR)
  }

  @Test(expected = classOf[Dbi.BadValueSizeException])
  @throws[IOException]
  def largeKeysRejected(): Unit = {
    val dbi = env.openDbi(DB_1, MDB_CREATE)
    val key = allocateDirect(env.getMaxKeySize + 1)
    key.limit(key.capacity)
    dbi.put(key, bb(2))
  }

  @Test def rangeSearch(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    val key = allocateDirect(env.getMaxKeySize)
    key.put("cherry".getBytes(UTF_8)).flip
    db.put(key, bb(1))
    key.clear
    key.put("strawberry".getBytes(UTF_8)).flip
    db.put(key, bb(3))
    key.clear
    key.put("pineapple".getBytes(UTF_8)).flip
    db.put(key, bb(2))
    try {
      val txn = env.txnRead
      try {
        val start = allocateDirect(env.getMaxKeySize)
        start.put("a".getBytes(UTF_8)).flip
        val end = allocateDirect(env.getMaxKeySize)
        end.put("z".getBytes(UTF_8)).flip
        val keysFound = new util.ArrayList[String]
        try {
          val ckr = db.iterate(txn, closed(start, end))
          try{
            for (kv <- ckr) {
              keysFound.add(UTF_8.decode(kv.key).toString)
            }
          } finally
          {
            if (ckr != null) ckr.close()
          }
        }
        assertEquals(3, keysFound.size)
      } finally if (txn != null) txn.close()
    }
  }

  @Test def readOnlyTxnAllowedInReadOnlyEnv(): Unit = {
    env.openDbi(DB_1, MDB_CREATE)
    try {
      val roEnv = create.setMaxReaders(1).open(path, MDB_NOSUBDIR, MDB_RDONLY_ENV)
      try assertThat(roEnv.txnRead, is(notNullValue))
      finally if (roEnv != null) roEnv.close()
    }
  }

  @Test(expected = classOf[Txn.EnvIsReadOnly]) def readWriteTxnDeniedInReadOnlyEnv(): Unit = {
    env.openDbi(DB_1, MDB_CREATE)
    env.close()
    try {
      val roEnv = create.setMaxReaders(1).open(path, MDB_NOSUBDIR, MDB_RDONLY_ENV)
      try roEnv.txnWrite // error
      finally if (roEnv != null) roEnv.close()
    }
  }

  @Test(expected = classOf[Txn.NotReadyException]) def testCheckNotCommitted(): Unit = {
    try {
      val txn = env.txnRead
      try {
        txn.commit()
        txn.checkReady()
      } finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Txn.ReadOnlyRequiredException]) def testCheckReadOnly(): Unit = {
    try {
      val txn = env.txnWrite
      try txn.checkReadOnly()
      finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Txn.ReadWriteRequiredException]) def testCheckWritesAllowed(): Unit = {
    try {
      val txn = env.txnRead
      try txn.checkWritesAllowed()
      finally if (txn != null) txn.close()
    }
  }

  @Test def testGetId(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    val txId1 = new AtomicLong
    val txId2 = new AtomicLong
    try {
      val tx1 = env.txnRead
      try txId1.set(tx1.getId)
      finally if (tx1 != null) tx1.close()
    }
    db.put(bb(1), bb(2))
    try {
      val tx2 = env.txnRead
      try txId2.set(tx2.getId)
      finally if (tx2 != null) tx2.close()
    }
    // should not see the same snapshot
    assertThat(txId1.get, is(not(txId2.get)))
  }

  @Test def txCanCommitThenCloseWithoutError(): Unit = {
    try {
      val txn = env.txnRead
      try {
        assertThat(txn.getState, is(READY))
        txn.commit()
        assertThat(txn.getState, is(DONE))
      } finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Txn.NotReadyException]) def txCannotAbortIfAlreadyCommitted(): Unit = {
    try {
      val txn = env.txnRead
      try {
        assertThat(txn.getState, is(READY))
        txn.commit()
        assertThat(txn.getState, is(DONE))
        txn.abort()
      } finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Txn.NotReadyException]) def txCannotCommitTwice(): Unit = {
    try {
      val txn = env.txnRead
      try {
        txn.commit()
        txn.commit() // error
      } finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def txConstructionDeniedIfEnvClosed(): Unit = {
    env.close()
    env.txnRead
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def txRenewDeniedIfEnvClosed(): Unit = {
    val txnRead = env.txnRead
    txnRead.close()
    env.close()
    txnRead.renew()
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def txCloseDeniedIfEnvClosed(): Unit = {
    val txnRead = env.txnRead
    env.close()
    txnRead.close()
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def txCommitDeniedIfEnvClosed(): Unit = {
    val txnRead = env.txnRead
    env.close()
    txnRead.commit()
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def txAbortDeniedIfEnvClosed(): Unit = {
    val txnRead = env.txnRead
    env.close()
    txnRead.abort()
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def txResetDeniedIfEnvClosed(): Unit = {
    val txnRead = env.txnRead
    env.close()
    txnRead.reset()
  }

  @Test def txParent(): Unit = {
    try {
      val txRoot = env.txnWrite
      val txChild = env.txn(txRoot)
      try {
        assertThat(txRoot.getParent, is(nullValue))
        assertThat(txChild.getParent, is(txRoot))
      } finally {
        if (txRoot != null) txRoot.close()
        if (txChild != null) txChild.close()
      }
    }
  }

  @Test(expected = classOf[Env.AlreadyClosedException]) def txParentDeniedIfEnvClosed(): Unit = {
    try {
      val txRoot = env.txnWrite
      val txChild = env.txn(txRoot)
      try {
        env.close()
        assertThat(txChild.getParent, is(txRoot))
      } finally {
        if (txRoot != null) txRoot.close()
        if (txChild != null) txChild.close()
      }
    }
  }

  @Test(expected = classOf[Txn.IncompatibleParent]) def txParentROChildRWIncompatible(): Unit = {
    try {
      val txRoot = env.txnRead
      try env.txn(txRoot) // error
      finally if (txRoot != null) txRoot.close()
    }
  }

  @Test(expected = classOf[Txn.IncompatibleParent]) def txParentRWChildROIncompatible(): Unit = {
    try {
      val txRoot = env.txnWrite
      try env.txn(txRoot, MDB_RDONLY_TXN) // error
      finally if (txRoot != null) txRoot.close()
    }
  }

  @Test def txReadOnly(): Unit = {
    try {
      val txn = env.txnRead
      try {
        assertThat(txn.getParent, is(nullValue))
        assertThat(txn.getState, is(READY))
        assertThat(txn.isReadOnly, is(true))
        txn.checkReady()
        txn.checkReadOnly()
        txn.reset()
        assertThat(txn.getState, is(RESET))
        txn.renew()
        assertThat(txn.getState, is(READY))
        txn.commit()
        assertThat(txn.getState, is(DONE))
        txn.close()
        assertThat(txn.getState, is(RELEASED))
      } finally if (txn != null) txn.close()
    }
  }

  @Test def txReadWrite(): Unit = {
    val txn = env.txnWrite
    assertThat(txn.getParent, is(nullValue))
    assertThat(txn.getState, is(READY))
    assertThat(txn.isReadOnly, is(false))
    txn.checkReady()
    txn.checkWritesAllowed()
    txn.commit()
    assertThat(txn.getState, is(DONE))
    txn.close()
    assertThat(txn.getState, is(RELEASED))
  }

  @Test(expected = classOf[Txn.NotResetException]) def txRenewDeniedWithoutPriorReset(): Unit = {
    try {
      val txn = env.txnRead
      try txn.renew()
      finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Txn.ResetException]) def txResetDeniedForAlreadyResetTransaction(): Unit = {
    try {
      val txn = env.txnRead
      try {
        txn.reset()
        txn.renew()
        txn.reset()
        txn.reset()
      } finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Txn.ReadOnlyRequiredException]) def txResetDeniedForReadWriteTransaction(): Unit = {
    try {
      val txn = env.txnWrite
      try txn.reset()
      finally if (txn != null) txn.close()
    }
  }

  @Test(expected = classOf[Dbi.BadValueSizeException])
  @throws[IOException]
  def zeroByteKeysRejected(): Unit = {
    val dbi = env.openDbi(DB_1, MDB_CREATE)
    val key = allocateDirect(4)
    key.putInt(1)
    assertThat(key.remaining, is(0)) // because key.flip() skipped
    dbi.put(key, bb(2))
  }
}