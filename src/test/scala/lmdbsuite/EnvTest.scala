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
import com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES

import java.nio.ByteBuffer.allocateDirect
import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.CoreMatchers.is
import org.hamcrest.CoreMatchers.notNullValue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.greaterThan
import torch.lmdb.flags.CopyFlags.MDB_CP_COMPACT
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.db.Env.Builder.MAX_READERS_DEFAULT
import torch.lmdb.db.Env.create
import torch.lmdb.db.Env.open
import torch.lmdb.flags.EnvFlags.MDB_NOSUBDIR
import torch.lmdb.flags.EnvFlags.MDB_RDONLY_ENV
import lmdbsuite.TestUtils.DB_1
import lmdbsuite.TestUtils.bb

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util.Random
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import torch.lmdb.db.{Env, Txn}
import torch.lmdb.db.Env.AlreadyClosedException
import torch.lmdb.db.Env.AlreadyOpenException
import torch.lmdb.db.Env.Builder
import torch.lmdb.db.Env.InvalidCopyDestination
import torch.lmdb.db.Env.MapFullException
import torch.lmdb.db.Txn.BadReaderLockException

/** Test {@link Env}. */
final class EnvTest {
  @Rule final def tmpDef = new TemporaryFolder(new File("./tmp"))
  val tmp = tmpDef

  @Test
  @throws[IOException]
  def byteUnit(): Unit = {
//    val tmp = new TemporaryFolder
    val path = new File("./tmp") // tmp.newFile
    try {
      val env = create.setMaxReaders(1).setMapSize(MEBIBYTES.toBytes(1)).open(path, MDB_NOSUBDIR)
      try {
        val info = env.info
        assertThat(info.mapSize, is(MEBIBYTES.toBytes(1)))
      } finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyOpenException])
  @throws[IOException]
  def cannotChangeMapSizeAfterOpen(): Unit = {
    val path = new File("./tmp") //tmp.newFile
    val builder = create.setMaxReaders(1)
    try {
      val env = builder.open(path, MDB_NOSUBDIR)
      try builder.setMapSize(1)
      finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyOpenException])
  @throws[IOException]
  def cannotChangeMaxDbsAfterOpen(): Unit = {
    val path = new File("./tmp") //tmp.newFile
    val builder = create.setMaxReaders(1)
    try {
      val env = builder.open(path, MDB_NOSUBDIR)
      try builder.setMaxDbs(1)
      finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyOpenException])
  @throws[IOException]
  def cannotChangeMaxReadersAfterOpen(): Unit = {
    val path = new File("./tmp") //tmp.newFile
    val builder = create.setMaxReaders(1)
    try {
      val env = builder.open(path, MDB_NOSUBDIR)
      try builder.setMaxReaders(1)
      finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Env.AlreadyClosedException])
  @throws[IOException]
  def cannotInfoOnceClosed(): Unit = {
    val path = tmp.newFile
    val env = create.setMaxReaders(1).open(path, MDB_NOSUBDIR)
    env.close()
    env.info
  }

  @Test(expected = classOf[Env.AlreadyOpenException])
  @throws[IOException]
  def cannotOpenTwice(): Unit = {
    val path = tmp.newFile
    val builder = create.setMaxReaders(1)
    builder.open(path, MDB_NOSUBDIR).close()
    builder.open(path, MDB_NOSUBDIR)
  }

  @Test(expected = classOf[IllegalArgumentException]) def cannotOverflowMapSize(): Unit = {
    val builder = create.setMaxReaders(1)
    val mb = 1_024 * 1_024
    val size = mb * 2_048 // as per issue 18
    builder.setMapSize(size)
  }

  @Test(expected = classOf[Env.AlreadyClosedException])
  @throws[IOException]
  def cannotStatOnceClosed(): Unit = {
    val path = tmp.newFile
    val env = create.setMaxReaders(1).open(path, MDB_NOSUBDIR)
    env.close()
    env.stat
  }

  @Test(expected = classOf[Env.AlreadyClosedException])
  @throws[IOException]
  def cannotSyncOnceClosed(): Unit = {
    val path = tmp.newFile
    val env = create.setMaxReaders(1).open(path, MDB_NOSUBDIR)
    env.close()
    env.sync(false)
  }

  @Test
  @throws[IOException]
  def copyDirectoryBased(): Unit = {
    val dest = new File("./tmp") // tmp.newFolder
    assertThat(dest.exists, is(true))
    assertThat(dest.isDirectory, is(true))
    assertThat(dest.list.length, is(0))
    val src = tmp.newFolder
    try {
      val env = create.setMaxReaders(1).open(src)
      try {
        env.copy(dest, MDB_CP_COMPACT)
        assertThat(dest.list.length, is(1))
      } finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Env.InvalidCopyDestination])
  @throws[IOException]
  def copyDirectoryRejectsFileDestination(): Unit = {
    val dest = tmp.newFile
    val src = tmp.newFolder
    try {
      val env = create.setMaxReaders(1).open(src)
      try env.copy(dest, MDB_CP_COMPACT)
      finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Env.InvalidCopyDestination])
  @throws[IOException]
  def copyDirectoryRejectsMissingDestination(): Unit = {
    val dest = new File("./tmp") // tmp.newFolder
    assertThat(dest.delete, is(true))
    val src = tmp.newFolder
    try {
      val env = create.setMaxReaders(1).open(src)
      try env.copy(dest, MDB_CP_COMPACT)
      finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Env.InvalidCopyDestination])
  @throws[IOException]
  def copyDirectoryRejectsNonEmptyDestination(): Unit = {
    val dest = tmp.newFolder
    val subDir = new File(dest, "hello")
    assertThat(subDir.mkdir, is(true))
    val src = tmp.newFolder
    try {
      val env = create.setMaxReaders(1).open(src)
      try env.copy(dest, MDB_CP_COMPACT)
      finally if (env != null) env.close()
    }
  }

  @Test
  @throws[IOException]
  def copyFileBased(): Unit = {
    val dest = tmp.newFile
    assertThat(dest.delete, is(true))
    assertThat(dest.exists, is(false))
    val src = tmp.newFile
    try {
      val env = create.setMaxReaders(1).open(src, MDB_NOSUBDIR)
      try env.copy(dest, MDB_CP_COMPACT)
      finally if (env != null) env.close()
    }
//    assertThat(dest.length, greaterThan(0L))
  }

  @Test(expected = classOf[Env.InvalidCopyDestination])
  @throws[IOException]
  def copyFileRejectsExistingDestination(): Unit = {
    val dest = tmp.newFile
    assertThat(dest.exists, is(true))
    val src = tmp.newFile
    try {
      val env = create.setMaxReaders(1).open(src, MDB_NOSUBDIR)
      try env.copy(dest, MDB_CP_COMPACT)
      finally if (env != null) env.close()
    }
  }

  @Test
  @throws[IOException]
  def createAsDirectory(): Unit = {
    val path = tmp.newFolder
    val env = create.setMaxReaders(1).open(path)
    assertThat(path.isDirectory, is(true))
    env.sync(false)
    env.close()
    assertThat(env.isClosed, is(true))
    env.close() // safe to repeat
  }

  @Test
  @throws[IOException]
  def createAsFile(): Unit = {
    val path = tmp.newFile
    try {
      val env = create.setMapSize(1_024 * 1_024).setMaxDbs(1).setMaxReaders(1).open(path, MDB_NOSUBDIR)
      try {
        env.sync(true)
        assertThat(path.isFile, is(true))
      } finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Txn.BadReaderLockException])
  @throws[IOException]
  def detectTransactionThreadViolation(): Unit = {
    val path = tmp.newFile
    try {
      val env = create.setMaxReaders(1).open(path, MDB_NOSUBDIR)
      try {
        env.txnRead
        env.txnRead
      } finally if (env != null) env.close()
    }
  }

  @Test
  @throws[IOException]
  def info(): Unit = {
    val path = tmp.newFile
    try {
      val env = create.setMaxReaders(4).setMapSize(123_456).open(path, MDB_NOSUBDIR)
      try {
        val info = env.info
        assertThat(info, is(notNullValue))
        assertThat(info.lastPageNumber, is(1L))
        assertThat(info.lastTransactionId, is(0L))
        assertThat(info.mapAddress, is(0L))
        assertThat(info.mapSize, is(123_456L))
        assertThat(info.maxReaders, is(4))
        assertThat(info.numReaders, is(0))
        assertThat(info.toString, containsString("maxReaders="))
        assertThat(env.getMaxKeySize, is(511))
      } finally if (env != null) env.close()
    }
  }

  @Test(expected = classOf[Env.MapFullException])
  @throws[IOException]
  def mapFull(): Unit = {
    val path = tmp.newFolder
    val k = new Array[Byte](500)
    val key = allocateDirect(500)
    val `val` = allocateDirect(1_024)
    val rnd = new Random
    try {
      val env = create.setMaxReaders(1).setMapSize(MEBIBYTES.toBytes(8)).setMaxDbs(1).open(path)
      try {
        val db = env.openDbi(DB_1, MDB_CREATE)
        while (true) {
          rnd.nextBytes(k)
          key.clear
          key.put(k).flip
          `val`.clear
          db.put(key, `val`)
        }
      } finally if (env != null) env.close()
    }
  }

  @Test
  @throws[IOException]
  def readOnlySupported(): Unit = {
    val path = tmp.newFolder
    try {
      val rwEnv = create.setMaxReaders(1).open(path)
      try {
        val rwDb = rwEnv.openDbi(DB_1, MDB_CREATE)
        rwDb.put(bb(1), bb(42))
      } finally if (rwEnv != null) rwEnv.close()
    }
    try {
      val roEnv = create.setMaxReaders(1).open(path, MDB_RDONLY_ENV)
      try {
        val roDb = roEnv.openDbi(DB_1)
        try {
          val roTxn = roEnv.txnRead
          try assertThat(roDb.get(roTxn, bb(1)), notNullValue)
          finally if (roTxn != null) roTxn.close()
        }
      } finally if (roEnv != null) roEnv.close()
    }
  }

  @Test
  @throws[IOException]
  def setMapSize(): Unit = {
    val path = tmp.newFolder
    val k = new Array[Byte](500)
    val key = allocateDirect(500)
    val `val` = allocateDirect(1_024)
    val rnd = new Random
    try {
      val env = create.setMaxReaders(1).setMapSize(KIBIBYTES.toBytes(256)).setMaxDbs(1).open(path)
      try {
        val db = env.openDbi(DB_1, MDB_CREATE)
        db.put(bb(1), bb(42))
        var mapFullExThrown = false
        try for (i <- 0 until 70) {
          rnd.nextBytes(k)
          key.clear
          key.put(k).flip
          `val`.clear
          db.put(key, `val`)
        }
        catch {
          case mfE: Env.MapFullException =>
            mapFullExThrown = true
        }
        assertThat(mapFullExThrown, is(true))
        env.setMapSize(KIBIBYTES.toBytes(1024))
        try {
          val roTxn = env.txnRead
          try assertThat(db.get(roTxn, bb(1)).get, is(42))
          finally if (roTxn != null) roTxn.close()
        }
        mapFullExThrown = false
        try for (i <- 0 until 70) {
          rnd.nextBytes(k)
          key.clear
          key.put(k).flip
          `val`.clear
          db.put(key, `val`)
        }
        catch {
          case mfE: Env.MapFullException =>
            mapFullExThrown = true
        }
        assertThat(mapFullExThrown, is(false))
      } finally if (env != null) env.close()
    }
  }

  @Test
  @throws[IOException]
  def stats(): Unit = {
    val path = tmp.newFile
    try {
      val env = create.setMaxReaders(1).open(path, MDB_NOSUBDIR)
      try {
        val stat = env.stat
        assertThat(stat, is(notNullValue))
        assertThat(stat.branchPages, is(0L))
        assertThat(stat.depth, is(0))
        assertThat(stat.entries, is(0L))
        assertThat(stat.leafPages, is(0L))
        assertThat(stat.overflowPages, is(0L))
        assertThat(stat.pageSize % 4_096, is(0))
        assertThat(stat.toString, containsString("pageSize="))
      } finally if (env != null) env.close()
    }
  }

  @Test
  @throws[IOException]
  def testDefaultOpen(): Unit = {
    val path = tmp.newFolder
    try {
      val env = open(path, 10)
      try {
        val info = env.info
        assertThat(info.maxReaders, is(MAX_READERS_DEFAULT))
        val db = env.openDbi("test", MDB_CREATE)
        db.put(allocateDirect(1), allocateDirect(1))
      } finally if (env != null) env.close()
    }
  }
}