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

import java.nio.ByteBuffer.allocateDirect
import java.nio.charset.StandardCharsets.UTF_8
import org.junit.Assert.{fail, fail as skey}
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.db.Env.create

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.mockito.MockedStatic
import org.mockito.Mockito
import torch.lmdb.db.{Dbi, Txn}
import torch.lmdb.flags.MaskedFlag

object GarbageCollectionTest {
  private val DB_NAME = "my DB"
  private val KEY_PREFIX = "Uncorruptedkey"
  private val VAL_PREFIX = "Uncorruptedval"
}

class GarbageCollectionTest {
  @Rule final val tmp = new TemporaryFolder

  @Test
  @throws[IOException]
  def buffersNotGarbageCollectedTest(): Unit = {
    val path = tmp.newFolder
    try {
      val env = create.setMapSize(2_085_760_999).setMaxDbs(1).open(path)
      try {
        val db = env.openDbi(GarbageCollectionTest.DB_NAME, MDB_CREATE)
        try {
          val txn = env.txnWrite
          try {
            for (i <- 0 until 5_000) {
              putBuffer(db, txn, i)
            }
            txn.commit()
          } finally if (txn != null) txn.close()
        }
        // Call GC before writing to LMDB and after last reference to buffer by
        // changing the behavior of mask
        try {
          val mockedStatic = Mockito.mockStatic(classOf[MaskedFlag])
          try {
//            mockedStatic.when(MaskedFlag.mask).thenAnswer((invocationOnMock: InvocationOnMock) => {
//              System.gc()
//              0
//            })
            val gcRecordWrites = Integer.getInteger("gcRecordWrites", 50)
            try {
              val txn = env.txnWrite
              try {
                for (i <- 0 until gcRecordWrites) {
                  putBuffer(db, txn, i)
                }
                txn.commit()
              } finally if (txn != null) txn.close()
            }
          } finally if (mockedStatic != null) mockedStatic.close()
        }
        // Find corrupt keys
        try {
          val txn = env.txnRead
          try try {
            val c = db.openCursor(txn)
            try if (c.first) {
              val rkey = new Array[Byte](c.key.remaining)
              c.key.get(rkey)
              val rval = new Array[Byte](c.vals.remaining)
              c.vals.get(rval)
              val skey = new String(rkey, UTF_8)
              val sval = new String(rval, UTF_8)
              if (!skey.startsWith("Uncorruptedkey")) fail("Found corrupt key " + skey)
              if (!sval.startsWith("Uncorruptedval")) fail("Found corrupt val " + sval)
              while (c.next) {
                val rkey = new Array[Byte](c.key.remaining)
                c.key.get(rkey)
                val rval = new Array[Byte](c.vals.remaining)
                c.vals.get(rval)
                val skey = new String(rkey, UTF_8)
                val sval = new String(rval, UTF_8)
                if (!skey.startsWith("Uncorruptedkey")) fail("Found corrupt key " + skey)
                if (!sval.startsWith("Uncorruptedval")) fail("Found corrupt val " + sval)
              } 
            }
            finally if (c != null) c.close()
          }
          finally if (txn != null) txn.close()
        }
      } finally if (env != null) env.close()
    }
  }

  private def putBuffer(db: Dbi[ByteBuffer], txn: Txn[ByteBuffer], i: Int): Unit = {
    val key = allocateDirect(24)
    val `val` = allocateDirect(24)
    key.put((GarbageCollectionTest.KEY_PREFIX + i).getBytes(UTF_8)).flip
    `val`.put((GarbageCollectionTest.VAL_PREFIX + i).getBytes(UTF_8)).flip
    db.put(txn, key, `val`)
  }
}