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

import java.lang.Integer.BYTES
import java.nio.ByteBuffer.allocate
import java.nio.ByteBuffer.allocateDirect
import java.nio.ByteOrder.BIG_ENDIAN
import java.nio.ByteOrder.LITTLE_ENDIAN
import org.hamcrest.CoreMatchers.is
import org.hamcrest.CoreMatchers.not
import org.hamcrest.CoreMatchers.notNullValue
import org.hamcrest.CoreMatchers.startsWith
import org.hamcrest.MatcherAssert.assertThat
import torch.lmdb.proxy.BufferProxy.MDB_VAL_STRUCT_SIZE
import torch.lmdb.proxy.ByteBufferProxy.AbstractByteBufferProxy.findField
import torch.lmdb.proxy.ByteBufferProxy.PROXY_OPTIMAL
import torch.lmdb.proxy.ByteBufferProxy.PROXY_SAFE
import torch.lmdb.flags.DbiFlags.MDB_CREATE
import torch.lmdb.db.Env.create
import torch.lmdb.db.Library.RUNTIME
import lmdbsuite.TestUtils.DB_1
import lmdbsuite.TestUtils.invokePrivateConstructor
import torch.lmdb.UnsafeAccess.ALLOW_UNSAFE

import java.io.File
import java.io.IOException
import java.lang.reflect.Field
import java.nio.ByteBuffer
import jnr.ffi.Pointer
import jnr.ffi.provider.MemoryManager
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import torch.lmdb.db.Env
import torch.lmdb.proxy.ByteBufferProxy.BufferMustBeDirectException
import torch.lmdb.db.Env.ReadersFullException
import torch.lmdb.exceptions.LmdbException
import torch.lmdb.proxy.{BufferProxy, ByteBufferProxy}

/** Test {@link ByteBufferProxy}. */
object ByteBufferProxyTest {
  val MEM_MGR = RUNTIME.getMemoryManager
}

final class ByteBufferProxyTest {
  @Rule final def tmpDef = new TemporaryFolder
  val tmp = tmpDef

  @Test(expected = classOf[ByteBufferProxy.BufferMustBeDirectException])
  @throws[IOException]
  def buffersMustBeDirect(): Unit = {
    val path = new File("./tmp") //tmp.newFolder("./tmp")
    try {
      val env = create.setMaxReaders(1).open(path)
      try {
        val db = env.openDbi(DB_1, MDB_CREATE)
        val key = allocate(100)
        key.putInt(1).flip
        val `val` = allocate(100)
        `val`.putInt(1).flip
        db.put(key, `val`) // error
      } finally if (env != null) env.close()
    }
  }

  @Test def byteOrderResets(): Unit = {
    val retries = 100
    for (i <- 0 until retries) {
      val bb = PROXY_OPTIMAL.allocate
      bb.order(LITTLE_ENDIAN)
      PROXY_OPTIMAL.deallocate(bb)
    }

    for (i <- 0 until retries) {
      println(s"${(PROXY_OPTIMAL.allocate.order)}")
      assertThat(PROXY_OPTIMAL.allocate.order, is(BIG_ENDIAN))
    }
  }

  @Test def coverPrivateConstructor(): Unit = {
    invokePrivateConstructor(classOf[ByteBufferProxy])
  }

  @Test(expected = classOf[LmdbException]) def fieldNeverFound(): Unit = {
    findField(classOf[Exception], "notARealField")
  }

  @Test def fieldSuperclassScan(): Unit = {
    val f = findField(classOf[Env.ReadersFullException], "rc")
    assertThat(f, is(notNullValue))
  }

  @Test def inOutBuffersProxyOptimal(): Unit = {
    checkInOut(PROXY_OPTIMAL)
  }

  @Test def inOutBuffersProxySafe(): Unit = {
    checkInOut(PROXY_SAFE)
  }

  @Test def optimalAlwaysAvailable(): Unit = {
    val v = PROXY_OPTIMAL
    assertThat(v, is(notNullValue))
  }

  @Test def safeCanBeForced(): Unit = {
    val v = PROXY_SAFE
    assertThat(v, is(notNullValue))
    assertThat(v.getClass.getSimpleName, startsWith("Reflect"))
  }

  @Test def unsafeIsDefault(): Unit = {
    assertThat(ALLOW_UNSAFE, is(true))
    val v = PROXY_OPTIMAL
    assertThat(v, is(notNullValue))
    assertThat(v, is(not(PROXY_SAFE)))
    assertThat(v.getClass.getSimpleName, startsWith("Unsafe"))
  }

  private def checkInOut(v: BufferProxy[ByteBuffer]): Unit = {
    // allocate a buffer larger than max key size
    val b = allocateDirect(1_000)
    b.putInt(1)
    b.putInt(2)
    b.putInt(3)
    b.flip
    b.position(BYTES) // skip 1
    val p = ByteBufferProxyTest.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false)
    v.in(b, p, p.address)
    val bb = allocateDirect(1)
    v.out(bb, p, p.address)
    println(s"${bb.getInt}  bb ${bb.remaining()}")
    assertThat(bb.getInt, is(2))
    assertThat(bb.getInt, is(3))
    assertThat(bb.remaining, is(0))
  }
}