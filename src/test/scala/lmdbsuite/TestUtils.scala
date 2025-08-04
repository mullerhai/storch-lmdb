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

import io.netty.buffer.PooledByteBufAllocator.DEFAULT

import java.lang.Integer.BYTES
import java.nio.ByteBuffer.allocateDirect
import io.netty.buffer.ByteBuf

import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.nio.ByteBuffer
import org.agrona.MutableDirectBuffer
import org.agrona.concurrent.UnsafeBuffer
import torch.lmdb.exceptions.LmdbException

/** Static constants and methods that are convenient when writing LMDB-related tests. */
object TestUtils {
  val DB_1 = "test-db-1"
  val POSIX_MODE = 0x1b4

  def ba(value: Int) = {
    val b = new UnsafeBuffer(new Array[Byte](4))
    b.putInt(0, value)
    b.byteArray
  }

  def bb(value: Int) = {
    val bb = allocateDirect(BYTES)
    bb.putInt(value).flip
    bb
  }

  def invokePrivateConstructor(clazz: Class[?]): Unit = {
    try {
      val c = clazz.getDeclaredConstructor()
      c.setAccessible(true)
      c.newInstance()
    } catch {
      case e@(_: NoSuchMethodException | _: InstantiationException | _: IllegalAccessException | _: IllegalArgumentException | _: InvocationTargetException) =>
        throw new LmdbException("Private construction failed", e)
    }
  }

  def mdb(value: Int) = {
    val b = new UnsafeBuffer(allocateDirect(BYTES))
    b.putInt(0, value)
    b
  }

  def nb(value: Int) = {
    val b = DEFAULT.directBuffer(BYTES)
    b.writeInt(value)
    b
  }
}

final class TestUtils private {
}