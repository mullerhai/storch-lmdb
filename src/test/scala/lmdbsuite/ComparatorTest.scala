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
import java.nio.charset.StandardCharsets.US_ASCII
import org.hamcrest.core.Is.is
import org.junit.Assert.assertThat
import torch.lmdb.proxy.ByteArrayProxy.PROXY_BA
import torch.lmdb.proxy.ByteBufProxy.PROXY_NETTY
import torch.lmdb.proxy.ByteBufferProxy.PROXY_OPTIMAL
import lmdbsuite.ComparatorTest.ComparatorResult.EQUAL_TO
import lmdbsuite.ComparatorTest.ComparatorResult.GREATER_THAN
import lmdbsuite.ComparatorTest.ComparatorResult.LESS_THAN
import lmdbsuite.ComparatorTest.ComparatorResult.get
import torch.lmdb.DirectBufferProxy.PROXY_DB
import com.google.common.primitives.SignedBytes
import com.google.common.primitives.UnsignedBytes
import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer
import java.util
import java.util.Comparator
import org.agrona.DirectBuffer
import org.agrona.concurrent.UnsafeBuffer
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameter
import org.junit.runners.Parameterized.Parameters

/** Tests comparator functions are consistent across buffers. */
@RunWith(classOf[Parameterized]) object ComparatorTest {
  // H = 1 (high), L = 0 (low), X = byte not set in buffer
  private val HL = buffer(1, 0)
  private val HLLLLLLL = buffer(1, 0, 0, 0, 0, 0, 0, 0)
  private val HLLLLLLX = buffer(1, 0, 0, 0, 0, 0, 0)
  private val HX = buffer(1)
  private val LH = buffer(0, 1)
  private val LHLLLLLL = buffer(0, 1, 0, 0, 0, 0, 0, 0)
  private val LL = buffer(0, 0)
  private val LLLLLLLL = buffer(0, 0, 0, 0, 0, 0, 0, 0)
  private val LLLLLLLX = buffer(0, 0, 0, 0, 0, 0, 0)
  private val LX = buffer(0)
  private val XX = buffer(0, 0)

  @Parameters(name = "{index}: comparable: {0}") def data: Array[AnyRef] = {
    val string = new ComparatorTest.StringRunner
    val db = new ComparatorTest.DirectBufferRunner
    val ba = new ComparatorTest.ByteArrayRunner
    val bb = new ComparatorTest.ByteBufferRunner
    val netty = new ComparatorTest.NettyRunner
    val gub = new ComparatorTest.GuavaUnsignedBytes
    val gsb = new ComparatorTest.GuavaSignedBytes
    Array[AnyRef](string, db, ba, bb, netty, gub, gsb)
  }

  private def buffer(bytes: Int*) = {
    val array = new Array[Byte](bytes.length)
    for (i <- 0 until bytes.length) {
      array(i) = bytes(i).toByte
    }
    array
  }

  /** Tests {@link ByteArrayProxy}. */
  final private class ByteArrayRunner extends ComparatorTest.ComparatorRunner {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val c = PROXY_BA.getComparator()
      c.compare(o1, o2)
    }
  }

  /** Tests {@link ByteBufferProxy}. */
  final private class ByteBufferRunner extends ComparatorTest.ComparatorRunner {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val c = PROXY_OPTIMAL.getComparator()
      // Convert arrays to buffers that are larger than the array, with
      // limit set at the array length. One buffer bigger than the other.
      var o1b = arrayToBuffer(o1, o1.length * 3)
      var o2b = arrayToBuffer(o2, o2.length * 2)
      val result = c.compare(o1b, o2b)
      // Now swap which buffer is bigger
      o1b = arrayToBuffer(o1, o1.length * 2)
      o2b = arrayToBuffer(o2, o2.length * 3)
      val result2 = c.compare(o1b, o2b)
      assertThat(result2, is(result))
      // Now try with buffers sized to the array.
      o1b = ByteBuffer.wrap(o1)
      o2b = ByteBuffer.wrap(o2)
      val result3 = c.compare(o1b, o2b)
      assertThat(result3, is(result))
      result
    }

    private def arrayToBuffer(arr: Array[Byte], bufferCapacity: Int) = {
      if (bufferCapacity < arr.length) throw new IllegalArgumentException("bufferCapacity < arr.length")
      val newArr = util.Arrays.copyOf(arr, bufferCapacity)
      val byteBuffer = ByteBuffer.wrap(newArr)
      byteBuffer.limit(arr.length)
      byteBuffer.position(0)
      byteBuffer
    }
  }

  /** Tests {@link DirectBufferProxy}. */
  final private class DirectBufferRunner extends ComparatorTest.ComparatorRunner {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val o1b = new UnsafeBuffer(o1)
      val o2b = new UnsafeBuffer(o2)
      val c = PROXY_DB.getComparator()
      c.compare(o1b, o2b)
    }
  }

  /** Tests using Guava's {@link SignedBytes} comparator. */
  final private class GuavaSignedBytes extends ComparatorTest.ComparatorRunner {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val c = SignedBytes.lexicographicalComparator
      c.compare(o1, o2)
    }
  }

  /** Tests using Guava's {@link UnsignedBytes} comparator. */
  final private class GuavaUnsignedBytes extends ComparatorTest.ComparatorRunner {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val c = UnsignedBytes.lexicographicalComparator
      c.compare(o1, o2)
    }
  }

  /** Tests {@link ByteBufProxy}. */
  final private class NettyRunner extends ComparatorTest.ComparatorRunner {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val o1b = DEFAULT.directBuffer(o1.length)
      val o2b = DEFAULT.directBuffer(o2.length)
      o1b.writeBytes(o1)
      o2b.writeBytes(o2)
      val c = PROXY_NETTY.getComparator()
      c.compare(o1b, o2b)
    }
  }

  /**
   * Tests {@link String} by providing a reference implementation of what a comparator involving
   * ASCII-encoded bytes should return.
   */
  final private class StringRunner extends ComparatorTest.ComparatorRunner {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val o1s = new String(o1, US_ASCII)
      val o2s = new String(o2, US_ASCII)
      o1s.compareTo(o2s)
    }
  }

  /** Converts an integer result code into its contractual meaning. */
  object ComparatorResult extends Enumeration {
    type ComparatorResult = Value
    val LESS_THAN, EQUAL_TO, GREATER_THAN = Value

    def get(comparatorResult: Int): ComparatorResult = {
      if (comparatorResult == 0) return EQUAL_TO
      if (comparatorResult < 0) LESS_THAN
      else GREATER_THAN
    }
  }

  /** Interface that can test a {@link BufferProxy} <code>compare</code> method. */
  trait ComparatorRunner {
    /**
     * Convert the passed byte arrays into the proxy's relevant buffer type and then invoke the
     * comparator.
     *
     * @param o1 lhs buffer content
     * @param o2 rhs buffer content
     * @return as per {@link Comparable}
     */
    def compare(o1: Array[Byte], o2: Array[Byte]): Int
  }
}

@RunWith(classOf[Parameterized]) final class ComparatorTest {
  /** Injected by {@link # data ( )} with appropriate runner. */
    @Parameter var comparator: ComparatorTest.ComparatorRunner = null

  @Test def atLeastOneBufferHasEightBytes(): Unit = {
    assertThat(get(comparator.compare(ComparatorTest.HLLLLLLL, ComparatorTest.LLLLLLLL)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LLLLLLLL, ComparatorTest.HLLLLLLL)), is(LESS_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LHLLLLLL, ComparatorTest.LLLLLLLL)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LLLLLLLL, ComparatorTest.LHLLLLLL)), is(LESS_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LLLLLLLL, ComparatorTest.LLLLLLLX)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LLLLLLLX, ComparatorTest.LLLLLLLL)), is(LESS_THAN))
    assertThat(get(comparator.compare(ComparatorTest.HLLLLLLL, ComparatorTest.HLLLLLLX)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.HLLLLLLX, ComparatorTest.HLLLLLLL)), is(LESS_THAN))
    assertThat(get(comparator.compare(ComparatorTest.HLLLLLLX, ComparatorTest.LHLLLLLL)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LHLLLLLL, ComparatorTest.HLLLLLLX)), is(LESS_THAN))
  }

  @Test def buffersOfTwoBytes(): Unit = {
    assertThat(get(comparator.compare(ComparatorTest.LL, ComparatorTest.XX)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.XX, ComparatorTest.LL)), is(LESS_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LL, ComparatorTest.LX)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LX, ComparatorTest.LL)), is(LESS_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LH, ComparatorTest.LX)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LX, ComparatorTest.HL)), is(LESS_THAN))
    assertThat(get(comparator.compare(ComparatorTest.HX, ComparatorTest.LL)), is(GREATER_THAN))
    assertThat(get(comparator.compare(ComparatorTest.LH, ComparatorTest.HX)), is(LESS_THAN))
  }

  @Test def equalBuffers(): Unit = {
    assertThat(get(comparator.compare(ComparatorTest.LL, ComparatorTest.LL)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.HX, ComparatorTest.HX)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.LH, ComparatorTest.LH)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.LL, ComparatorTest.LL)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.LX, ComparatorTest.LX)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.HLLLLLLL, ComparatorTest.HLLLLLLL)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.HLLLLLLX, ComparatorTest.HLLLLLLX)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.LHLLLLLL, ComparatorTest.LHLLLLLL)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.LLLLLLLL, ComparatorTest.LLLLLLLL)), is(EQUAL_TO))
    assertThat(get(comparator.compare(ComparatorTest.LLLLLLLX, ComparatorTest.LLLLLLLX)), is(EQUAL_TO))
  }
}