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

import org.hamcrest.CoreMatchers.is
import org.hamcrest.CoreMatchers.nullValue
import org.hamcrest.MatcherAssert.assertThat
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
import torch.lmdb.enums.IteratorOp.{CALL_NEXT_OP, RELEASE, TERMINATE}

import java.util
import org.junit.Before
import org.junit.Test
import torch.lmdb.db.KeyRange
import torch.lmdb.enums.CursorOp
import torch.lmdb.enums.IteratorOp
import torch.lmdb.enums.KeyRangeType

import java.util.Comparator

/**
 * Test {@link KeyRange}.
 *
 * <p>This test case focuses on the contractual correctness detailed in {@link KeyRangeType}. It
 * does this using integers as per the JavaDoc examples.
 */
object KeyRangeTest {
  /**
   * Cursor that behaves like an LMDB cursor would.
   *
   * <p>We use <code>Integer</code> rather than the primitive to represent a <code>null</code>
   * buffer.
   */
  private object FakeCursor {
    private val KEYS = Array[Int](2, 4, 6, 8)
  }

  final private class FakeCursor {
    private var position = 0

    def apply(op: CursorOp, startKey: Integer): Integer = {
      var key: Integer = null
      op match {
        case CursorOp.FIRST =>
          first
        case CursorOp.LAST =>
          last
        case CursorOp.NEXT =>
          next
        case CursorOp.PREV =>
          prev
        case CursorOp.GET_START_KEY =>
          getWithSetRange(startKey)
        case CursorOp.GET_START_KEY_BACKWARD =>
          key = getWithSetRange(startKey)
          if (key != null) return key
          last
        case _ =>
          throw new IllegalStateException("Unknown operation")
      }
    }

    def first = {
      position = 0
      FakeCursor.KEYS(position)
    }

    def getWithSetRange(startKey: Integer): Integer = {
      for (idx <- 0 until FakeCursor.KEYS.length) {
        val candidate = FakeCursor.KEYS(idx)
        if (candidate == startKey || candidate > startKey) {
          position = idx
          return FakeCursor.KEYS(position)
        }
      }
      null
    }

    def last = {
      position = FakeCursor.KEYS.length - 1
      FakeCursor.KEYS(position)
    }

    def next: Integer = {
      position += 1
      if (position == FakeCursor.KEYS.length) return null
      FakeCursor.KEYS(position)
    }

    def prev: Integer = {
      position -= 1
      if (position == -1) return null
      FakeCursor.KEYS(position)
    }

    def reset(): Unit = {
      position = 0
    }
  }
}

final class KeyRangeTest {
  final private val cursor = new KeyRangeTest.FakeCursor

  @Test def allBackwardTest(): Unit = {
    verify(allBackward, 8, 6, 4, 2)
  }

  @Test def allTest(): Unit = {
    verify(all, 2, 4, 6, 8)
  }

  @Test def atLeastBackwardTest(): Unit = {
    verify(atLeastBackward(5), 4, 2)
    verify(atLeastBackward(6), 6, 4, 2)
    verify(atLeastBackward(9), 8, 6, 4, 2)
  }

  @Test def atLeastTest(): Unit = {
    verify(atLeast(5), 6, 8)
    verify(atLeast(6), 6, 8)
  }

  @Test def atMostBackwardTest(): Unit = {
    verify(atMostBackward(5), 8, 6)
    verify(atMostBackward(6), 8, 6)
  }

  @Test def atMostTest(): Unit = {
    verify(atMost(5), 2, 4)
    verify(atMost(6), 2, 4, 6)
  }

  @Before def before(): Unit = {
    cursor.reset()
  }

  @Test def closedBackwardTest(): Unit = {
    verify(closedBackward(7, 3), 6, 4)
    verify(closedBackward(6, 2), 6, 4, 2)
    verify(closedBackward(9, 3), 8, 6, 4)
  }

  @Test def closedOpenBackwardTest(): Unit = {
    verify(closedOpenBackward(8, 3), 8, 6, 4)
    verify(closedOpenBackward(7, 2), 6, 4)
    verify(closedOpenBackward(9, 3), 8, 6, 4)
  }

  @Test def closedOpenTest(): Unit = {
    verify(closedOpen(3, 8), 4, 6)
    verify(closedOpen(2, 6), 2, 4)
  }

  @Test def closedTest(): Unit = {
    verify(closed(3, 7), 4, 6)
    verify(closed(2, 6), 2, 4, 6)
  }

  @Test def fakeCursor(): Unit = {
    assertThat(cursor.first, is(2))
    assertThat(cursor.next, is(4))
    assertThat(cursor.next, is(6))
    assertThat(cursor.next, is(8))
    assertThat(cursor.next, nullValue)
    assertThat(cursor.first, is(2))
    assertThat(cursor.prev, nullValue)
    assertThat(cursor.getWithSetRange(3), is(4))
    assertThat(cursor.next, is(6))
    assertThat(cursor.getWithSetRange(1), is(2))
    assertThat(cursor.last, is(8))
    assertThat(cursor.getWithSetRange(100), nullValue)
  }

  @Test def greaterThanBackwardTest(): Unit = {
    verify(greaterThanBackward(6), 4, 2)
    verify(greaterThanBackward(7), 6, 4, 2)
    verify(greaterThanBackward(9), 8, 6, 4, 2)
  }

  @Test def greaterThanTest(): Unit = {
    verify(greaterThan(4), 6, 8)
    verify(greaterThan(3), 4, 6, 8)
  }

  @Test def lessThanBackwardTest(): Unit = {
    verify(lessThanBackward(5), 8, 6)
    verify(lessThanBackward(2), 8, 6, 4)
  }

  @Test def lessThanTest(): Unit = {
    verify(lessThan(5), 2, 4)
    verify(lessThan(8), 2, 4, 6)
  }

  @Test def openBackwardTest(): Unit = {
    verify(openBackward(7, 2), 6, 4)
    verify(openBackward(8, 1), 6, 4, 2)
    verify(openBackward(9, 4), 8, 6)
  }

  @Test def openClosedBackwardTest(): Unit = {
    verify(openClosedBackward(7, 2), 6, 4, 2)
    verify(openClosedBackward(8, 4), 6, 4)
    verify(openClosedBackward(9, 4), 8, 6, 4)
  }

  @Test def openClosedTest(): Unit = {
    verify(openClosed(3, 8), 4, 6, 8)
    verify(openClosed(2, 6), 4, 6)
  }

  @Test def openTest(): Unit = {
    verify(open(3, 7), 4, 6)
    verify(open(2, 8), 4, 6)
  }

  private def verify(range: KeyRange[Integer], expected: Int*): Unit = {
    val results = new util.ArrayList[Integer]
    var buff = cursor.apply(range.getType.initialOp, range.getStart)
    var op: IteratorOp = null
    val comparator:Comparator[Integer] = Integer.compare
    op = range.getType.iteratorOp(range.getStart, range.getStop, buff, comparator)
    op match {
      case CALL_NEXT_OP =>
        buff = cursor.apply(range.getType.nextOp, range.getStart)
      case TERMINATE =>
      case RELEASE =>
        results.add(buff)
        buff = cursor.apply(range.getType.nextOp, range.getStart)
      case _ =>
        throw new IllegalStateException("Unknown operation")
    }
    while (op ne TERMINATE) {
      val comparator:Comparator[Integer] = Integer.compare
      op = range.getType.iteratorOp(range.getStart, range.getStop, buff, comparator)
      op match {
        case CALL_NEXT_OP =>
          buff = cursor.apply(range.getType.nextOp, range.getStart)
        case TERMINATE =>
        case RELEASE =>
          results.add(buff)
          buff = cursor.apply(range.getType.nextOp, range.getStart)
        case _ =>
          throw new IllegalStateException("Unknown operation")
      }
    } 
    for (idx <- 0 until results.size) {
      assertThat("idx " + idx, results.get(idx), is(expected(idx)))
    }
    assertThat(results.size, is(expected.length))
  }
}