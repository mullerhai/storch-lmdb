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
package torch.lmdb

import torch.lmdb.CursorIterable.State.RELEASED
import torch.lmdb.CursorIterable.State.REQUIRES_INITIAL_OP
import torch.lmdb.CursorIterable.State.REQUIRES_ITERATOR_OP
import torch.lmdb.CursorIterable.State.REQUIRES_NEXT_OP
import torch.lmdb.CursorIterable.State.TERMINATED
import torch.lmdb.CursorOp.{GET_START_KEY, GET_START_KEY_BACKWARD, LAST, NEXT, PREV}
import torch.lmdb.GetOp.MDB_SET_RANGE
import torch.lmdb.IteratorOp.{CALL_NEXT_OP, RELEASE, TERMINATE}

import java.util.Comparator
import java.util
import java.util.NoSuchElementException
import torch.lmdb.CursorOp
import torch.lmdb.CursorOp.*
import torch.lmdb.IteratorOp.*

/**
 * {@link Iterable} that creates a single {@link Iterator} that will iterate over a {@link Cursor}
 * as specified by a {@link KeyRange}.
 *
 * <p>An instance will create and close its own cursor.
 *
 * @param < T> buffer type
 */
object CursorIterable {
  /**
   * Holder for a key and value pair.
   *
   * <p>The same holder instance will always be returned for a given iterator. The returned keys and
   * values may change or point to different memory locations following changes in the iterator,
   * cursor or transaction.
   *
   * @param < T> buffer type
   */
  final class KeyVal[T] {
    private var k: T = null
    private var v: T = null

    /**
     * The key.
     *
     * @return key
     */
    def key: T = k

    /**
     * The value.
     *
     * @return value
     */
    def vals: T = v

    private[torch] def setK(key: T): Unit = {
      this.k = key
    }

    private[torch] def setV(vals: T): Unit = {
      this.v = vals
    }
  }

  /** Represents the internal {@link CursorIterable} state. */
  private[torch] object State extends Enumeration {
    type State = Value
    val REQUIRES_INITIAL_OP, REQUIRES_NEXT_OP, REQUIRES_ITERATOR_OP, RELEASED, TERMINATED = Value
  }
}

final class CursorIterable[T] private[torch](txn: Txn[T], dbi: Dbi[T], private val range: KeyRange[T], private val comparator: Comparator[T]) extends Iterable[CursorIterable.KeyVal[T]] with AutoCloseable {
  this.cursor = dbi.openCursor(txn)
  this.entry = new CursorIterable.KeyVal[T]
  final private var cursor: Cursor[T] = null
  final private var entry: CursorIterable.KeyVal[T] = null
  private var iteratorReturned = false
  private var state = REQUIRES_INITIAL_OP

  override def close(): Unit = {
    cursor.close()
  }

  /**
   * Obtain an iterator.
   *
   * <p>As iteration of the returned iterator will cause movement of the underlying LMDB cursor, an
   * {@link IllegalStateException} is thrown if an attempt is made to obtain the iterator more than
   * once. For advanced cursor control (such as being able to iterate over the same data multiple
   * times etc) please instead refer to {@link Dbi# openCursor ( org.lmdbjava.Txn )}.
   *
   * @return an iterator
   */
  override def iterator: util.Iterator[CursorIterable.KeyVal[T]] = {
    if (iteratorReturned) throw new IllegalStateException("Iterator can only be returned once")
    iteratorReturned = true
    new util.Iterator[CursorIterable.KeyVal[T]]() {
      override def hasNext: Boolean = {
        while ((state ne RELEASED) && (state ne TERMINATED)) update()
        state eq RELEASED
      }

      override def next: CursorIterable.KeyVal[T] = {
        if (!hasNext) throw new NoSuchElementException
        state = REQUIRES_NEXT_OP
        entry
      }

      override def remove(): Unit = {
        cursor.delete()
      }
    }
  }

  private def executeCursorOp(op: CursorOp): Unit = {
    var found = false
    op match {
      case FIRST =>
        found = cursor.first
      case LAST =>
        found = cursor.last
      case NEXT =>
        found = cursor.next
      case PREV =>
        found = cursor.prev
      case GET_START_KEY =>
        found = cursor.get(range.getStart, MDB_SET_RANGE)
      case GET_START_KEY_BACKWARD =>
        found = cursor.get(range.getStart, MDB_SET_RANGE) || cursor.last
      case _ =>
        throw new IllegalStateException("Unknown cursor operation")
    }
    entry.setK(if (found) cursor.key
    else null)
    entry.setV(if (found) cursor.vals
    else null)
  }

  private def executeIteratorOp(): Unit = {
    val op = range.getType.iteratorOp(range.getStart, range.getStop, entry.key, comparator)
    op match {
      case CALL_NEXT_OP =>
        executeCursorOp(range.getType.nextOp)
        state = REQUIRES_ITERATOR_OP
      case TERMINATE =>
        state = TERMINATED
      case RELEASE =>
        state = RELEASED
      case _ =>
        throw new IllegalStateException("Unknown operation")
    }
  }

  private def update(): Unit = {
    state match {
      case REQUIRES_INITIAL_OP =>
        executeCursorOp(range.getType.initialOp)
        state = REQUIRES_ITERATOR_OP
      case REQUIRES_NEXT_OP =>
        executeCursorOp(range.getType.nextOp)
        state = REQUIRES_ITERATOR_OP
      case REQUIRES_ITERATOR_OP =>
        executeIteratorOp()
      case TERMINATED =>
      case _ =>
        throw new IllegalStateException("Unknown state")
    }
  }
}