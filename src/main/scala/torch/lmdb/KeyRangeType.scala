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

import java.util.Objects.requireNonNull
import torch.lmdb.CursorOp.FIRST
import torch.lmdb.CursorOp.GET_START_KEY
import torch.lmdb.CursorOp.GET_START_KEY_BACKWARD
import torch.lmdb.CursorOp.LAST
import torch.lmdb.CursorOp.NEXT
import torch.lmdb.CursorOp.PREV
import torch.lmdb.IteratorOp.CALL_NEXT_OP
import torch.lmdb.IteratorOp.RELEASE
import torch.lmdb.IteratorOp.TERMINATE
import java.util.Comparator

/**
 * Key range type.
 *
 * <p>The terminology used in this class is adapted from Google Guava's ranges. Refer to the <a
 * href="https://github.com/google/guava/wiki/RangesExplained">Ranges Explained</a> wiki page for
 * more information. LmddJava prepends either "FORWARD" or "BACKWARD" to denote the iterator order.
 *
 * <p>In the examples below, it is assumed the table has keys 2, 4, 6 and 8.
 */
//object KeyRangeType extends Enumeration {
//  type KeyRangeType = Value
//  val
enum KeyRangeType:
  
  case 
  /**
   * Starting on the first key and iterate forward until no keys remain.
   *
   * <p>The "start" and "stop" values are ignored.
   *
   * <p>In our example, the returned keys would be 2, 4, 6 and 8.
   */
  FORWARD_ALL,

  /**
   * Start on the passed key (or the first key immediately after it) and iterate forward until no
   * keys remain.
   *
   * <p>The "start" value is required. The "stop" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 6 and 8. With a
   * passed key of 6, the returned keys would be 6 and 8.
   */
  FORWARD_AT_LEAST,

  /**
   * Start on the first key and iterate forward until a key equal to it (or the first key
   * immediately after it) is reached.
   *
   * <p>The "stop" value is required. The "start" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 2 and 4. With a
   * passed key of 6, the returned keys would be 2, 4 and 6.
   */
  FORWARD_AT_MOST,

  /**
   * Iterate forward between the passed keys, matching on the first keys directly equal to the
   * passed key (or immediately following it in the case of the "start" key, or immediately
   * preceding it in the case of the "stop" key).
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 3 - 7, the returned keys would be 4 and 6.
   * With a range of 2 - 6, the keys would be 2, 4 and 6.
   */
  FORWARD_CLOSED,

  /**
   * Iterate forward between the passed keys, matching on the first keys directly equal to the
   * passed key (or immediately following it in the case of the "start" key, or immediately
   * preceding it in the case of the "stop" key). Do not return the "stop" key.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 3 - 8, the returned keys would be 4 and 6.
   * With a range of 2 - 6, the keys would be 2 and 4.
   */
  FORWARD_CLOSED_OPEN,

  /**
   * Start after the passed key (but not equal to it) and iterate forward until no keys remain.
   *
   * <p>The "start" value is required. The "stop" value is ignored.
   *
   * <p>In our example and with a passed search key of 4, the returned keys would be 6 and 8. With a
   * passed key of 3, the returned keys would be 4, 6 and 8.
   */
  FORWARD_GREATER_THAN,

  /**
   * Start on the first key and iterate forward until a key the passed key has been reached (but do
   * not return that key).
   *
   * <p>The "stop" value is required. The "start" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 2 and 4. With a
   * passed key of 8, the returned keys would be 2, 4 and 6.
   */
  FORWARD_LESS_THAN,

  /**
   * Iterate forward between the passed keys but not equal to either of them.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 3 - 7, the returned keys would be 4 and 6.
   * With a range of 2 - 8, the key would be 4 and 6.
   */
  FORWARD_OPEN,

  /**
   * Iterate forward between the passed keys. Do not return the "start" key, but do return the
   * "stop" key.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 3 - 8, the returned keys would be 4, 6 and
   * 8. With a range of 2 - 6, the keys would be 4 and 6.
   */
  FORWARD_OPEN_CLOSED,

  /**
   * Start on the last key and iterate backward until no keys remain.
   *
   * <p>The "start" and "stop" values are ignored.
   *
   * <p>In our example, the returned keys would be 8, 6, 4 and 2.
   */
  BACKWARD_ALL,

  /**
   * Start on the passed key (or the first key immediately preceding it) and iterate backward until
   * no keys remain.
   *
   * <p>The "start" value is required. The "stop" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 4 and 2. With a
   * passed key of 6, the returned keys would be 6, 4 and 2. With a passed key of 9, the returned
   * keys would be 8, 6, 4 and 2.
   */
  BACKWARD_AT_LEAST,

  /**
   * Start on the last key and iterate backward until a key equal to it (or the first key
   * immediately preceding it it) is reached.
   *
   * <p>The "stop" value is required. The "start" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 8 and 6. With a
   * passed key of 6, the returned keys would be 8 and 6.
   */
  BACKWARD_AT_MOST,

  /**
   * Iterate backward between the passed keys, matching on the first keys directly equal to the
   * passed key (or immediately preceding it in the case of the "start" key, or immediately
   * following it in the case of the "stop" key).
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 7 - 3, the returned keys would be 6 and 4.
   * With a range of 6 - 2, the keys would be 6, 4 and 2. With a range of 9 - 3, the returned keys
   * would be 8, 6 and 4.
   */
  BACKWARD_CLOSED,

  /**
   * Iterate backward between the passed keys, matching on the first keys directly equal to the
   * passed key (or immediately preceding it in the case of the "start" key, or immediately
   * following it in the case of the "stop" key). Do not return the "stop" key.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 8 - 3, the returned keys would be 8, 6 and
   * 4. With a range of 7 - 2, the keys would be 6 and 4. With a range of 9 - 3, the keys would be
   * 8, 6 and 4.
   */
  BACKWARD_CLOSED_OPEN,

  /**
   * Start immediate prior to the passed key (but not equal to it) and iterate backward until no
   * keys remain.
   *
   * <p>The "start" value is required. The "stop" value is ignored.
   *
   * <p>In our example and with a passed search key of 6, the returned keys would be 4 and 2. With a
   * passed key of 7, the returned keys would be 6, 4 and 2. With a passed key of 9, the returned
   * keys would be 8, 6, 4 and 2.
   */
  BACKWARD_GREATER_THAN,

  /**
   * Start on the last key and iterate backward until the last key greater than the passed "stop"
   * key is reached. Do not return the "stop" key.
   *
   * <p>The "stop" value is required. The "start" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 8 and 6. With a
   * passed key of 2, the returned keys would be 8, 6 and 4
   */
  BACKWARD_LESS_THAN,

  /**
   * Iterate backward between the passed keys, but do not return the passed keys.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 7 - 2, the returned keys would be 6 and 4.
   * With a range of 8 - 1, the keys would be 6, 4 and 2. With a range of 9 - 4, the keys would be 8
   * and 6.
   */
  BACKWARD_OPEN,

  /**
   * Iterate backward between the passed keys. Do not return the "start" key, but do return the
   * "stop" key.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 7 - 2, the returned keys would be 6, 4 and
   * 2. With a range of 8 - 4, the keys would be 6 and 4. With a range of 9 - 4, the keys would be
   * 8, 6 and 4.
   */
  BACKWARD_OPEN_CLOSED 
  private val directionForward = false
  private val startKeyRequired: Boolean = false
  private val stopKeyRequired: Boolean = false
  def this (directionForward: Boolean, startKeyRequired: Boolean, stopKeyRequired: Boolean) = {
    this ()
    this.directionForward = directionForward
    this.startKeyRequired = startKeyRequired
    this.stopKeyRequired = stopKeyRequired
  }

  /**
   * Whether the key space is iterated in the order provided by LMDB.
   *
   * @return true if forward, false if reverse
   */
  def isDirectionForward: Boolean = directionForward

  /**
   * Whether the iteration requires a "start" key.
   *
   * @return true if start key must be non-null
   */
  def isStartKeyRequired: Boolean = startKeyRequired

  /**
   * Whether the iteration requires a "stop" key.
   *
   * @return true if stop key must be non-null
   */
  def isStopKeyRequired: Boolean = stopKeyRequired

  /**
   * Determine the iterator action to take when iterator first begins.
   *
   * <p>The iterator will perform this action and present the resulting key to {@link 
 * #iteratorOp(java.util.Comparator, java.lang.Object)} for decision.
   *
   * @return appropriate action in response to this buffer
   */
  private[torch] def initialOp = this match {
    case FORWARD_ALL =>
      FIRST
    case FORWARD_AT_LEAST =>
      GET_START_KEY
    case FORWARD_AT_MOST =>
      FIRST
    case FORWARD_CLOSED =>
      GET_START_KEY
    case FORWARD_CLOSED_OPEN =>
      GET_START_KEY
    case FORWARD_GREATER_THAN =>
      GET_START_KEY
    case FORWARD_LESS_THAN =>
      FIRST
    case FORWARD_OPEN =>
      GET_START_KEY
    case FORWARD_OPEN_CLOSED =>
      GET_START_KEY
    case BACKWARD_ALL =>
      LAST
    case BACKWARD_AT_LEAST =>
      GET_START_KEY_BACKWARD
    case BACKWARD_AT_MOST =>
      LAST
    case BACKWARD_CLOSED =>
      GET_START_KEY_BACKWARD
    case BACKWARD_CLOSED_OPEN =>
      GET_START_KEY_BACKWARD
    case BACKWARD_GREATER_THAN =>
      GET_START_KEY_BACKWARD
    case BACKWARD_LESS_THAN =>
      LAST
    case BACKWARD_OPEN =>
      GET_START_KEY_BACKWARD
    case BACKWARD_OPEN_CLOSED =>
      GET_START_KEY_BACKWARD
    case _ =>
      throw new IllegalStateException("Invalid type")
  }

  /**
   * Determine the iterator's response to the presented key.
   *
   * @param <      T> buffer type
   * @param <      C> comparator for the buffers
   * @param start  start buffer
   * @param stop   stop buffer
   * @param buffer current key returned by LMDB (may be null)
   * @param c      comparator (required)
   * @return response to this key
   */
  private[torch] def iteratorOp[T, C <: Comparator[T]](start: T, stop: T, buffer: T, c: C): KeyRangeType.IteratorOp = {
    requireNonNull(c, "Comparator required")
    if (buffer == null) return TERMINATE
    this match {
      case FORWARD_ALL =>
        RELEASE
      case FORWARD_AT_LEAST =>
        RELEASE
      case FORWARD_AT_MOST =>
        if (c.compare(buffer, stop) > 0) TERMINATE
        else RELEASE
      case FORWARD_CLOSED =>
        if (c.compare(buffer, stop) > 0) TERMINATE
        else RELEASE
      case FORWARD_CLOSED_OPEN =>
        if (c.compare(buffer, stop) >= 0) TERMINATE
        else RELEASE
      case FORWARD_GREATER_THAN =>
        if (c.compare(buffer, start) == 0) CALL_NEXT_OP
        else RELEASE
      case FORWARD_LESS_THAN =>
        if (c.compare(buffer, stop) >= 0) TERMINATE
        else RELEASE
      case FORWARD_OPEN =>
        if (c.compare(buffer, start) == 0) return CALL_NEXT_OP
        if (c.compare(buffer, stop) >= 0) TERMINATE
        else RELEASE
      case FORWARD_OPEN_CLOSED =>
        if (c.compare(buffer, start) == 0) return CALL_NEXT_OP
        if (c.compare(buffer, stop) > 0) TERMINATE
        else RELEASE
      case BACKWARD_ALL =>
        RELEASE
      case BACKWARD_AT_LEAST =>
        if (c.compare(buffer, start) > 0) CALL_NEXT_OP
        else RELEASE // rewind
      case BACKWARD_AT_MOST =>
        if (c.compare(buffer, stop) >= 0) RELEASE
        else TERMINATE
      case BACKWARD_CLOSED =>
        if (c.compare(buffer, start) > 0) return CALL_NEXT_OP // rewind
        if (c.compare(buffer, stop) >= 0) RELEASE
        else TERMINATE
      case BACKWARD_CLOSED_OPEN =>
        if (c.compare(buffer, start) > 0) return CALL_NEXT_OP // rewind
        if (c.compare(buffer, stop) > 0) RELEASE
        else TERMINATE
      case BACKWARD_GREATER_THAN =>
        if (c.compare(buffer, start) >= 0) CALL_NEXT_OP
        else RELEASE
      case BACKWARD_LESS_THAN =>
        if (c.compare(buffer, stop) > 0) RELEASE
        else TERMINATE
      case BACKWARD_OPEN =>
        if (c.compare(buffer, start) >= 0) return CALL_NEXT_OP // rewind
        if (c.compare(buffer, stop) > 0) RELEASE
        else TERMINATE
      case BACKWARD_OPEN_CLOSED =>
        if (c.compare(buffer, start) >= 0) return CALL_NEXT_OP // rewind
        if (c.compare(buffer, stop) >= 0) RELEASE
        else TERMINATE
      case _ =>
        throw new IllegalStateException("Invalid type")
    }
  }

  /**
   * Determine the iterator action to take when "next" is called or upon request of {@link 
 * #iteratorOp(java.util.Comparator, java.lang.Object)}.
   *
   * <p>The iterator will perform this action and present the resulting key to {@link 
 * #iteratorOp(java.util.Comparator, java.lang.Object)} for decision.
   *
   * @return appropriate action for this key range type
   */
  private[torch] def nextOp = if (isDirectionForward) NEXT
  else PREV

  /** Action now required with the iterator. */
//  private[torch] object IteratorOp extends Enumeration {
//    type IteratorOp = Value
//    val
enum IteratorOp:
  case 
    /** Consider iterator completed. */
    TERMINATE,

    /** Call {@link KeyRange# nextOp ( )} again and try again. */
    CALL_NEXT_OP,

    /** Return the key to the user. */
    RELEASE
  

  /** Action now required with the cursor. */
//  private[torch] object CursorOp extends Enumeration {
//    type CursorOp = Value
//    val
  
enum CursorOp:
  case     
    /** Move to first. */
    FIRST,

    /** Move to last. */
    LAST,

    /** Get "start" key with {@link GetOp# MDB_SET_RANGE}. */
    GET_START_KEY,

    /** Get "start" key with {@link GetOp# MDB_SET_RANGE}, fall back to LAST. */
    GET_START_KEY_BACKWARD,

    /** Move forward. */
    NEXT,

    /** Move backward. */
    PREV 
  
}