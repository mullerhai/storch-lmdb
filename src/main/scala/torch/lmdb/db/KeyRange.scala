/*
 * Copyright © 2016-2025 The torch.lmdb Open Source Project
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
package torch.lmdb.db

import torch.lmdb.db.KeyRange
import torch.lmdb.enums.KeyRangeType
import torch.lmdb.enums.KeyRangeType.{BACKWARD_ALL, FORWARD_ALL}

import java.util.Objects.requireNonNull
//import .BACKWARD_ALL
//import .FORWARD_ALL

/**
 * Limits the range and direction of keys to iterate.
 *
 * <p>Immutable once created (although the buffers themselves may not be).
 *
 * @param < T> buffer type
 */
object KeyRange {
  private val BK = new KeyRange[AnyRef](BACKWARD_ALL, null, null)
  private val FW = new KeyRange[AnyRef](FORWARD_ALL, null, null)

  /**
   * Create a {@link KeyRangeType# FORWARD_ALL} range.
   *
   * @param < T> buffer type
   * @return a key range (never null)
   */
  def all[T]: KeyRange[T] = FW.asInstanceOf[KeyRange[T]]

  /**
   * Create a {@link KeyRangeType# BACKWARD_ALL} range.
   *
   * @param < T> buffer type
   * @return a key range (never null)
   */
  def allBackward[T]: KeyRange[T] = BK.asInstanceOf[KeyRange[T]]

  /**
   * Create a {@link KeyRangeType# FORWARD_AT_LEAST} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  def atLeast[T](start: T) = new KeyRange[T](KeyRangeType.FORWARD_AT_LEAST, start, null)

  /**
   * Create a {@link KeyRangeType# BACKWARD_AT_LEAST} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  def atLeastBackward[T](start: T) = new KeyRange[T](KeyRangeType.BACKWARD_AT_LEAST, start, null)

  /**
   * Create a {@link KeyRangeType# FORWARD_AT_MOST} range.
   *
   * @param <    T> buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  def atMost[T](stop: T) = new KeyRange[T](KeyRangeType.FORWARD_AT_MOST, null, stop)

  /**
   * Create a {@link KeyRangeType# BACKWARD_AT_MOST} range.
   *
   * @param <    T> buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  def atMostBackward[T](stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_AT_MOST, null, stop)

  /**
   * Create a {@link KeyRangeType# FORWARD_CLOSED} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def closed[T](start: T, stop: T) = new KeyRange[T](KeyRangeType.FORWARD_CLOSED, start, stop)

  /**
   * Create a {@link KeyRangeType# BACKWARD_CLOSED} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def closedBackward[T](start: T, stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_CLOSED, start, stop)

  /**
   * Create a {@link KeyRangeType# FORWARD_CLOSED_OPEN} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def closedOpen[T](start: T, stop: T) = new KeyRange[T](KeyRangeType.FORWARD_CLOSED_OPEN, start, stop)

  /**
   * Create a {@link KeyRangeType# BACKWARD_CLOSED_OPEN} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def closedOpenBackward[T](start: T, stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_CLOSED_OPEN, start, stop)

  /**
   * Create a {@link KeyRangeType# FORWARD_GREATER_THAN} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  def greaterThan[T](start: T) = new KeyRange[T](KeyRangeType.FORWARD_GREATER_THAN, start, null)

  /**
   * Create a {@link KeyRangeType# BACKWARD_GREATER_THAN} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  def greaterThanBackward[T](start: T) = new KeyRange[T](KeyRangeType.BACKWARD_GREATER_THAN, start, null)

  /**
   * Create a {@link KeyRangeType# FORWARD_LESS_THAN} range.
   *
   * @param <    T> buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  def lessThan[T](stop: T) = new KeyRange[T](KeyRangeType.FORWARD_LESS_THAN, null, stop)

  /**
   * Create a {@link KeyRangeType# BACKWARD_LESS_THAN} range.
   *
   * @param <    T> buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  def lessThanBackward[T](stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_LESS_THAN, null, stop)

  /**
   * Create a {@link KeyRangeType# FORWARD_OPEN} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def open[T](start: T, stop: T) = new KeyRange[T](KeyRangeType.FORWARD_OPEN, start, stop)

  /**
   * Create a {@link KeyRangeType# BACKWARD_OPEN} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def openBackward[T](start: T, stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_OPEN, start, stop)

  /**
   * Create a {@link KeyRangeType# FORWARD_OPEN_CLOSED} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def openClosed[T](start: T, stop: T) = new KeyRange[T](KeyRangeType.FORWARD_OPEN_CLOSED, start, stop)

  /**
   * Create a {@link KeyRangeType# BACKWARD_OPEN_CLOSED} range.
   *
   * @param <     T> buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def openClosedBackward[T](start: T, stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_OPEN_CLOSED, start, stop)
}

final class KeyRange[T](private val types: KeyRangeType, private val start: T | AnyRef, private val stop: T | AnyRef) {
  requireNonNull(types, "Key range type is required")
  if (types.isStartKeyRequired) requireNonNull(start, "Start key is required for this key range type")
  if (types.isStopKeyRequired) requireNonNull(stop, "Stop key is required for this key range type")

  /**
   * Start key.
   *
   * @return start key (may be null)
   */
  def getStart: T = start.asInstanceOf[T]

  /**
   * Stop key.
   *
   * @return stop key (may be null)
   */
  def getStop: T = stop.asInstanceOf[T]

  /**
   * Key range type.
   *
   * @return type (never null)
   */
  def getType: KeyRangeType = types
}