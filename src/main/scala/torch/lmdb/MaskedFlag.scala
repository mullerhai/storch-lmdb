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
import java.util
import java.util.Objects
import java.util.function.Predicate
import java.util.stream.Stream

/** Indicates an enum that can provide integers for each of its values. */
object MaskedFlag {
  /**
   * Fetch the integer mask for all presented flags.
   *
   * @param <     M> flag type
   * @param flags to mask (null or empty returns zero)
   * @return the integer mask for use in C
   */
    @SafeVarargs def mask[M <: MaskedFlag](flags: M*): Int = mask(false, flags*)

  /**
   * Fetch the integer mask for all presented flags.
   *
   * @param <     M> flag type
   * @param flags to mask (null or empty returns zero)
   * @return the integer mask for use in C
   */
  def mask[M <: MaskedFlag](flags: Stream[M]): Int = mask(false, flags)

  /**
   * Fetch the integer mask for the presented flags.
   *
   * @param <                    M> flag type
   * @param onlyPropagatedToLmdb if to include only the flags which are also propagate to the C code
   *                             or all of them
   * @param flags                to mask (null or empty returns zero)
   * @return the integer mask for use in C
   */
  @SafeVarargs def mask[M <: MaskedFlag](onlyPropagatedToLmdb: Boolean, flags: M*): Int = if (flags == null) 0
  else mask(onlyPropagatedToLmdb, java.util.Arrays.stream(flags))

  /**
   * Fetch the integer mask for all presented flags.
   *
   * @param <                    M> flag type
   * @param onlyPropagatedToLmdb if to include only the flags which are also propagate to the C code
   *                             or all of them
   * @param flags                to mask
   * @return the integer mask for use in C
   */
  def mask[M <: MaskedFlag](onlyPropagatedToLmdb: Boolean, flags: Stream[M]): Int = {
    val filter = if (onlyPropagatedToLmdb) then  MaskedFlag.isPropagatedToLmdb
    else (f: M) => true
    if (flags == null) 0
    else flags.filter(Objects.nonNull).filter(filter).map(M.getMask).reduce(0, (f1: Integer, f2: Integer) => f1 | f2)
  }

  /**
   * Indicates whether the passed flag has the relevant masked flag high.
   *
   * @param flags to evaluate (usually produced by {@link # mask ( org.lmdbjava.MaskedFlag...)}
   * @param test  the flag being sought (required)
   * @return true if set.
   */
  def isSet(flags: Int, test: MaskedFlag): Boolean = {
    requireNonNull(test)
    (flags & test.getMask) == test.getMask
  }
}

trait MaskedFlag {
  /**
   * Obtains the integer value for this enum which can be included in a mask.
   *
   * @return the integer value for combination into a mask
   */
  def getMask: Int

  /**
   * Indicates if the flag must be propagated to the underlying C code of LMDB or not.
   *
   * @return the boolean value indicating the propagation
   */
  def isPropagatedToLmdb = true
}