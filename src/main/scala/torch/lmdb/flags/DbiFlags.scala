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
package torch
package lmdb.flags

/** Flags for use when opening a [[Dbi]]. */
enum DbiFlags(private val mask: Int, private val propagatedToLmdb: Boolean = true) extends MaskedFlag:
  /** Use reverse string keys.
   *
   * <p>Keys are strings to be compared in reverse order, from the end of the strings to the
   * beginning. By default, keys are treated as strings and compared from beginning to end.
   */
  case MDB_REVERSEKEY extends DbiFlags(0x02)

  /** Use sorted duplicates.
   *
   * <p>Duplicate keys may be used in the database. Or, from another perspective, keys may have
   * multiple data items, stored in sorted order. By default keys must be unique and may have only a
   * single data item.
   */
  case MDB_DUPSORT extends DbiFlags(0x04)

  /** Numeric keys in native byte order: either unsigned int or size_t. The keys must all be of the
   * same size.
   */
  case MDB_INTEGERKEY extends DbiFlags(0x08)

  /** With [[MDB_DUPSORT]], sorted dup items have fixed size.
   *
   * <p>This flag may only be used in combination with [[MDB_DUPSORT]]. This option tells the
   * library that the data items for this database are all the same size, which allows further
   * optimizations in storage and retrieval. When all data items are the same size, the
   * [[SeekOp.MDB_GET_MULTIPLE]] and [[SeekOp.MDB_NEXT_MULTIPLE]] cursor operations may be used to
   * retrieve multiple items at once.
   */
  case MDB_DUPFIXED extends DbiFlags(0x10)

  /** With [[MDB_DUPSORT]], dups are [[MDB_INTEGERKEY]]-style integers.
   *
   * <p>This option specifies that duplicate data items are binary integers, similar to
   * [[MDB_INTEGERKEY]] keys.
   */
  case MDB_INTEGERDUP extends DbiFlags(0x20)

  /** Compare the '''numeric''' keys in native byte order and as unsigned.
   *
   * <p>This option is applied only to [[java.nio.ByteBuffer]], [[org.agrona.DirectBuffer]]
   * and byte array keys. [[io.netty.buffer.ByteBuf]] keys are always compared in native byte
   * order and as unsigned.
   */
  case MDB_UNSIGNEDKEY extends DbiFlags(0x30, false)

  /** With [[MDB_DUPSORT]], use reverse string dups.
   *
   * <p>This option specifies that duplicate data items should be compared as strings in reverse
   * order.
   */
  case MDB_REVERSEDUP extends DbiFlags(0x40)

  /** Create the named database if it doesn't exist.
   *
   * <p>This option is not allowed in a read-only transaction or a read-only environment.
   */
  case MDB_CREATE extends DbiFlags(0x4_0000)

  override def getMask: Int = mask
  override def isPropagatedToLmdb: Boolean = propagatedToLmdb