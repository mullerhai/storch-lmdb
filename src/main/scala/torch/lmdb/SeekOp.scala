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

/**
 * Flags for use when performing a {@link Cursor# seek ( org.lmdbjava.SeekOp )}.
 *
 * <p>Unlike most other LMDB enums, this enum is not bit masked.
 */
enum SeekOp:
  case MDB_FIRST,
  MDB_FIRST_DUP,
  MDB_GET_BOTH,
  MDB_GET_BOTH_RANGE,
  MDB_GET_CURRENT,
  MDB_GET_MULTIPLE,
  MDB_LAST,
  MDB_LAST_DUP,
  MDB_NEXT,
  MDB_NEXT_DUP,
  MDB_NEXT_MULTIPLE,
  MDB_NEXT_NODUP,
  MDB_PREV,
  MDB_PREV_DUP,
  MDB_PREV_NODUP 
object SeekOp {
//  type SeekOp = Value
//  val
//
//  /** Position at first key/data item. */
//  MDB_FIRST,
//
//  /** Position at first data item of current key. Only for {@link DbiFlags# MDB_DUPSORT}. */
//  MDB_FIRST_DUP,
//
//  /** Position at key/data pair. Only for {@link DbiFlags# MDB_DUPSORT}. */
//  MDB_GET_BOTH,
//
//  /** position at key, nearest data. Only for {@link DbiFlags# MDB_DUPSORT}. */
//  MDB_GET_BOTH_RANGE,
//
//  /** Return key/data at current cursor position. */
//  MDB_GET_CURRENT,
//
//  /**
//   * Return key and up to a page of duplicate data items from current cursor position. Move cursor
//   * to prepare for {@link # MDB_NEXT_MULTIPLE}. Only for {@link DbiFlags# MDB_DUPSORT}.
//   */
//  MDB_GET_MULTIPLE,
//
//  /** Position at last key/data item. */
//  MDB_LAST,
//
//  /** Position at last data item of current key. Only for {@link DbiFlags# MDB_DUPSORT}. */
//  MDB_LAST_DUP,
//
//  /** Position at next data item. */
//  MDB_NEXT,
//
//  /** Position at next data item of current key. Only for {@link DbiFlags# MDB_DUPSORT}. */
//  MDB_NEXT_DUP,
//
//  /**
//   * Return key and up to a page of duplicate data items from next cursor position. Move cursor to
//   * prepare for {@link # MDB_NEXT_MULTIPLE}. Only for {@link DbiFlags# MDB_DUPSORT}.
//   */
//  MDB_NEXT_MULTIPLE,
//
//  /** Position at first data item of next key. */
//  MDB_NEXT_NODUP,
//
//  /** Position at previous data item. */
//  MDB_PREV,
//
//  /** Position at previous data item of current key. {@link DbiFlags# MDB_DUPSORT}. */
//  MDB_PREV_DUP,
//
//  /** Position at last data item of previous key. */
//  MDB_PREV_NODUP = Value
  private var code = 0
  def this (code: Int) ={
    this ()
    this.code = code
  }

  /**
   * Obtain the integer code for use by LMDB C API.
   *
   * @return the code
   */
  def getCode: Int = code
}