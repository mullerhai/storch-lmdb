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
package lmdb.enums

import torch.lmdb.enums.SeekOp.code

/**
 * Flags for use when performing a {@link Cursor# seek ( org.torch.lmdb.SeekOp )}.
 *
 * <p>Unlike most other LMDB enums, this enum is not bit masked.
 */
enum SeekOp(val code: Int):
  case MDB_FIRST extends SeekOp(0)
  case MDB_FIRST_DUP extends SeekOp(1)
  case MDB_GET_BOTH extends SeekOp(2)
  case MDB_GET_BOTH_RANGE extends SeekOp(3)
  case MDB_GET_CURRENT extends SeekOp(4)
  case MDB_GET_MULTIPLE extends SeekOp(5)
  case MDB_LAST extends SeekOp(6)
  case MDB_LAST_DUP extends SeekOp(7)
  case MDB_NEXT extends SeekOp(8)
  case MDB_NEXT_DUP extends SeekOp(9)
  case MDB_NEXT_MULTIPLE extends SeekOp(10)
  case MDB_NEXT_NODUP extends SeekOp(11)
  case MDB_PREV extends SeekOp(12)
  case MDB_PREV_DUP extends SeekOp(13)
  case MDB_PREV_NODUP extends SeekOp(14)
  def getCode: Int = code
  override def toString: String = code.toString
 
  
  
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