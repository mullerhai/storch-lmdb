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

/**
 * Flags for use when performing a {@link Cursor# get ( java.lang.Object, org.torch.lmdb.GetOp)}.
 *
 * <p>Unlike most other LMDB enums, this enum is not bit masked.
 */
enum GetOp(code: Int):
  case MDB_SET extends GetOp(15)
  case MDB_SET_KEY extends GetOp(16)
  case MDB_SET_RANGE extends GetOp(17)
  def getCode: Int = code
  override def toString: String = code.toString
  
object GetOp  {
//  type GetOp = Value
//  val
//
//  /** Position at specified key. */
//  MDB_SET,
//
//  /** Position at specified key, return key + data. */
//  MDB_SET_KEY,
//
//  /** Position at first key greater than or equal to specified key. */
//  MDB_SET_RANGE = Value
  private var code = 0
  def this (code: Int)= {
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