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
package torch.lmdb.flags

import torch.lmdb.flags.MaskedFlag

/** Flags for use when performing a "put". */
enum PutFlags(val mask: Int) extends MaskedFlag:
  /** For put: Don't write if the key already exists. */
  case MDB_NOOVERWRITE extends PutFlags(0x10)
  
  /**
   * Only for #MDB_DUPSORT<br>
   * For put: don't write if the key and data pair already exist.<br>
   * For mdb_cursor_del: remove all duplicate data items.
   */
  case MDB_NODUPDATA extends PutFlags(0x20)
  
  /** For mdb_cursor_put: overwrite the current key/data pair. */
  case MDB_CURRENT extends PutFlags(0x40)
  
  /**
   * For put: Just reserve space for data, don't copy it. Return a pointer to the reserved space.
   */
  case MDB_RESERVE extends PutFlags(0x1_0000)
  
  /** Data is being appended, don't split full pages. */
  case MDB_APPEND extends PutFlags(0x2_0000)
  
  /** Duplicate data is being appended, don't split full pages. */
  case MDB_APPENDDUP extends PutFlags(0x4_0000)
  
  /** Store multiple data items in one call. Only for #MDB_DUPFIXED. */
  case MDB_MULTIPLE extends PutFlags(0x8_0000)

  override def getMask: Int = mask